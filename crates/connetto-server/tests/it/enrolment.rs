//! Device enrolment over a live session (R74 step 3).

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use super::auth_hooks::{Notice, Told};
use connetto_core::SessionId;
use connetto_core::device_cert::{
    ANDROID_ATTESTATION_CHALLENGE, AttestationLevel, CertificateRequest, CertificateSerial,
    DeploymentId, DeviceCertificate, DeviceIssuer, KeyId, RootCa,
};
use connetto_core::messages::{
    ControlMessage, DeviceAttestation, EnrolChallenge, EnrolChallengeRequest, EnrolGrant,
    EnrolRefusal, EnrolRefused, EnrolRequest,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::defaults::{connetto_device_certificates, connetto_device_enrolments};
use connetto_server::device_cert::{
    AndroidStatus, DeviceCertConfig, DeviceEnrolment, EnrolmentStore, RandomSource, StatusList,
};
use connetto_server::{AbuseConfig, LoopbackTransport, RequestGuard, ThrottleConfig, loopback};
use connetto_server::{
    AuthHooks, AuthService, CommittedNotice, DefaultAuthHooks, DeviceEnrolled, HookError,
    HookFuture, VerifiedClaims,
};
use connetto_test_harness::{ConnettoDefaults, Fixture, RosterAuth, WITHHELD_ID};
use diesel::{ExpressionMethods as _, JoinOnDsl as _, QueryDsl as _};
use diesel_async::{AsyncPgConnection, RunQueryDsl as _};
use rcgen::{
    BasicConstraints, CertificateParams, CustomExtension, IsCa, Issuer, KeyPair,
    PKCS_ECDSA_P256_SHA256, PublicKeyData as _,
};
use serde_bytes::ByteBuf;

use super::ticket_shared::{
    OkSigner, TicketManager, build_manager_with_guard, do_handshake_anon, do_handshake_with,
    setup_reader,
};

#[derive(Debug, PartialEq, Eq, diesel::Queryable)]
struct CertificateRow {
    user: String,
    key_id: Vec<u8>,
    serial: Vec<u8>,
}

/// Read every recorded certificate with its enrolling account and key.
async fn records(fixture: &Fixture) -> Vec<CertificateRow> {
    connetto_device_enrolments::table
        .inner_join(
            connetto_device_certificates::table
                .on(connetto_device_certificates::key_id.eq(connetto_device_enrolments::key_id)),
        )
        .select((
            connetto_device_enrolments::user_id,
            connetto_device_enrolments::key_id,
            connetto_device_certificates::serial,
        ))
        .load(&mut *fixture.admin().get().await.expect("connection"))
        .await
        .expect("certificates")
}

diesel::table! {
    enrolment_hook_writes (key_id) {
        key_id -> Binary,
    }
}

struct RecordThenRefuse {
    notices: Arc<Told<ConnettoDefaults>>,
}

impl AuthHooks<ConnettoDefaults> for RecordThenRefuse {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn device_enrolled<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        enrolled: &'a DeviceEnrolled<'a, String, ()>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            diesel::insert_into(enrolment_hook_writes::table)
                .values(enrolment_hook_writes::key_id.eq(enrolled.key.as_bytes().as_slice()))
                .execute(transaction)
                .await
                .map_err(|error| HookError::Backend(error.to_string()))?;
            Err(HookError::Refused(
                "device registration is closed".to_owned(),
            ))
        })
    }

    fn committed(&self, notice: Notice<ConnettoDefaults>) {
        self.notices.committed(notice);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_enrolment_rolls_back_the_device_and_the_hooks_write() {
    let fixture = Fixture::acquire().await;
    let (notices_hook, mut notices) = Told::<ConnettoDefaults>::channel();
    let auth = fixture
        .enrolment_auth(Arc::new(RecordThenRefuse {
            notices: notices_hook,
        }))
        .await;
    fixture
        .exec("CREATE TABLE enrolment_hook_writes (key_id BYTEA PRIMARY KEY)")
        .await;
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    let key = device_key();
    let handed = nonce(&mut client).await;
    assert_eq!(
        enrol(&mut client, &key, handed, None, vec![0xc0]).await,
        refused(
            EnrolRefusal::Refused {
                reason: "device registration is closed".to_owned()
            },
            "e"
        )
    );
    let mut connection = fixture.admin().get().await.expect("connection");
    assert!(
        EnrolmentStore::<ConnettoDefaults>::default()
            .key_record(
                &mut connection,
                None,
                KeyId::of_public_key(&key.subject_public_key_info())
            )
            .await
            .expect("enrolment")
            .is_none()
    );
    assert_eq!(records(&fixture).await.len(), 0, "no certificate recorded");
    let writes: i64 = enrolment_hook_writes::table
        .count()
        .get_result(&mut *connection)
        .await
        .expect("hook writes");
    assert_eq!(writes, 0);
    assert!(matches!(
        notices.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

const DAY: Duration = Duration::from_hours(24);

/// An issuer valid for a year and a month from now, under a fresh root.
fn issuer() -> (Vec<u8>, DeviceIssuer) {
    let (_, cert, issuer) = rooted_issuer();
    (cert, issuer)
}

/// A fresh root's certificate, an issuer's certificate it signed, and the issuer.
pub(super) fn rooted_issuer() -> (Vec<u8>, Vec<u8>, DeviceIssuer) {
    let now = SystemTime::now();
    let root = RootCa::create(
        DeploymentId::from_uuid(uuid::Uuid::from_u128(0x5eed)),
        now - DAY,
        3650 * DAY,
    )
    .expect("root");
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("issuer key");
    let cert = root
        .sign_issuer(
            &key.subject_public_key_info(),
            now - DAY,
            395 * DAY,
            CertificateSerial::new([1; 16]).expect("the serial is positive"),
        )
        .expect("issuer");
    let issuer = DeviceIssuer::new(cert.clone(), key, root.certificate()).expect("load");
    (root.certificate().to_vec(), cert, issuer)
}

/// A session for `identity`, enrolling through `auth` when configured.
async fn session(
    fixture: &Fixture,
    identity: Option<&str>,
    config: Option<DeviceCertConfig>,
    auth: &Arc<AuthService<ConnettoDefaults>>,
) -> LoopbackTransport {
    let manager = enrolling_manager(
        fixture,
        config.map(|config| DeviceEnrolment::new(config, Arc::clone(auth))),
    )
    .await;
    connect(fixture, &manager, identity).await
}

/// A manager that enrols through `enrolment` when one is given.
pub(super) async fn enrolling_manager(
    fixture: &Fixture,
    enrolment: Option<DeviceEnrolment<ConnettoDefaults>>,
) -> Arc<TicketManager<OkSigner>> {
    let manager = build_manager_with_guard(
        setup_reader(fixture).await,
        RosterAuth::granting("alice").withholding(WITHHELD_ID),
        Arc::new(RequestGuard::new(
            ThrottleConfig::default(),
            AbuseConfig::default(),
        )),
        OkSigner,
    );
    if let Some(enrolment) = enrolment {
        manager
            .install_device_enrolment(Arc::new(enrolment))
            .unwrap_or_else(|_| panic!("installed once"));
    }
    manager
}

/// A session for `identity`, `None` meaning anonymous, on `manager`.
pub(super) async fn connect(
    fixture: &Fixture,
    manager: &Arc<TicketManager<OkSigner>>,
    identity: Option<&str>,
) -> LoopbackTransport {
    static NEXT: AtomicU32 = AtomicU32::new(1);
    let (server_end, mut client) = loopback();
    tokio::spawn(Arc::clone(manager).serve(server_end));
    // One session per connection, as one per login, since a second live
    // connection on one session supersedes the first.
    let handle = format!("conn-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    match identity {
        Some(identity) => {
            let grant = format!("user:{identity}#{handle}");
            fixture
                .live_session(identity, SessionId::from_token_hash(&grant))
                .await;
            do_handshake_with(&mut client, &handle, &[&grant]).await;
        }
        None => do_handshake_anon(&mut client, &handle).await,
    }
    client
}

/// Send `message` and return the next control frame, past any revocation
/// lists the server pushes unasked.
pub(super) async fn ask(client: &mut LoopbackTransport, message: ControlMessage) -> ControlMessage {
    client.send_control(message).await.expect("send");
    loop {
        match client.recv().await.expect("recv") {
            Some(
                IncomingFrame::Control(ControlMessage::RevocationUpdate(_))
                | IncomingFrame::Bulk(_),
            ) => {}
            Some(IncomingFrame::Control(reply)) => return reply,
            None => panic!("the session closed"),
        }
    }
}

pub(super) async fn challenge(client: &mut LoopbackTransport) -> ControlMessage {
    ask(
        client,
        ControlMessage::EnrolChallengeRequest(EnrolChallengeRequest {
            request_id: "c".into(),
        }),
    )
    .await
}

pub(super) async fn nonce(client: &mut LoopbackTransport) -> [u8; 32] {
    match challenge(client).await {
        ControlMessage::EnrolChallenge(EnrolChallenge {
            request_id, nonce, ..
        }) => {
            assert_eq!(request_id, "c");
            nonce
        }
        other => panic!("expected a challenge, got {other:?}"),
    }
}

pub(super) async fn enrol(
    client: &mut LoopbackTransport,
    key: &KeyPair,
    nonce: [u8; 32],
    lifetime_secs: Option<u64>,
    descriptor: Vec<u8>,
) -> ControlMessage {
    enrol_attested(client, key, nonce, lifetime_secs, descriptor, None).await
}

pub(super) async fn enrol_attested(
    client: &mut LoopbackTransport,
    key: &KeyPair,
    nonce: [u8; 32],
    lifetime_secs: Option<u64>,
    descriptor: Vec<u8>,
    attestation: Option<DeviceAttestation>,
) -> ControlMessage {
    ask(
        client,
        ControlMessage::EnrolRequest(EnrolRequest {
            request_id: "e".into(),
            csr: CertificateRequest::build(key, &nonce).expect("csr"),
            lifetime_secs,
            descriptor,
            attestation,
        }),
    )
    .await
}

pub(super) fn refused(reason: EnrolRefusal, request_id: &str) -> ControlMessage {
    ControlMessage::EnrolRefused(EnrolRefused {
        request_id: request_id.into(),
        reason,
    })
}

pub(super) fn device_key() -> KeyPair {
    KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("device key")
}

/// One DER tag-length-value in the short form the fixtures take.
fn der_tl(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut out = vec![
        tag,
        u8::try_from(content.len()).expect("the fixture stays short"),
    ];
    out.extend_from_slice(content);
    out
}

/// A `KeyMint` `KeyDescription` the server parses: the version leads, the
/// security level is second, the fixed challenge is fifth.
fn keymint_description(level: u8) -> Vec<u8> {
    let children = [
        der_tl(0x02, &[0x00, 0x01, 0xF4]),
        der_tl(0x0A, &[level]),
        der_tl(0x04, b""),
        der_tl(0x30, &[0x01, 0x01, 0xFF]),
        der_tl(0x04, ANDROID_ATTESTATION_CHALLENGE),
    ];
    der_tl(0x30, &children.concat())
}

/// A chip-proven Android attestation for `key`: the attestation root's DER,
/// the chain (leaf first), and a clean status list to check it against.
pub(super) fn chip_attestation(key: &KeyPair) -> (Vec<u8>, Vec<Vec<u8>>, StatusList) {
    let root_key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("attestation root key");
    let mut root_params =
        CertificateParams::new(vec!["attestation root".to_owned()]).expect("root params");
    root_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let root_cert = root_params
        .self_signed(&root_key)
        .expect("attestation root");

    let intermediate_key =
        KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("intermediate key");
    let mut intermediate_params =
        CertificateParams::new(vec!["attestation intermediate".to_owned()])
            .expect("intermediate params");
    intermediate_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let root_issuer = Issuer::new(
        CertificateParams::new(vec!["attestation root".to_owned()]).expect("root params"),
        &root_key,
    );
    let intermediate_cert = intermediate_params
        .signed_by(&intermediate_key, &root_issuer)
        .expect("intermediate");

    let mut leaf_params = CertificateParams::new(vec!["device".to_owned()]).expect("leaf params");
    leaf_params.custom_extensions = vec![CustomExtension::from_oid_content(
        &[1, 3, 6, 1, 4, 1, 11129, 2, 1, 17],
        keymint_description(1),
    )];
    let intermediate_issuer = Issuer::new(
        CertificateParams::new(vec!["attestation intermediate".to_owned()])
            .expect("intermediate params"),
        &intermediate_key,
    );
    let leaf_cert = leaf_params
        .signed_by(key, &intermediate_issuer)
        .expect("leaf");

    let file = tempfile::NamedTempFile::new().expect("a status list file");
    std::fs::write(file.path(), r#"{"entries": {}}"#).expect("a clean status list");
    let status = StatusList::new(AndroidStatus::File(file.path().to_path_buf()));

    (
        root_cert.der().to_vec(),
        vec![
            leaf_cert.der().to_vec(),
            intermediate_cert.der().to_vec(),
            root_cert.der().to_vec(),
        ],
        status,
    )
}

/// The attestation a chain enrolment sends, the leaf's certificate first.
pub(super) fn android_evidence(chain: Vec<Vec<u8>>) -> DeviceAttestation {
    DeviceAttestation::AndroidKeyChain(chain.into_iter().map(ByteBuf::from).collect())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_signed_in_device_enrols_and_is_recorded() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (issuer_cert, issuer) = issuer();
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer)),
        &auth,
    )
    .await;
    let key = device_key();
    let handed = nonce(&mut client).await;

    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant {
        request_id, chain, ..
    }) = reply
    else {
        panic!("expected a grant, got {reply:?}");
    };
    assert_eq!(request_id, "e");
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].as_slice(), issuer_cert.as_slice());
    let leaf = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    let key_id = KeyId::of_public_key(&key.subject_public_key_info());
    assert_eq!(leaf.identity().account(), "alice");
    assert_eq!(leaf.identity().key(), key_id);
    let lifetime = leaf
        .not_after()
        .duration_since(leaf.not_before())
        .expect("ordered");
    assert_eq!(lifetime, DAY, "an unrequested lifetime is the default");

    let records = records(&fixture).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].user, "alice");
    assert_eq!(records[0].key_id.as_slice(), key_id.as_bytes());
    assert_eq!(records[0].serial.as_slice(), leaf.serial());
    let devices = EnrolmentStore::<ConnettoDefaults>::default()
        .devices(
            &mut fixture.admin().get().await.expect("connection"),
            &"alice".to_owned(),
        )
        .await
        .expect("devices");
    assert_eq!(devices[0].descriptor, vec![0xc0]);
}

/// A session presenting the login grant `grant`, on `manager`, a distinct
/// connection from the ones `connect` opens.
async fn connect_granted(
    fixture: &Fixture,
    manager: &Arc<TicketManager<OkSigner>>,
    grant: &str,
) -> LoopbackTransport {
    static NEXT: AtomicU32 = AtomicU32::new(10_000);
    let (server_end, mut client) = loopback();
    tokio::spawn(Arc::clone(manager).serve(server_end));
    let handle = format!("conn-granted-{}", NEXT.fetch_add(1, Ordering::Relaxed));
    let user = grant
        .strip_prefix("bot:")
        .expect("bot grant")
        .split_once('#')
        .expect("bot list")
        .0;
    fixture
        .live_session(user, SessionId::from_token_hash(grant))
        .await;
    do_handshake_with(&mut client, &handle, &[grant]).await;
    client
}

/// A person session's enrolled certificate carries the wildcard its
/// connection binds, and a bot session's carries exactly the list its login
/// resolved (R77 decision 12).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_enrolled_certificate_carries_the_sessions_bot_list() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (_, issuer) = issuer();
    let manager = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(issuer),
            Arc::clone(&auth),
        )),
    )
    .await;

    let key = device_key();
    let mut person = connect(&fixture, &manager, Some("alice")).await;
    let handed = nonce(&mut person).await;
    let reply = enrol(&mut person, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    let leaf = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    assert_eq!(
        leaf.bot_list(),
        Some(connetto_core::BOT_LIST_WILDCARD),
        "the person's certificate carries the wildcard"
    );

    let key = device_key();
    let mut bot = connect_granted(&fixture, &manager, "bot:alice#orders:read").await;
    let handed = nonce(&mut bot).await;
    let reply = enrol(&mut bot, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    let leaf = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    assert_eq!(
        leaf.bot_list(),
        Some("orders:read"),
        "the bot's certificate carries its list"
    );
}

/// A renewal after the list changed carries the current list, keeping the
/// attestation level the first enrolment recorded (R77 decision 12).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renewal_carries_the_changed_bot_list_and_keeps_the_level() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (_, issuer) = issuer();
    let manager = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(issuer),
            Arc::clone(&auth),
        )),
    )
    .await;
    let key = device_key();

    let mut first = connect_granted(&fixture, &manager, "bot:alice#orders:read").await;
    let handed = nonce(&mut first).await;
    let reply = enrol(&mut first, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    let first = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    assert_eq!(
        first.bot_list(),
        Some("orders:read"),
        "the first enrolment carries the list"
    );

    let mut second =
        connect_granted(&fixture, &manager, "bot:alice#orders:read,orders:insert").await;
    let handed = nonce(&mut second).await;
    let reply = enrol(&mut second, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("the renewal is a grant, got {reply:?}");
    };
    let renewed = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    assert_eq!(
        renewed.bot_list(),
        Some("orders:read,orders:insert"),
        "the renewal carries the changed list"
    );
    assert_eq!(
        renewed.attestation(),
        first.attestation(),
        "the renewal keeps the recorded level"
    );
}

/// A key's first enrolment tells the application, and its renewal tells
/// nothing more (R105 decision 9).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_a_keys_first_enrolment_tells_the_application() {
    let fixture = Fixture::acquire().await;
    let (hooks, mut notices) = Told::<ConnettoDefaults>::channel();
    let auth = fixture.enrolment_auth(hooks).await;
    let (_, issuer) = issuer();
    let enrolment = DeviceEnrolment::new(DeviceCertConfig::new(issuer), auth);
    let manager = enrolling_manager(&fixture, Some(enrolment)).await;
    let mut client = connect(&fixture, &manager, Some("alice")).await;
    let key = device_key();

    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert!(matches!(reply, ControlMessage::EnrolGrant(_)), "{reply:?}");
    let notice = notices
        .try_recv()
        .expect("first enrolment committed before grant");
    let CommittedNotice::DeviceEnrolled {
        session,
        key: enrolled_key,
        device,
        ..
    } = notice
    else {
        panic!("expected device enrolment, got {notice:?}");
    };
    assert_eq!(session.user_id, "alice");
    assert_eq!(
        enrolled_key,
        KeyId::of_public_key(&key.subject_public_key_info())
    );
    assert_eq!(device, vec![0xc0]);
    let devices = EnrolmentStore::<ConnettoDefaults>::default()
        .devices(
            &mut fixture.admin().get().await.expect("connection"),
            &"alice".to_owned(),
        )
        .await
        .expect("devices");
    assert_eq!(session.session_id, devices[0].session);
    assert!(matches!(
        notices.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));

    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert!(matches!(reply, ControlMessage::EnrolGrant(_)), "{reply:?}");
    assert!(matches!(
        notices.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
}

/// The serial a held draw hands the enrolment, its leading zero a
/// certificate cannot carry.
const ZERO_LEADING: [u8; 16] = [
    0x00, 109, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83,
];
/// The serial a redraw hands it, one the certificate can carry.
const REDRAWN: [u8; 16] = [
    109, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84,
];

/// A source that holds its first 16-octet draw, a serial with a leading
/// zero, and hands the redraw one the certificate can carry.
struct HeldDraw {
    held: AtomicU32,
}

impl RandomSource for HeldDraw {
    fn fill(&self, dest: &mut [u8]) -> Result<(), ring::error::Unspecified> {
        if dest.len() == 16 {
            let held = self.held.fetch_add(1, Ordering::Relaxed);
            dest.copy_from_slice(if held == 0 { &ZERO_LEADING } else { &REDRAWN });
        } else {
            dest.fill(0x11);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_held_zero_leading_draw_is_redrawn_and_recorded_as_issued() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (issuer_cert, issuer) = issuer();
    let enrolment = DeviceEnrolment::new(DeviceCertConfig::new(issuer), Arc::clone(&auth))
        .with_random(Box::new(HeldDraw {
            held: AtomicU32::new(0),
        }));
    let manager = enrolling_manager(&fixture, Some(enrolment)).await;
    let mut client = connect(&fixture, &manager, Some("alice")).await;
    let key = device_key();
    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].as_slice(), issuer_cert.as_slice());
    let leaf = DeviceCertificate::parse(&chain[0]).expect("the leaf meets the profile");
    assert_eq!(leaf.serial(), REDRAWN.as_slice());
    let records = records(&fixture).await;
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].serial.as_slice(), leaf.serial());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_anonymous_caller_cannot_enrol() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(
        &fixture,
        None,
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    assert_eq!(
        challenge(&mut client).await,
        refused(EnrolRefusal::Unidentified, "c")
    );
    let reply = enrol(&mut client, &device_key(), [0; 32], None, vec![0xc0]).await;
    assert_eq!(reply, refused(EnrolRefusal::Unidentified, "e"));
    assert_eq!(records(&fixture).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_server_without_an_issuer_refuses() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(&fixture, Some("alice"), None, &auth).await;
    assert_eq!(
        challenge(&mut client).await,
        refused(EnrolRefusal::IssuerUnavailable, "c")
    );
}

/// An account no certificate can name, one holding a character outside the
/// identity URI's path set, is refused unidentified and nothing is recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_account_no_certificate_can_name_is_refused() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(
        &fixture,
        Some("al@ice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    let key = device_key();
    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert_eq!(reply, refused(EnrolRefusal::Unidentified, "e"));
    assert_eq!(records(&fixture).await, Vec::new());
}

/// An issuer expiring before the certificate would cannot sign it, so the
/// enrolment is refused as an unavailable issuer and nothing is recorded.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_issuer_expiring_before_the_certificate_refuses() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let now = SystemTime::now();
    let root = RootCa::create(
        DeploymentId::from_uuid(uuid::Uuid::from_u128(0x5eed)),
        now - DAY,
        3650 * DAY,
    )
    .expect("root");
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("issuer key");
    let cert = root
        .sign_issuer(
            &key.subject_public_key_info(),
            now - DAY,
            2 * DAY,
            CertificateSerial::new([1; 16]).expect("the serial is positive"),
        )
        .expect("issuer");
    let lapsing = DeviceIssuer::new(cert, key, root.certificate()).expect("load");
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(lapsing)),
        &auth,
    )
    .await;
    let handed = nonce(&mut client).await;
    let reply = enrol(
        &mut client,
        &device_key(),
        handed,
        Some(7 * DAY.as_secs()),
        Vec::new(),
    )
    .await;
    assert_eq!(reply, refused(EnrolRefusal::IssuerUnavailable, "e"));
    assert_eq!(records(&fixture).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_nonce_serves_one_request_only() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    let key = device_key();
    let reply = enrol(&mut client, &key, [9; 32], None, vec![0xc0]).await;
    assert_eq!(
        reply,
        refused(EnrolRefusal::ChallengeExpired, "e"),
        "no challenge was asked"
    );

    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, [9; 32], None, vec![0xc0]).await;
    assert_eq!(
        reply,
        refused(EnrolRefusal::ChallengeExpired, "e"),
        "a nonce the server never handed"
    );
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert_eq!(
        reply,
        refused(EnrolRefusal::ChallengeExpired, "e"),
        "the failed attempt spent it"
    );

    let handed = nonce(&mut client).await;
    assert!(matches!(
        enrol(&mut client, &key, handed, None, vec![0xc0]).await,
        ControlMessage::EnrolGrant(_)
    ));
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert_eq!(
        reply,
        refused(EnrolRefusal::ChallengeExpired, "e"),
        "a nonce is spent once used"
    );
    assert_eq!(records(&fixture).await.len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_expired_nonce_is_refused() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let config = DeviceCertConfig::new(issuer().1).with_challenge_window(Duration::ZERO);
    let mut client = session(&fixture, Some("alice"), Some(config), &auth).await;
    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &device_key(), handed, None, vec![0xc0]).await;
    assert_eq!(reply, refused(EnrolRefusal::ChallengeExpired, "e"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lifetime_over_the_ceiling_is_refused_and_one_under_it_granted() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let config = DeviceCertConfig::new(issuer().1).with_lifetime_ceiling(7 * DAY);
    let mut client = session(&fixture, Some("alice"), Some(config), &auth).await;
    let key = device_key();
    let nonce_one = nonce(&mut client).await;
    let over = 7 * 86_400 + 1;
    let reply = enrol(&mut client, &key, nonce_one, Some(over), Vec::new()).await;
    assert_eq!(
        reply,
        refused(
            EnrolRefusal::OverCeiling {
                ceiling_secs: 7 * 86_400
            },
            "e"
        )
    );

    let nonce_two = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, nonce_two, Some(7 * 86_400), vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    let leaf = DeviceCertificate::parse(&chain[0]).expect("profile");
    let lifetime = leaf
        .not_after()
        .duration_since(leaf.not_before())
        .expect("ordered");
    assert_eq!(lifetime, 7 * DAY);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_key_is_refused() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let key = device_key();
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    let handed = nonce(&mut client).await;
    assert!(matches!(
        enrol(&mut client, &key, handed, None, vec![0xc0]).await,
        ControlMessage::EnrolGrant(_)
    ));
    EnrolmentStore::<ConnettoDefaults>::default()
        .revoke(
            &mut fixture.admin().get().await.expect("connection"),
            None,
            KeyId::of_public_key(&key.subject_public_key_info()),
            SystemTime::now(),
        )
        .await
        .expect("revoke");
    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    assert_eq!(reply, refused(EnrolRefusal::Revoked, "e"));
    assert_eq!(
        records(&fixture).await.len(),
        1,
        "the renewal records nothing"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_malformed_request_or_oversized_descriptor_is_refused() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;

    nonce(&mut client).await;
    let reply = ask(
        &mut client,
        ControlMessage::EnrolRequest(EnrolRequest {
            request_id: "e".into(),
            csr: b"not a request".to_vec(),
            lifetime_secs: None,
            descriptor: Vec::new(),
            attestation: None,
        }),
    )
    .await;
    assert_eq!(reply, refused(EnrolRefusal::InvalidRequest, "e"));

    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &device_key(), handed, None, vec![0; 4097]).await;
    assert_eq!(reply, refused(EnrolRefusal::InvalidRequest, "e"));
    assert_eq!(records(&fixture).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_app_attestation_that_does_not_parse_is_refused() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let mut client = session(
        &fixture,
        Some("alice"),
        Some(DeviceCertConfig::new(issuer().1)),
        &auth,
    )
    .await;
    let handed = nonce(&mut client).await;
    let reply = enrol_attested(
        &mut client,
        &device_key(),
        handed,
        None,
        Vec::new(),
        Some(DeviceAttestation::AppleAppAttest {
            key_id: vec![1; 32],
            attestation: b"not an attestation object".to_vec(),
        }),
    )
    .await;
    assert_eq!(reply, refused(EnrolRefusal::InvalidRequest, "e"));
    assert_eq!(records(&fixture).await, Vec::new());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_renewal_keeps_the_attestation_level_the_first_enrolment_recorded() {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (_, issuer) = issuer();
    let key = device_key();
    let (root, chain, status) = chip_attestation(&key);
    let config = DeviceCertConfig::new(issuer)
        .with_android_roots(vec![root])
        .with_accepted_attestation([AttestationLevel::ChipProven]);
    let manager = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(config, Arc::clone(&auth)).with_status_list(status)),
    )
    .await;
    let mut client = connect(&fixture, &manager, Some("alice")).await;

    // The first enrolment: a chip-proven chain records `chip-proven`.
    let handed = nonce(&mut client).await;
    let reply = enrol_attested(
        &mut client,
        &key,
        handed,
        None,
        vec![0xc0],
        Some(android_evidence(chain)),
    )
    .await;
    let ControlMessage::EnrolGrant(EnrolGrant {
        request_id,
        chain: granted,
        ..
    }) = reply
    else {
        panic!("expected a grant, got {reply:?}");
    };
    assert_eq!(request_id, "e");
    let leaf = DeviceCertificate::parse(&granted[0]).expect("the leaf meets the profile");
    assert_eq!(leaf.attestation(), AttestationLevel::ChipProven);

    // A renewal without any attestation keeps the recorded level.
    let handed = nonce(&mut client).await;
    let reply = enrol(&mut client, &key, handed, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain: renewed, .. }) = reply else {
        panic!("a renewal keeps the stored level, got {reply:?}");
    };
    let renewed = DeviceCertificate::parse(&renewed[0]).expect("the renewal meets the profile");
    assert_eq!(renewed.attestation(), AttestationLevel::ChipProven);
}
