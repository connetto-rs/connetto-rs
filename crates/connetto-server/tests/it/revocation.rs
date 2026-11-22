//! Revoking a device and publishing the lists that say so (R74 step 5).

use std::sync::Arc;
use std::time::Duration;

use connetto_core::device_cert::{DeviceCertificate, KeyId, RevocationList};
use connetto_core::messages::{
    ControlMessage, DeviceRevokedAck, DevicesList, DevicesRequest, EnrolGrant, EnrolRefusal,
    FatalErrorReason, RevocationUpdate, RevokeDeviceRequest, SignedList,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_server::defaults::ConnettoDefaults;
use connetto_server::device_cert::{DeviceCertConfig, DeviceEnrolment, EnrolmentStore};
use connetto_server::{
    AuthHooks, CommittedNotice, DefaultAuthHooks, DeviceRevoked, EndCause, HookError, HookFuture,
    LoopbackTransport, SessionsEnded, VerifiedClaims,
};
use connetto_test_harness::Fixture;
use diesel_async::AsyncPgConnection;
use futures_util::FutureExt as _;
use parking_lot::Mutex;
use rcgen::{KeyPair, PublicKeyData as _};

use super::enrolment::{
    ask, connect, device_key, enrol, enrolling_manager, nonce, refused, rooted_issuer,
};

const BOUND: Duration = Duration::from_secs(20);

/// The next pushed revocation lists, within the bound.
async fn next_lists(client: &mut LoopbackTransport) -> Vec<SignedList> {
    tokio::time::timeout(BOUND, async {
        loop {
            match client.recv().await.expect("recv") {
                Some(IncomingFrame::Control(ControlMessage::RevocationUpdate(
                    RevocationUpdate { lists },
                ))) => return lists,
                Some(_) => {}
                None => panic!("the session closed before its lists came"),
            }
        }
    })
    .await
    .expect("the lists arrive within the bound")
}

/// The one list in `lists`, verified against `root`.
fn verified(lists: &[SignedList], root: &[u8]) -> RevocationList {
    let [list] = lists else {
        panic!("one issuer, one list, got {}", lists.len());
    };
    RevocationList::verify(&list.list, &list.signer, &[root.to_vec()]).expect("the list verifies")
}

/// Enrol `key` on `client` and return its certificate.
async fn enrolled(client: &mut LoopbackTransport, key: &KeyPair) -> DeviceCertificate {
    let nonce = nonce(client).await;
    let ControlMessage::EnrolGrant(EnrolGrant { chain, .. }) =
        enrol(client, key, nonce, None, vec![0xc0]).await
    else {
        panic!("expected a grant");
    };
    DeviceCertificate::parse(&chain[0]).expect("profile")
}

fn key_id(key: &KeyPair) -> KeyId {
    KeyId::of_public_key(&key.subject_public_key_info())
}

/// The reason `client` is closed with, within the bound.
async fn closed_by(client: &mut LoopbackTransport) -> FatalErrorReason {
    tokio::time::timeout(BOUND, async {
        loop {
            match client.recv().await.expect("recv") {
                Some(IncomingFrame::Control(ControlMessage::FatalError(fatal))) => {
                    return fatal.reason;
                }
                Some(_) => {}
                None => panic!("closed without a reason"),
            }
        }
    })
    .await
    .expect("the session closes within the bound")
}

/// Hooks recording committed session revocations.
#[derive(Default)]
struct RevokedSessions {
    sessions: Mutex<Vec<connetto_core::SessionId>>,
    committed: tokio::sync::Notify,
}

impl RevokedSessions {
    /// Count session ends after the first committed notice arrives.
    async fn count(&self) -> usize {
        let committed = self.committed.notified();
        if self.sessions.lock().is_empty() {
            tokio::time::timeout(BOUND, committed)
                .await
                .expect("session end notice");
        }
        self.sessions.lock().len()
    }
}

impl AuthHooks<ConnettoDefaults> for RevokedSessions {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn committed(&self, notice: CommittedNotice<String, ()>) {
        if let CommittedNotice::SessionsEnded {
            sessions,
            cause: EndCause::Revoked,
            ..
        } = notice
        {
            self.sessions.lock().extend(sessions);
            self.committed.notify_one();
        }
    }
}

/// A manager recording committed session revocations.
async fn revoking_manager(
    fixture: &Fixture,
    issuer: connetto_core::device_cert::DeviceIssuer,
) -> (
    Arc<super::ticket_shared::TicketManager<super::ticket_shared::OkSigner>>,
    Arc<RevokedSessions>,
) {
    let hooks = Arc::new(RevokedSessions::default());
    let revoked = Arc::clone(&hooks);
    let auth = fixture.enrolment_auth(hooks).await;
    let enrolment = DeviceEnrolment::new(DeviceCertConfig::new(issuer), auth);
    (enrolling_manager(fixture, Some(enrolment)).await, revoked)
}

/// Fail one list-number statement using a sequence that survives rollback.
async fn fail_next_list_number(fixture: &Fixture) {
    fixture.setup(&[
        "CREATE SEQUENCE fail_list_once",
        "CREATE FUNCTION fail_list_once() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF nextval('fail_list_once') = 1 THEN RAISE EXCEPTION 'list publication refused'; END IF; RETURN NEW; END $$",
        "CREATE TRIGGER fail_list_once BEFORE INSERT ON connetto_device_lists FOR EACH ROW EXECUTE FUNCTION fail_list_once()",
    ]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_handshake_and_a_grant_carry_the_current_list() {
    let fixture = Fixture::acquire().await;
    let (root, _, issuer) = rooted_issuer();
    let (manager, _) = revoking_manager(&fixture, issuer).await;
    let mut client = connect(&fixture, &manager, Some("alice")).await;
    let list = verified(&next_lists(&mut client).await, &root);
    assert!(list.number() >= 1);
    let nonce = nonce(&mut client).await;
    let reply = enrol(&mut client, &device_key(), nonce, None, vec![0xc0]).await;
    let ControlMessage::EnrolGrant(grant) = reply else {
        panic!("expected a grant, got {reply:?}");
    };
    assert_eq!(
        verified(&grant.revocation_lists, &root).number(),
        list.number()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_anonymous_session_gets_no_list() {
    let fixture = Fixture::acquire().await;
    let (_, _, issuer) = rooted_issuer();
    let (manager, _) = revoking_manager(&fixture, issuer).await;
    let mut client = connect(&fixture, &manager, None).await;
    let reply = ask(
        &mut client,
        ControlMessage::DevicesRequest(DevicesRequest {
            request_id: "d".into(),
        }),
    )
    .await;
    assert_eq!(reply, refused(EnrolRefusal::Unidentified, "d"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reporting_a_device_lost_closes_it_lists_it_and_revokes_its_session() {
    let fixture = Fixture::acquire().await;
    let (root, _, issuer) = rooted_issuer();
    let (manager, revoked_sessions) = revoking_manager(&fixture, issuer).await;
    let mut phone = connect(&fixture, &manager, Some("alice")).await;
    let mut laptop = connect(&fixture, &manager, Some("alice")).await;
    let phone_key = device_key();
    let laptop_key = device_key();
    let phone_cert = enrolled(&mut phone, &phone_key).await;
    let laptop_cert = enrolled(&mut laptop, &laptop_key).await;
    let before = verified(&manager_lists(&fixture, &manager).await, &root);
    assert!(!before.revokes(phone_cert.serial()));

    let reply = ask(
        &mut laptop,
        ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
            request_id: "r".into(),
            key_id: *key_id(&phone_key).as_bytes(),
        }),
    )
    .await;
    assert_eq!(
        reply,
        ControlMessage::DeviceRevokedAck(DeviceRevokedAck {
            request_id: "r".into()
        })
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "its auth-store session is revoked"
    );
    let after = verified(&next_lists(&mut laptop).await, &root);
    assert!(
        after.number() > before.number(),
        "a new list takes the next number"
    );
    assert!(after.revokes(phone_cert.serial()));
    assert!(!after.revokes(laptop_cert.serial()));

    let closed = tokio::time::timeout(BOUND, async {
        loop {
            match phone.recv().await.expect("recv") {
                Some(IncomingFrame::Control(ControlMessage::FatalError(fatal))) => {
                    return fatal.reason;
                }
                Some(_) => {}
                None => panic!("closed without a reason"),
            }
        }
    })
    .await
    .expect("the reported device is closed within the bound");
    assert_eq!(closed, FatalErrorReason::DeviceRevoked);
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "its auth-store session is revoked"
    );

    let ControlMessage::DevicesList(DevicesList { devices, .. }) = ask(
        &mut laptop,
        ControlMessage::DevicesRequest(DevicesRequest {
            request_id: "d".into(),
        }),
    )
    .await
    else {
        panic!("expected the device list");
    };
    assert_eq!(devices.len(), 2);
    let phone_entry = devices
        .iter()
        .find(|device| device.key_id == *key_id(&phone_key).as_bytes())
        .expect("the phone is listed");
    assert!(phone_entry.revoked_at_secs.is_some());
    assert_eq!(phone_entry.descriptor, vec![0xc0]);
    let laptop_entry = devices
        .iter()
        .find(|device| device.key_id == *key_id(&laptop_key).as_bytes())
        .expect("the laptop is listed");
    assert!(laptop_entry.revoked_at_secs.is_none());

    let mut thief = connect(&fixture, &manager, Some("alice")).await;
    let nonce = nonce(&mut thief).await;
    assert_eq!(
        enrol(&mut thief, &phone_key, nonce, None, vec![0xc0]).await,
        refused(EnrolRefusal::Revoked, "e"),
        "the revoked key cannot renew"
    );
}

/// The lists the manager publishes now, read through a fresh session.
async fn manager_lists(
    fixture: &Fixture,
    manager: &Arc<super::ticket_shared::TicketManager<super::ticket_shared::OkSigner>>,
) -> Vec<SignedList> {
    let mut reader = connect(fixture, manager, Some("alice")).await;
    next_lists(&mut reader).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn only_the_owner_revokes_and_an_unknown_key_is_refused() {
    let fixture = Fixture::acquire().await;
    let (_, _, issuer) = rooted_issuer();
    let (manager, revoked_sessions) = revoking_manager(&fixture, issuer).await;
    let mut alice = connect(&fixture, &manager, Some("alice")).await;
    let mut bob = connect(&fixture, &manager, Some("bob")).await;
    let alice_key = device_key();
    enrolled(&mut alice, &alice_key).await;
    for (key, what) in [
        (key_id(&alice_key), "another account's key"),
        (KeyId::from_bytes([7; 32]), "an unknown key"),
    ] {
        let reply = ask(
            &mut bob,
            ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
                request_id: "r".into(),
                key_id: *key.as_bytes(),
            }),
        )
        .await;
        assert_eq!(reply, refused(EnrolRefusal::InvalidRequest, "r"), "{what}");
    }
    assert!(revoked_sessions.sessions.lock().is_empty());
    let nonce = nonce(&mut bob).await;
    assert_eq!(
        enrol(&mut bob, &alice_key, nonce, None, vec![0xc0]).await,
        refused(EnrolRefusal::InvalidRequest, "e"),
        "a key enrolled under one account is refused to another"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_operator_revokes_any_key_and_a_repeat_republishes() {
    let fixture = Fixture::acquire().await;
    let (root, _, issuer) = rooted_issuer();
    let (manager, revoked_sessions) = revoking_manager(&fixture, issuer).await;
    let mut alice = connect(&fixture, &manager, Some("alice")).await;
    let mut watcher = connect(&fixture, &manager, Some("bob")).await;
    let _ = next_lists(&mut watcher).await;
    let key = device_key();
    let cert = enrolled(&mut alice, &key).await;
    assert!(
        manager
            .revoke_device(key_id(&key))
            .await
            .expect("the first revoke"),
        "a fresh revoke is new"
    );
    let first = verified(&next_lists(&mut watcher).await, &root);
    assert!(
        first.revokes(cert.serial()),
        "every identified session hears of it"
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the fresh revoke revokes the session"
    );
    assert!(
        !manager
            .revoke_device(key_id(&key))
            .await
            .expect("the repeat"),
        "a second revoke answers that nothing is new"
    );
    let second = verified(&next_lists(&mut watcher).await, &root);
    assert!(
        second.number() > first.number(),
        "a second revoke still publishes a new list"
    );
    assert!(
        second.revokes(cert.serial()),
        "the repeat's list revokes the key"
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "an ended session emits no duplicate notice"
    );
    assert!(
        manager
            .revoke_device(KeyId::from_bytes([1; 32]))
            .await
            .is_err(),
        "an unenrolled key is not a device"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_publish_is_retried_by_the_next_revoke() {
    let fixture = Fixture::acquire().await;
    let (root, _, issuer) = rooted_issuer();
    let (manager, revoked_sessions) = revoking_manager(&fixture, issuer).await;
    let mut alice = connect(&fixture, &manager, Some("alice")).await;
    let mut watcher = connect(&fixture, &manager, Some("bob")).await;
    let _ = next_lists(&mut watcher).await;
    let key = device_key();
    let cert = enrolled(&mut alice, &key).await;
    // The first revoke records the revocation but the publish fails.
    fail_next_list_number(&fixture).await;
    assert!(
        manager.revoke_device(key_id(&key)).await.is_err(),
        "a failed publish is unavailable"
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the session end committed"
    );
    assert!(
        !manager
            .revoke_device(key_id(&key))
            .await
            .expect("the retry"),
        "the key is already revoked"
    );
    let pushed = verified(&next_lists(&mut watcher).await, &root);
    assert!(
        pushed.revokes(cert.serial()),
        "the retry's list revokes the key"
    );
    let closed = closed_by(&mut alice).await;
    assert_eq!(closed, FatalErrorReason::DeviceRevoked);
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the session end is reported once"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_owners_second_revoke_still_closes_the_device() {
    let fixture = Fixture::acquire().await;
    let (root, _, issuer) = rooted_issuer();
    let (manager, revoked_sessions) = revoking_manager(&fixture, issuer).await;
    let mut device = connect(&fixture, &manager, Some("alice")).await;
    let mut owner = connect(&fixture, &manager, Some("alice")).await;
    let mut watcher = connect(&fixture, &manager, Some("bob")).await;
    let key = device_key();
    let cert = enrolled(&mut device, &key).await;
    let _ = next_lists(&mut watcher).await;
    // The first revoke answers unavailable because its publish fails.
    fail_next_list_number(&fixture).await;
    let reply = ask(
        &mut owner,
        ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
            request_id: "r1".into(),
            key_id: *key_id(&key).as_bytes(),
        }),
    )
    .await;
    assert_eq!(
        reply,
        refused(EnrolRefusal::IssuerUnavailable, "r1"),
        "a failed publish leaves the device reachable"
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the session end committed"
    );
    let reply = ask(
        &mut owner,
        ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
            request_id: "r2".into(),
            key_id: *key_id(&key).as_bytes(),
        }),
    )
    .await;
    assert_eq!(
        reply,
        ControlMessage::DeviceRevokedAck(DeviceRevokedAck {
            request_id: "r2".into()
        })
    );
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the second revoke revokes the device's session"
    );
    let pushed = verified(&next_lists(&mut watcher).await, &root);
    assert!(
        pushed.revokes(cert.serial()),
        "the second revoke's list revokes the key"
    );
    let closed = closed_by(&mut device).await;
    assert_eq!(closed, FatalErrorReason::DeviceRevoked);
    assert_eq!(
        revoked_sessions.count().await,
        1,
        "the session end is reported once"
    );
}

/// An issuer as its certificate and its PKCS #8 key.
type IssuerBytes = (Vec<u8>, Vec<u8>);

/// A root and two issuers it signed, each as its certificate and PKCS #8 key,
/// so a test loads one issuer into two configurations.
fn root_and_two_issuers() -> (connetto_core::device_cert::RootCa, IssuerBytes, IssuerBytes) {
    use connetto_core::device_cert::{CertificateSerial, DeploymentId, RootCa};
    let now = std::time::SystemTime::now();
    let day = Duration::from_hours(24);
    let root = RootCa::create(
        DeploymentId::from_uuid(uuid::Uuid::from_u128(0x0f0f)),
        now - day,
        3650 * day,
    )
    .expect("root");
    let issuer = |serial: u8| {
        let key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).expect("issuer key");
        let cert = root
            .sign_issuer(
                &key.subject_public_key_info(),
                now - day,
                395 * day,
                CertificateSerial::new([serial; 16]).expect("the serial is positive"),
            )
            .expect("issuer");
        (cert, key.serialize_der())
    };
    let (old, new) = (issuer(1), issuer(2));
    (root, old, new)
}

fn load(
    root: &connetto_core::device_cert::RootCa,
    (cert, key): &IssuerBytes,
) -> connetto_core::device_cert::DeviceIssuer {
    connetto_core::device_cert::DeviceIssuer::from_pkcs8(cert.clone(), key, root.certificate())
        .expect("load the issuer")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retired_issuer_keeps_listing_its_certificates_beside_the_roots_list() {
    use connetto_core::device_cert::Revoked;
    let fixture = Fixture::acquire().await;
    let store = fixture.enrolment_auth(Arc::new(DefaultAuthHooks)).await;
    let (root, old, new) = root_and_two_issuers();
    let roots = [root.certificate().to_vec()];

    let before = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(load(&root, &old)),
            Arc::clone(&store) as _,
        )),
    )
    .await;
    let mut device = connect(&fixture, &before, Some("alice")).await;
    let key = device_key();
    let cert = enrolled(&mut device, &key).await;

    // The root revokes some third issuer offline, and the operator rotates.
    let now = std::time::SystemTime::now();
    let root_list = SignedList {
        list: root
            .sign_list(
                1,
                &[Revoked {
                    serial: vec![9; 16],
                    at: now,
                }],
                now,
                now + Duration::from_hours(24 * 400),
            )
            .expect("the root signs"),
        signer: root.certificate().to_vec(),
    };
    let after = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(load(&root, &new))
                .with_retired_issuer(load(&root, &old))
                .with_root_list(root_list),
            Arc::clone(&store) as _,
        )),
    )
    .await;
    assert!(after.revoke_device(key_id(&key)).await.expect("revoke"));
    let lists = manager_lists(&fixture, &after).await;
    assert_eq!(
        lists.len(),
        3,
        "the current issuer, the retired one, the root"
    );
    let by_signer = |signer: &[u8]| {
        let list = lists
            .iter()
            .find(|list| list.signer == signer)
            .expect("a list from that signer");
        RevocationList::verify(&list.list, &list.signer, &roots).expect("verifies")
    };
    assert!(
        by_signer(&old.0).revokes(cert.serial()),
        "the retired issuer lists its own"
    );
    assert!(
        !by_signer(&new.0).revokes(cert.serial()),
        "the new issuer signed none of it"
    );
    assert!(by_signer(root.certificate()).revokes(&[9; 16]));
}

struct BlockedRevocation {
    started: Mutex<Option<tokio::sync::oneshot::Sender<connetto_core::SessionId>>>,
    release: Mutex<Option<tokio::sync::oneshot::Receiver<connetto_core::SessionId>>>,
    committed: Mutex<Option<connetto_core::SessionId>>,
}

impl AuthHooks<ConnettoDefaults> for BlockedRevocation {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn sessions_ended<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        ended: &'a SessionsEnded<'a, String>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            let [session] = ended.sessions else {
                panic!("one session is revoked");
            };
            self.started
                .lock()
                .take()
                .expect("one revocation")
                .send(*session)
                .expect("revocation observer");
            let release = self.release.lock().take().expect("one release");
            let released = tokio::time::timeout(BOUND, release)
                .await
                .expect("revocation release within the bound")
                .expect("revocation release");
            assert_eq!(released, *session);
            Ok(())
        })
    }

    fn committed(&self, notice: CommittedNotice<String, ()>) {
        if let CommittedNotice::SessionsEnded { sessions, .. } = notice {
            assert_eq!(sessions.len(), 1);
            *self.committed.lock() = Some(sessions[0]);
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn self_revocation_acknowledges_completion_before_closing() {
    let fixture = Fixture::acquire().await;
    let (_, _, issuer) = rooted_issuer();
    let (started, started_receiver) = tokio::sync::oneshot::channel();
    let (release, release_receiver) = tokio::sync::oneshot::channel();
    let hooks = Arc::new(BlockedRevocation {
        started: Mutex::new(Some(started)),
        release: Mutex::new(Some(release_receiver)),
        committed: Mutex::new(None),
    });
    let auth = fixture
        .enrolment_auth(Arc::<BlockedRevocation>::clone(&hooks))
        .await;
    let enrolment = DeviceEnrolment::new(DeviceCertConfig::new(issuer), auth);
    let manager = enrolling_manager(&fixture, Some(enrolment)).await;
    let mut device = connect(&fixture, &manager, Some("alice")).await;
    let key = device_key();
    let _ = enrolled(&mut device, &key).await;
    device
        .send_control(ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
            request_id: "self-revoke".into(),
            key_id: *key_id(&key).as_bytes(),
        }))
        .await
        .expect("request self-revocation");
    let session = tokio::time::timeout(BOUND, started_receiver)
        .await
        .expect("revoker starts within the bound")
        .expect("revoker started");
    assert!(
        device.recv().now_or_never().is_none(),
        "neither acknowledgement nor closure precedes revoker completion"
    );
    release.send(session).expect("release the session revoker");
    let reply = tokio::time::timeout(BOUND, device.recv())
        .await
        .expect("acknowledgement within the bound")
        .expect("receive acknowledgement");
    assert_eq!(
        reply,
        Some(IncomingFrame::Control(ControlMessage::DeviceRevokedAck(
            DeviceRevokedAck {
                request_id: "self-revoke".into()
            }
        )))
    );
    assert_eq!(*hooks.committed.lock(), Some(session));
    assert_eq!(
        closed_by(&mut device).await,
        FatalErrorReason::DeviceRevoked
    );
}

/// The hook whose refusal a rollback test exercises.
#[derive(Clone, Copy)]
enum RefusedHook {
    Device,
    Session,
}

impl AuthHooks<ConnettoDefaults> for RefusedHook {
    fn user_for<'a>(
        &'a self,
        transaction: &'a mut AsyncPgConnection,
        claims: &'a VerifiedClaims,
    ) -> HookFuture<'a, String> {
        AuthHooks::<ConnettoDefaults>::user_for(&DefaultAuthHooks, transaction, claims)
    }

    fn device_revoked<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        _revoked: &'a DeviceRevoked<'a, String>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            if matches!(self, Self::Device) {
                Err(HookError::Refused("keep this device".to_owned()))
            } else {
                Ok(())
            }
        })
    }

    fn sessions_ended<'a>(
        &'a self,
        _transaction: &'a mut AsyncPgConnection,
        ended: &'a SessionsEnded<'a, String>,
    ) -> HookFuture<'a, ()> {
        Box::pin(async move {
            assert_eq!(ended.cause, EndCause::Revoked);
            if matches!(self, Self::Session) {
                Err(HookError::Refused("keep this session".to_owned()))
            } else {
                Ok(())
            }
        })
    }
}

/// A refused self-revocation preserves the key, session, and connection.
async fn refused_self_revocation(hook: RefusedHook, reason: &str) {
    let fixture = Fixture::acquire().await;
    let auth = fixture.enrolment_auth(Arc::new(hook)).await;
    let (_, _, issuer) = rooted_issuer();
    let manager = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(issuer),
            Arc::clone(&auth),
        )),
    )
    .await;
    let mut device = connect(&fixture, &manager, Some("alice")).await;
    let key = device_key();
    enrolled(&mut device, &key).await;
    let store = EnrolmentStore::<ConnettoDefaults>::default();
    let session = {
        let mut connection = fixture.admin().get().await.expect("admin connection");
        let devices = store
            .devices(&mut connection, &"alice".to_owned())
            .await
            .expect("devices");
        assert_eq!(devices.len(), 1);
        devices[0].session
    };
    let reply = tokio::time::timeout(
        BOUND,
        ask(
            &mut device,
            ControlMessage::RevokeDeviceRequest(RevokeDeviceRequest {
                request_id: "self-revoke".into(),
                key_id: *key_id(&key).as_bytes(),
            }),
        ),
    )
    .await
    .expect("self-revocation answers");
    assert_eq!(
        reply,
        refused(
            EnrolRefusal::Refused {
                reason: reason.to_owned()
            },
            "self-revoke"
        )
    );
    assert!(auth.session_is_live(session).await.expect("session state"));
    {
        let mut connection = fixture.admin().get().await.expect("admin connection");
        let devices = store
            .devices(&mut connection, &"alice".to_owned())
            .await
            .expect("devices");
        assert_eq!(devices.len(), 1);
        assert!(devices[0].revoked_at.is_none());
    }
    tokio::time::timeout(BOUND, enrolled(&mut device, &key))
        .await
        .expect("the same connection renews the key");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_device_revoked_hook_preserves_the_device_and_session() {
    refused_self_revocation(RefusedHook::Device, "keep this device").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_sessions_ended_hook_rolls_back_the_key_revocation() {
    refused_self_revocation(RefusedHook::Session, "keep this session").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revocation_commits_the_device_then_its_last_enrolling_session() {
    let fixture = Fixture::acquire().await;
    let (hooks, mut notices) = super::auth_hooks::Told::<ConnettoDefaults>::channel();
    let auth = fixture.enrolment_auth(hooks).await;
    let (_, _, issuer) = rooted_issuer();
    let manager = enrolling_manager(
        &fixture,
        Some(DeviceEnrolment::new(
            DeviceCertConfig::new(issuer),
            Arc::clone(&auth),
        )),
    )
    .await;
    let mut original = connect(&fixture, &manager, Some("alice")).await;
    let key = device_key();
    enrolled(&mut original, &key).await;
    let first_session = match tokio::time::timeout(BOUND, notices.recv())
        .await
        .expect("enrolled notice")
        .expect("notice channel")
    {
        CommittedNotice::DeviceEnrolled {
            session,
            key: enrolled_key,
            ..
        } => {
            assert_eq!(enrolled_key, key_id(&key));
            session.session_id
        }
        notice => panic!("unexpected notice {notice:?}"),
    };
    let mut renewed = connect(&fixture, &manager, Some("alice")).await;
    enrolled(&mut renewed, &key).await;
    let last_session = {
        let mut connection = fixture.admin().get().await.expect("admin connection");
        EnrolmentStore::<ConnettoDefaults>::default()
            .devices(&mut connection, &"alice".to_owned())
            .await
            .expect("devices")[0]
            .session
    };
    assert_ne!(first_session, last_session);
    assert!(manager.revoke_device(key_id(&key)).await.expect("revoke"));
    match tokio::time::timeout(BOUND, notices.recv())
        .await
        .expect("device notice")
        .expect("notice channel")
    {
        CommittedNotice::DeviceRevoked {
            user_id,
            key: revoked_key,
        } => {
            assert_eq!(user_id, "alice");
            assert_eq!(revoked_key, key_id(&key));
        }
        notice => panic!("unexpected notice {notice:?}"),
    }
    match tokio::time::timeout(BOUND, notices.recv())
        .await
        .expect("session notice")
        .expect("notice channel")
    {
        CommittedNotice::SessionsEnded {
            user_id,
            sessions,
            cause,
        } => {
            assert_eq!(user_id, "alice");
            assert_eq!(sessions, vec![last_session]);
            assert_eq!(cause, EndCause::Revoked);
        }
        notice => panic!("unexpected notice {notice:?}"),
    }
    assert!(
        auth.session_is_live(first_session)
            .await
            .expect("original session")
    );
    assert!(
        !auth
            .session_is_live(last_session)
            .await
            .expect("renewing session")
    );
    assert!(notices.try_recv().is_err());
}
