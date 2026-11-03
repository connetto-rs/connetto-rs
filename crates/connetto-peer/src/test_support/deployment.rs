//! A deployment minted in memory: a root, its issuers and the device
//! certificates they sign, and nodes serving them on loopback.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use connetto_core::device_cert::{
    AttestationLevel, CertificateRequest, CertificateSerial, CertificateSigner, DeploymentId,
    DeviceCertificate, DeviceIdentity, DeviceIssuer, DeviceKey, DeviceKeyError, KeyHome, KeyId,
    Revoked, RootCa, key_id, public_key_info,
};
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, SigningKey as RcgenSigningKey};
use tokio::sync::mpsc;

use crate::{Identity, Node, PeerEvent, SystemClock, Trust};

/// One day, the unit the fixture's validity windows are counted in.
pub const DAY: Duration = Duration::from_hours(24);

/// A test serial as the issuer takes it, its first byte positive.
///
/// # Panics
///
/// When the first byte is not positive.
#[must_use]
pub fn positive(bytes: [u8; 16]) -> CertificateSerial {
    CertificateSerial::new(bytes).expect("the test serial is positive")
}

/// A device key held in memory, standing in for a chip.
struct Device(Arc<KeyPair>);

impl Clone for Device {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

impl DeviceKey for Device {
    fn public_point(&self) -> [u8; 65] {
        self.0
            .public_key_raw()
            .try_into()
            .expect("an uncompressed P-256 point")
    }

    fn sign(&self, message: &[u8]) -> Result<Vec<u8>, DeviceKeyError> {
        self.0
            .sign(message)
            .map_err(|err| DeviceKeyError::Platform(Box::new(err)))
    }

    fn home(&self) -> KeyHome {
        KeyHome::Software
    }
}

/// An issuer beside the certificate and key it signs with.
pub struct Issuer {
    /// The issuer certificate, DER.
    certificate: Vec<u8>,
    /// The signing key, for the lists it signs.
    signer: DeviceIssuer,
    /// The serial the root used when signing the issuer.
    pub serial: [u8; 16],
}

/// A deployment a test mints: one root and the issuers and devices it
/// signs.
pub struct Deployment {
    root: RootCa,
}

impl Deployment {
    /// Mint a root for deployment `id`, valid around `now`.
    ///
    /// # Panics
    ///
    /// When the root does not mint.
    #[must_use]
    pub fn new(id: u128, now: SystemTime) -> Self {
        let root = RootCa::create(
            DeploymentId::from_uuid(uuid::Uuid::from_u128(id)),
            now - 10 * DAY,
            3650 * DAY,
        )
        .expect("the root mints");
        Self { root }
    }

    /// The root's DER, what a device is built with.
    #[must_use]
    pub fn root_der(&self) -> Vec<u8> {
        self.root.certificate().to_vec()
    }

    /// The root as a signer, so a proof can mint a leaf under its own key.
    ///
    /// # Panics
    ///
    /// When the root's own key does not load as a signer.
    #[must_use]
    pub fn root_signer(&self) -> DeviceIssuer {
        DeviceIssuer::from_pkcs8(
            self.root.certificate().to_vec(),
            &self.root.private_key_der(),
            self.root.certificate(),
        )
        .expect("the root signs its own name")
    }

    /// Sign one more issuer, valid around `now`.
    ///
    /// # Panics
    ///
    /// When the issuer's key or certificate does not mint.
    #[must_use]
    pub fn add_issuer(&self, now: SystemTime, serial: [u8; 16]) -> Issuer {
        let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("the issuer key");
        let holder = Device(Arc::new(key));
        let stored = rustls_pki_types::PrivatePkcs8KeyDer::from(holder.0.serialize_der());
        let key = KeyPair::from_pkcs8_der_and_sign_algo(&stored, &PKCS_ECDSA_P256_SHA256)
            .expect("the key round-trips");
        let certificate = self
            .root
            .sign_issuer(
                &public_key_info(&holder),
                now - 10 * DAY,
                366 * DAY,
                positive(serial),
            )
            .expect("the issuer signs");
        let signer = DeviceIssuer::new(certificate.clone(), key, self.root.certificate())
            .expect("the issuer loads");
        Issuer {
            certificate,
            signer,
            serial,
        }
    }

    /// A device certificate under `issuer`, valid from `not_before` for
    /// `lifetime`, carrying `bot_list`.
    ///
    /// # Panics
    ///
    /// When the device key or its certificate does not mint.
    #[expect(
        clippy::too_many_arguments,
        reason = "the serial and window join the identity and level the one fixture certificate carries, and a request struct would hide the same arity behind another type"
    )]
    #[must_use]
    pub fn device_with_bot_list(
        &self,
        issuer: &Issuer,
        account: &str,
        not_before: SystemTime,
        lifetime: Duration,
        serial: [u8; 16],
        level: AttestationLevel,
        bot_list: &str,
    ) -> Peer {
        let key = Device(Arc::new(
            KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("the device key"),
        ));
        let csr =
            CertificateRequest::build(&CertificateSigner::new(&key), &[1; 32]).expect("the csr");
        let request = CertificateRequest::parse(&csr).expect("the request parses");
        let leaf = issuer
            .signer
            .issue(
                &request,
                account,
                not_before,
                lifetime,
                positive(serial),
                level,
                bot_list,
            )
            .expect("the certificate issues");
        Peer::parsed(key, level, leaf, issuer)
    }

    /// A device certificate under `issuer`, valid from `not_before` for `lifetime`, the
    /// person's wildcard the fixture binds.
    ///
    /// # Panics
    ///
    /// When the device key or its certificate does not mint.
    #[must_use]
    pub fn device(
        &self,
        issuer: &Issuer,
        account: &str,
        not_before: SystemTime,
        lifetime: Duration,
        serial: [u8; 16],
        level: AttestationLevel,
    ) -> Peer {
        self.device_with_bot_list(
            issuer,
            account,
            not_before,
            lifetime,
            serial,
            level,
            connetto_core::BOT_LIST_WILDCARD,
        )
    }

    /// Reissue the device `peer` under `issuer`, a new serial and window,
    /// carrying the peer's own bot list as a renewal does.
    ///
    /// # Panics
    ///
    /// When the reissued certificate does not mint.
    #[must_use]
    pub fn reissue(
        &self,
        issuer: &Issuer,
        peer: &Peer,
        not_before: SystemTime,
        lifetime: Duration,
        serial: [u8; 16],
    ) -> Peer {
        let key = peer.key.clone();
        let csr =
            CertificateRequest::build(&CertificateSigner::new(&key), &[1; 32]).expect("the csr");
        let request = CertificateRequest::parse(&csr).expect("the request parses");
        let leaf = issuer
            .signer
            .issue(
                &request,
                peer.identity.account(),
                not_before,
                lifetime,
                positive(serial),
                peer.level,
                &peer.bot_list,
            )
            .expect("the reissue grants");
        Peer::parsed(key, peer.level, leaf, issuer)
    }

    /// An issuer-signed list `number` revoking `revoked`, dated at `now`.
    ///
    /// # Panics
    ///
    /// When the list does not sign.
    #[must_use]
    pub fn issuer_list(
        &self,
        issuer: &Issuer,
        number: u64,
        revoked: &[Revoked],
        now: SystemTime,
    ) -> (Vec<u8>, Vec<u8>) {
        let list = issuer
            .signer
            .sign_list(number, revoked, now, now + 7 * DAY)
            .expect("the list signs");
        (list, issuer.certificate.clone())
    }

    /// A root-signed list `number` revoking `revoked`, dated at `now`.
    ///
    /// # Panics
    ///
    /// When the list does not sign.
    #[must_use]
    pub fn root_list(
        &self,
        number: u64,
        revoked: &[Revoked],
        now: SystemTime,
    ) -> (Vec<u8>, Vec<u8>) {
        let list = self
            .root
            .sign_list(number, revoked, now, now + 7 * DAY)
            .expect("the list signs");
        (list, self.root_der())
    }
}

/// A device certificate beside the key that signs for it.
pub struct Peer {
    /// The key, held in memory.
    key: Device,
    /// What the certificate names.
    pub identity: DeviceIdentity,
    /// The serial a list names the leaf by.
    pub serial: Vec<u8>,
    /// The attestation level the leaf records.
    level: AttestationLevel,
    /// The bot list the leaf carries.
    pub bot_list: String,
    /// The leaf, DER.
    pub leaf: Vec<u8>,
    /// The issuer certificate, DER.
    issuer: Vec<u8>,
}

impl Peer {
    /// The peer `leaf` names, signed for by `key` under `issuer`.
    fn parsed(key: Device, level: AttestationLevel, leaf: Vec<u8>, issuer: &Issuer) -> Self {
        let parsed = DeviceCertificate::parse(&leaf).expect("the profile holds");
        Self {
            key,
            identity: parsed.identity().clone(),
            serial: parsed.serial().to_vec(),
            level,
            bot_list: parsed
                .bot_list()
                .expect("the fixture always issues the extension")
                .to_owned(),
            leaf,
            issuer: issuer.certificate.clone(),
        }
    }

    /// The presented identity the node serves and dials with.
    #[must_use]
    pub fn identity(&self) -> Identity {
        Identity {
            certificate: self.leaf.clone(),
            issuer: self.issuer.clone(),
            key: Arc::new(self.key.clone()),
        }
    }

    /// The key id, what the duplicate rule compares.
    #[must_use]
    pub fn key_id(&self) -> KeyId {
        key_id(&self.key)
    }

    /// The key's DER public key info, what a link reports.
    #[must_use]
    pub fn public_key_info(&self) -> Vec<u8> {
        public_key_info(&self.key)
    }
}

/// A node trusting one deployment root at every attestation level, on the
/// system clock.
///
/// # Panics
///
/// When the root holds no key.
#[must_use]
pub fn node(root_der: Vec<u8>, events: mpsc::UnboundedSender<PeerEvent>) -> Node {
    Node::new(
        Trust {
            roots: vec![root_der],
            accepted: AttestationLevel::ALL.to_vec(),
        },
        Arc::new(SystemClock),
        events,
    )
    .expect("the roots hold keys")
}

/// An event channel pair.
#[must_use]
pub fn events() -> (
    mpsc::UnboundedSender<PeerEvent>,
    mpsc::UnboundedReceiver<PeerEvent>,
) {
    mpsc::unbounded_channel()
}

/// The whole second the wall clock stands on, as X.509 carries time.
///
/// # Panics
///
/// When the wall clock stands before the epoch.
#[must_use]
pub fn whole_second() -> SystemTime {
    let secs = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .expect("after the epoch")
        .as_secs();
    SystemTime::UNIX_EPOCH + Duration::from_secs(secs)
}

/// Serve `peer` on loopback, answering the bound address.
///
/// # Panics
///
/// When the loopback listener does not bind.
pub fn serve(node: &Node, peer: &Peer) -> SocketAddr {
    node.serve(SocketAddr::from(([127, 0, 0, 1], 0)), peer.identity())
        .expect("the listener binds")
}
