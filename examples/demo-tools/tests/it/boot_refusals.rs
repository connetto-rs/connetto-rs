//! `demo-server` reads its settings from the environment and refuses to boot,
//! naming the setting, on a value it cannot use.

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

use connetto_core::device_cert::layout::{ISSUER_CERTIFICATE, ISSUER_KEY, ROOT_CERTIFICATE};
use connetto_core::device_cert::{CertificateSerial, DeploymentId, RootCa};
use connetto_test_harness::server_fixture::{
    Authorization, NO_POLICIES, PG_SERIAL, reset_fixture, with_user_url,
};
use connetto_test_harness::{Fixture, isolated_session_keyring};
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::bb8::Pool;
use rcgen::{KeyPair, PKCS_ECDSA_P256_SHA256, PublicKeyData as _};

use super::e2e::{build_auth_stack, run_server_exit_output};

/// What `demo-ca init` and `demo-ca issuer` leave on disk, as the operator
/// copies it to the server: `root.der` beside an issuer directory.
fn ca_output(dir: &Path) -> (PathBuf, PathBuf) {
    let day = Duration::from_hours(24);
    let now = SystemTime::now();
    let root = RootCa::create(
        DeploymentId::from_uuid(uuid::Uuid::from_u128(7)),
        now - day,
        3650 * day,
    )
    .expect("root");
    let key = KeyPair::generate_for(&PKCS_ECDSA_P256_SHA256).expect("issuer key");
    let certificate = root
        .sign_issuer(
            &key.subject_public_key_info(),
            now - day,
            395 * day,
            CertificateSerial::new([2; 16]).expect("the serial is positive"),
        )
        .expect("issuer");
    let issuer_dir = dir.join("issuer-2026");
    std::fs::create_dir(&issuer_dir).expect("issuer dir");
    std::fs::write(issuer_dir.join(ISSUER_CERTIFICATE), certificate).expect("issuer.der");
    std::fs::write(issuer_dir.join(ISSUER_KEY), key.serialize_der()).expect("issuer.key");
    let root_path = dir.join(ROOT_CERTIFICATE);
    std::fs::write(&root_path, root.certificate()).expect("root.der");
    (root_path, issuer_dir)
}

/// The server refuses to boot on a `CONNETTO_COURIER` that is neither `on`
/// nor `off`, naming it.
#[tokio::test]
async fn the_server_refuses_an_unknown_courier_switch() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let url = fixture.admin_url().to_owned();
    let reader_url = with_user_url(&url, "app_reader", "app_reader");
    let auth_stack = build_auth_stack().await;
    let auth_env = auth_stack.env_pairs("http://127.0.0.1:0");
    let mut envs: Vec<(&str, &str)> = auth_env
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    envs.push(("CONNETTO_COURIER", "yes"));
    let output = run_server_exit_output(&url, Some(&reader_url), &envs).await;
    assert!(
        !output.status.success(),
        "the unknown switch refuses the boot"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("unknown CONNETTO_COURIER \"yes\""),
        "the refusal names the value, got: {stderr}"
    );
}

/// The server reads the device settings from `demo-ca`'s directories: a root
/// without an issuer directory refuses naming the setting, and an issuer
/// directory turns device identity on, so a deployment without the enrolment
/// tables refuses naming the first.
#[tokio::test]
async fn the_server_turns_device_identity_on_from_the_issuer_directory() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let manager =
        AsyncDieselConnectionManager::<AsyncPgConnection>::new(fixture.admin_url().to_owned());
    let pool = Pool::builder().build(manager).await.expect("build pool");
    reset_fixture(&pool, &fixture).await;
    fixture.provision_auth_tables().await;
    let url = fixture.admin_url().to_owned();
    let reader_url = with_user_url(&url, "app_reader", "app_reader");
    let auth_stack = build_auth_stack().await;
    let auth_env = auth_stack.env_pairs("http://127.0.0.1:0");
    let authorization = Authorization::provision(&fixture, NO_POLICIES).await;
    let ca = tempfile::TempDir::new().expect("ca dir");
    let (root, issuer_dir) = ca_output(ca.path());
    let (root, issuer_dir) = (root.display().to_string(), issuer_dir.display().to_string());
    let base: Vec<(&str, &str)> = auth_env
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .chain(authorization.env_pairs())
        .collect();

    let mut root_only = base.clone();
    root_only.push(("CONNETTO_DEVICE_ROOT", &root));
    let output = run_server_exit_output(&url, Some(&reader_url), &root_only).await;
    assert!(!output.status.success(), "a root alone refuses");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("CONNETTO_DEVICE_ROOT is set without CONNETTO_DEVICE_ISSUER_DIR"),
        "{stderr}"
    );

    let mut on = base;
    on.push(("CONNETTO_DEVICE_ROOT", &root));
    on.push(("CONNETTO_DEVICE_ISSUER_DIR", &issuer_dir));
    // The publication setup creates the enrolment base table, so the first
    // enrolment table the boot names is the certificate one (R77 decision 11).
    let output = run_server_exit_output(&url, Some(&reader_url), &on).await;
    assert!(!output.status.success(), "no enrolment tables refuses");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("table connetto_device_certificates does not exist"),
        "{stderr}"
    );
}

/// A device setting the server cannot use refuses boot naming the setting it
/// choked on, every refusal before the server binds.
#[tokio::test]
async fn the_server_refuses_unusable_device_settings() {
    let _keyring = isolated_session_keyring();
    let _serial = PG_SERIAL.lock().await;
    let fixture = Fixture::acquire().await;
    let url = fixture.admin_url().to_owned();
    let reader_url = with_user_url(&url, "app_reader", "app_reader");
    let auth_stack = build_auth_stack().await;
    let auth_env = auth_stack.env_pairs("http://127.0.0.1:0");
    let base: Vec<(&str, &str)> = auth_env
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let ca = tempfile::TempDir::new().expect("ca dir");
    let (root, issuer_dir) = ca_output(ca.path());
    let (root, issuer_dir) = (root.display().to_string(), issuer_dir.display().to_string());
    let missing = |name: &str| ca.path().join(name).display().to_string();
    let retired_dir = missing("no-such-issuer");
    let root_list = missing("no-such-list");
    let cases: Vec<(Vec<(&str, &str)>, &str)> = vec![
        (
            vec![("CONNETTO_DEVICE_ACCEPTED_ATTESTATION", "chip-proven,bogus")],
            "CONNETTO_DEVICE_ACCEPTED_ATTESTATION names \"bogus\"",
        ),
        (
            vec![
                ("CONNETTO_DEVICE_APP_ATTEST_APP_IDS", "TEAMID.bundle.id"),
                ("CONNETTO_DEVICE_APP_ATTEST_ENVIRONMENT", "staging"),
            ],
            "CONNETTO_DEVICE_APP_ATTEST_ENVIRONMENT is \"staging\"",
        ),
        (
            vec![("CONNETTO_DEVICE_CERT_CEILING_SECS", "soon")],
            "parsing CONNETTO_DEVICE_CERT_CEILING_SECS",
        ),
        (
            vec![("CONNETTO_DEVICE_RETIRED_ISSUER_DIRS", &retired_dir)],
            "reading the issuer certificate",
        ),
        (
            vec![("CONNETTO_DEVICE_ROOT_LIST", &root_list)],
            "reading the root's list",
        ),
    ];

    for (extra, expected) in cases {
        let mut envs = base.clone();
        envs.push(("CONNETTO_DEVICE_ROOT", root.as_str()));
        envs.push(("CONNETTO_DEVICE_ISSUER_DIR", issuer_dir.as_str()));
        envs.extend(extra);
        let output = run_server_exit_output(&url, Some(&reader_url), &envs).await;
        assert!(!output.status.success(), "{expected} refuses");
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(
            stderr.contains(expected),
            "the refusal names the setting, got: {stderr}"
        );
    }
}
