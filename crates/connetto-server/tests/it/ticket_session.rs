//! The content ticket mint asks whether a login's session is still live
//! (R109): a connection can stay open after its session ended, and the mint
//! refuses it a ticket rather than hand out one the file server refuses. A
//! ticket names the login's session, and a caller with no login is never
//! asked about.

use std::sync::{Arc, Mutex};

use connetto_core::LivenessUnknown;
use connetto_core::auth::ContentCaller;
use connetto_core::messages::{
    CONTENT_TICKET_REFUSED, CONTENT_TICKET_SIGNER_ERROR, ContentVerb, ControlMessage, NonFatalError,
};
use connetto_core::traits::Transport;
use connetto_server::{AbuseConfig, RequestGuard, ThrottleConfig, loopback};
use connetto_test_harness::{Fixture, RosterAuth};

use super::ticket_shared::{
    ADMITS_ALICE_OR_KEY, AnsweringAuthority, FILE_ID, KEY_GRANT, RecordingSigner,
    build_manager_answering, do_handshake_with, request_ticket, setup_reader_admitting,
};

/// The answer to one read ticket request of a caller presenting `grants`,
/// whose login sessions the store answers `live` for, with every caller the
/// signer minted for and how many questions the store was asked.
async fn ask(
    grants: &[&str],
    live: Result<bool, LivenessUnknown>,
) -> (ControlMessage, Vec<ContentCaller>, usize) {
    let fixture = Fixture::acquire().await;
    let reader_pool = setup_reader_admitting(&fixture, ADMITS_ALICE_OR_KEY).await;
    let minted = Arc::new(Mutex::new(Vec::new()));
    let asked = Arc::new(Mutex::new(0));
    let manager = build_manager_answering(
        reader_pool,
        RosterAuth::granting("alice"),
        Arc::new(RequestGuard::new(
            ThrottleConfig::new(),
            AbuseConfig::default(),
        )),
        RecordingSigner(Arc::clone(&minted)),
        connetto_core::auth::DEFAULT_USER_SETTING,
        Arc::new(AnsweringAuthority {
            live,
            asked: Arc::clone(&asked),
        }),
    );
    let (server_end, mut client) = loopback();
    let server = tokio::spawn(manager.serve(server_end));
    do_handshake_with(&mut client, "holder", grants).await;
    let answer = request_ticket(&mut client, "read", FILE_ID, ContentVerb::Read).await;
    client.close().await.expect("close");
    server.await.expect("join").expect("session ok");
    let minted = core::mem::take(
        &mut *minted
            .lock()
            .expect("the recording signer's mutex is never poisoned"),
    );
    let asked = *asked
        .lock()
        .expect("the question counter's mutex is never poisoned");
    (answer, minted, asked)
}

fn refused_with(answer: &ControlMessage, expected: &str, why: &str) {
    let ControlMessage::NonFatalError(NonFatalError { detail, .. }) = answer else {
        panic!("{why}: refused, not {answer:?}");
    };
    assert_eq!(detail, expected, "{why}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_live_login_mints_a_ticket_naming_its_session() {
    let (answer, minted, asked) = ask(&["user:alice"], Ok(true)).await;
    assert!(
        matches!(answer, ControlMessage::ContentTicketGrant(_)),
        "the live login is granted, not {answer:?}"
    );
    assert_eq!(asked, 1, "the mint asked about the login's session once");
    let [caller] = minted.as_slice() else {
        panic!("one ticket minted, not {minted:?}");
    };
    assert!(
        caller.session().is_some(),
        "the ticket names the login's session: {caller:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_connection_whose_session_ended_is_refused_a_ticket() {
    let (answer, minted, _) = ask(&["user:alice"], Ok(false)).await;
    refused_with(
        &answer,
        CONTENT_TICKET_REFUSED,
        "an ended session reads like an invisible file",
    );
    assert!(minted.is_empty(), "nothing was minted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_login_the_store_cannot_answer_for_is_refused_retryably() {
    let (answer, minted, _) = ask(
        &["user:alice"],
        Err(LivenessUnknown("the store is down".to_owned())),
    )
    .await;
    refused_with(
        &answer,
        CONTENT_TICKET_SIGNER_ERROR,
        "a store failure answers the retryable detail",
    );
    assert!(minted.is_empty(), "nothing was minted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_share_key_alone_mints_a_ticket_naming_no_session() {
    let (answer, minted, asked) = ask(
        &[KEY_GRANT],
        Err(LivenessUnknown("the store is down".to_owned())),
    )
    .await;
    assert!(
        matches!(answer, ControlMessage::ContentTicketGrant(_)),
        "a share key is granted while the session store is down, not {answer:?}"
    );
    assert_eq!(asked, 0, "the store was never asked");
    let [caller] = minted.as_slice() else {
        panic!("one ticket minted, not {minted:?}");
    };
    assert_eq!(caller.session(), None, "the ticket names no session");
}
