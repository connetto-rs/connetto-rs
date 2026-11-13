//! Write tickets a caller carries for another account (R79 decision 17): minted
//! only when the deployment's `connetto_file_owner` answers that account for a
//! file the caller can see, and only for a caller holding an account, every
//! refusal reading like an invisible file.

use std::sync::Arc;

use connetto_core::auth::ContentCarry;
use connetto_core::messages::{
    CONTENT_TICKET_REFUSED, ContentTicketGrant, ContentVerb, ControlMessage, NonFatalError,
};
use connetto_core::traits::Transport;
use connetto_server::ThrottleConfig;
use connetto_test_harness::{Fixture, RosterAuth};

use super::ticket_shared::{
    ADMITS_ALICE, ADMITS_KEY, CarrySigner, FILE_ID, KEY_GRANT, open_session_with_grants,
    open_session_with_handshake, request_ticket_for, setup_reader_with_owner,
};

const WRITE: ContentVerb = ContentVerb::Write { declared_len: 10 };

/// The answer to one ticket request of `grants` against a deployment whose
/// visibility predicate is `visible` and whose file owner is `owner`, and
/// every carry the signer was asked to mint.
async fn ask(
    visible: &str,
    owner: &str,
    grants: &[&str],
    verb: ContentVerb,
    for_owner: Option<&str>,
) -> (ControlMessage, Vec<Option<ContentCarry>>) {
    let fixture = Fixture::acquire().await;
    let reader_pool = setup_reader_with_owner(&fixture, visible, owner).await;
    let minted = Arc::default();
    let signer = CarrySigner(Arc::clone(&minted));
    let throttle = ThrottleConfig::new();
    let (mut client, server) = if grants == ["user:alice"] {
        open_session_with_handshake(
            reader_pool,
            RosterAuth::granting("alice"),
            signer,
            &throttle,
            "alice",
        )
        .await
    } else {
        open_session_with_grants(
            reader_pool,
            RosterAuth::granting("alice"),
            signer,
            &throttle,
            "holder",
            grants,
        )
        .await
    };
    let answer = request_ticket_for(&mut client, "carry", FILE_ID, verb, for_owner).await;
    client.close().await.expect("close");
    server.await.expect("join").expect("session ok");
    let minted = core::mem::take(
        &mut *minted
            .lock()
            .expect("the carry signer's mutex is never poisoned"),
    );
    (answer, minted)
}

fn assert_refused(answer: &ControlMessage, minted: &[Option<ContentCarry>], why: &str) {
    let ControlMessage::NonFatalError(NonFatalError { detail, .. }) = answer else {
        panic!("{why}: refused, not {answer:?}");
    };
    assert_eq!(
        detail, CONTENT_TICKET_REFUSED,
        "{why}: read like an invisible file"
    );
    assert!(minted.is_empty(), "{why}: nothing was minted");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_for_the_owner_the_function_answers_carries_the_owner_and_session() {
    let (answer, minted) = ask(ADMITS_ALICE, "'bob'", &["user:alice"], WRITE, Some("bob")).await;
    assert!(
        matches!(
            answer,
            ControlMessage::ContentTicketGrant(ContentTicketGrant { .. })
        ),
        "{answer:?}"
    );
    let [Some(carry)] = minted.as_slice() else {
        panic!("one carried mint, not {minted:?}");
    };
    assert_eq!(carry.owner, "bob");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_write_naming_no_owner_carries_nothing() {
    let (answer, minted) = ask(ADMITS_ALICE, "'bob'", &["user:alice"], WRITE, None).await;
    assert!(
        matches!(answer, ControlMessage::ContentTicketGrant(_)),
        "{answer:?}"
    );
    assert_eq!(minted, [None]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_for_owner_the_function_denies_is_refused() {
    let (answer, minted) = ask(ADMITS_ALICE, "'carol'", &["user:alice"], WRITE, Some("bob")).await;
    assert_refused(&answer, &minted, "the function answers another account");
    let (answer, minted) = ask(ADMITS_ALICE, "NULL", &["user:alice"], WRITE, Some("bob")).await;
    assert_refused(&answer, &minted, "the function answers nobody");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_invisible_file_is_refused_whatever_its_owner() {
    let (answer, minted) = ask("false", "'bob'", &["user:alice"], WRITE, Some("bob")).await;
    assert_refused(&answer, &minted, "the caller cannot see the file");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carried_read_is_refused() {
    let (answer, minted) = ask(
        ADMITS_ALICE,
        "'bob'",
        &["user:alice"],
        ContentVerb::Read,
        Some("bob"),
    )
    .await;
    assert_refused(&answer, &minted, "only a write is carried");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_caller_holding_no_account_is_refused_a_carry() {
    let (answer, minted) = ask(ADMITS_KEY, "'bob'", &[KEY_GRANT], WRITE, Some("bob")).await;
    assert_refused(&answer, &minted, "a share key credits no account");
}
