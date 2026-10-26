//! The list-and-end contract every auth store keeps (R105 decision 3),
//! checked against the in-memory store in `authn_generic_id.rs` and the
//! database store in `authn_db.rs`.

use connetto_server::{AuthStore, SessionId};

/// `ids` sorted, so two answers compare whatever order a store gives.
fn sorted(ids: impl IntoIterator<Item = SessionId>) -> Vec<SessionId> {
    let mut ids: Vec<_> = ids.into_iter().collect();
    ids.sort_by_key(|id| *id.as_uuid());
    ids
}

/// The ids of `user`'s live sessions, sorted.
async fn listed<S: AuthStore>(store: &S, user: &S::Id) -> Vec<SessionId> {
    sorted(
        store
            .sessions_of(user)
            .await
            .expect("list")
            .into_iter()
            .map(|record| record.session_id),
    )
}

/// `store` lists exactly `own`, `user`'s three live sessions, and ends one
/// of them, then all but one, then all, never touching `other`, another
/// person's session.
pub(super) async fn ends_only_the_persons_sessions<S: AuthStore>(
    store: &S,
    user: &S::Id,
    own: [SessionId; 3],
    other: SessionId,
) {
    assert_eq!(
        listed(store, user).await,
        sorted(own),
        "the person's live sessions and nobody else's"
    );
    assert!(
        !store.end_owned(user, other).await.expect("end"),
        "another person's session ends nothing"
    );
    assert!(store.end_owned(user, own[0]).await.expect("end"));
    assert!(
        !store.end_owned(user, own[0]).await.expect("end"),
        "an ended session ends once"
    );
    assert_eq!(
        store
            .end_all_of(user, Some(own[2]))
            .await
            .expect("end the others"),
        vec![own[1]]
    );
    assert_eq!(listed(store, user).await, vec![own[2]]);
    assert_eq!(
        store.end_all_of(user, None).await.expect("end all"),
        vec![own[2]]
    );
    assert_eq!(listed(store, user).await, Vec::new());
    assert!(
        store.session_is_live(other).await.expect("live"),
        "the other person's session is untouched"
    );
}
