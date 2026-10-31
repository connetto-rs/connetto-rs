//! Auth-error retry tests (Docker-gated).
//!
//! When the authorization service is unreachable, `dispatch_event` must
//! return `Err(SessionError::AuthUnavailable)` so the ingest loop can hold the
//! event and retry without advancing the replication slot cursor. Before the
//! fix, the error was silently discarded and `Ok(())` was returned, causing
//! `source.ack` to be called and the cursor to advance past the unanswered
//! event.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use connetto_core::auth::Principal;
use connetto_core::messages::{
    BulkMessage, ControlMessage, Handshake, PauseCause, SUBSCRIPTION_REFUSED, Subscribe,
    SubscriptionSpec,
};
use connetto_core::test_support::TestGrantChecker;
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{Cursor, PROTOCOL_VERSION};
use connetto_server::{
    ManagerBuilder, Materializer, NoConnector, PageSpec, PgReadConnector, PgSnapshotSource,
    ReconnectEvent, RlsAuthError, SessionError, SnapshotEstimate, SnapshotPage, SnapshotSource,
    loopback, pg_write_target,
};
use connetto_test_harness::{Client, ConnettoDefaults, Fixture, committed_at};
use subql::backend::Postgres;
use subql::visibility::openfga::OpenFgaError;
use subql::visibility::{RowView, RowWrite, Verdict, VisibilityPolicy};
use subql::{CdcSource, PgChangeEvent, PgSqliteEmuSource};

use crate::one_event::OneEvent;

const PG_DDL: &str = "CREATE TABLE items (id INT PRIMARY KEY, data TEXT);";

/// Auth policy whose `may_see` always returns an error, simulating an
/// unreachable authorization service.
struct AlwaysErrSee;

impl VisibilityPolicy for AlwaysErrSee {
    type Watcher = Arc<Principal>;
    type Error = io::Error;
    type Backend = Postgres;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_see<R>(
        &self,
        _row: &R,
        _watchers: &[Self::Watcher],
        _verdicts: &mut [Verdict],
    ) -> Result<(), Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Err(io::Error::other("auth service unreachable"))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}

/// Auth policy whose `may_see` and `may_write` answer based on an atomic flag.
/// When the flag is `false` both methods return an error, simulating an
/// unreachable auth service. Flip it to `true` to restore the service.
/// Snapshot source that returns an empty patchset.
struct EmptySnapshot;

impl SnapshotSource for EmptySnapshot {
    type Error = std::convert::Infallible;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn estimate(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::BindValue],
        _caller: &Principal,
    ) -> Result<SnapshotEstimate, Self::Error> {
        Ok(SnapshotEstimate {
            rows: 0.0,
            width: 0,
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn snapshot_page(
        &self,
        _select_sql: &str,
        _binds: &[connetto_core::messages::BindValue],
        _auth: &Principal,
        _page: &PageSpec,
    ) -> Result<SnapshotPage, Self::Error> {
        Ok(SnapshotPage {
            patchset: Vec::new(),
            cursor: Cursor::new(Vec::new()),
            next: None,
            filled: false,
            widest_row: 0,
            rows: 0,
            bytes: 0,
        })
    }
}

async fn next_control<T: Transport>(transport: &mut T) -> ControlMessage {
    match transport.recv().await.expect("recv frame") {
        Some(IncomingFrame::Control(msg)) => msg,
        other => panic!("expected control frame, got {other:?}"),
    }
}

/// The aggregate updates `counter` receives until it goes idle, in order.
async fn aggregate_updates(counter: &mut Client) -> Vec<Option<String>> {
    let mut counts = Vec::new();
    loop {
        let control =
            tokio::time::timeout(Duration::from_millis(500), counter.next_control()).await;
        match control {
            Err(_) => break,
            Ok(ControlMessage::AggregateUpdate(update)) => counts.push(update.result_json),
            Ok(ControlMessage::DeliveryPaused { .. } | ControlMessage::DeliveryResumed) => {}
            Ok(other) => panic!("unexpected control {other:?}"),
        }
    }
    counts
}

/// When `may_see` returns an error, `dispatch_event` must return
/// `Err(SessionError::AuthUnavailable)` so the ingest loop withholds the
/// source `ack`, keeping the replication slot cursor stationary until the
/// auth service recovers.
///
/// Before the fix: `let _ = auth.may_see(...)` discarded the error, verdicts
/// stayed as pre-filled denials, and `dispatch_event` returned `Ok(())`. The
/// ingest loop then called `source.ack` and the event was permanently lost.
///
/// After the fix: the error propagates as `Err(AuthUnavailable)`, the ingest
/// loop retries, and `DeliveryPaused` is sent to all live sessions.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dispatch_event_returns_auth_unavailable_and_holds_cursor() {
    let fixture = Fixture::acquire().await;
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL).build().expect("materializer"),
        EmptySnapshot,
        AlwaysErrSee,
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
            .expect("build write target"),
    )
    .build();

    // Connect a client and subscribe so a route is registered. `dispatch_event`
    // only consults the auth policy when there is at least one watcher.
    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.clone().serve(server_transport));

    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "client-a")
                .with_grant(connetto_core::messages::Grant::new("user:client-a")),
        ))
        .await
        .expect("send handshake");
    // The server leads every connection with a challenge a person ignores (R91).
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };

    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "items".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM items"),
        }))
        .await
        .expect("send subscribe");
    // Drain the empty snapshot (begin, patch, end).
    let ControlMessage::SnapshotBegin(_) = next_control(&mut client).await else {
        panic!("expected snapshot begin");
    };
    match client.recv().await.expect("recv") {
        Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {}
        other => panic!("expected snapshot patch, got {other:?}"),
    }
    let ControlMessage::SnapshotEnd(_) = next_control(&mut client).await else {
        panic!("expected snapshot end");
    };

    // Drive one CDC insert that matches the subscription. The route exists so
    // `may_see` is called with a non-empty watcher list.
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (1, 'hello')")
        .expect("execute dml");
    let event = source
        .next_item()
        .await
        .expect("poll source")
        .and_then(subql::SourceItem::into_event)
        .expect("one event");

    // The key assertion. Before the fix: Ok(()) (error discarded, cursor
    // would advance). After the fix: Err(AuthUnavailable) (cursor held).
    let result = manager.dispatch_event(&event).await;
    assert!(
        matches!(result, Err(SessionError::AuthUnavailable(_))),
        "auth error must propagate so the ingest loop holds the source cursor; got {result:?}",
    );
    // No live patch was delivered: the auth question was not answered.
    let idle = tokio::time::timeout(Duration::from_millis(200), client.recv()).await;
    assert!(
        idle.is_err() || matches!(idle, Ok(Ok(None))),
        "no live patch must arrive while auth is unreachable",
    );
}

/// When `ingest` encounters auth errors it emits `AuthRetrying` events
/// through the `on_event` callback (same convention as source reconnects),
/// and broadcasts `DeliveryPaused` to every live session on the first error.
/// After the auth service recovers it broadcasts `DeliveryResumed`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_emits_auth_retry_events_and_broadcasts_pause_resume() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let fixture = Fixture::acquire().await;
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL).build().expect("materializer"),
        EmptySnapshot,
        AlwaysErrSee,
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
            .expect("build write target"),
    )
    .build();

    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.clone().serve(server_transport));

    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "client-b")
                .with_grant(connetto_core::messages::Grant::new("user:client-b")),
        ))
        .await
        .expect("send handshake");
    // The server leads every connection with a challenge a person ignores (R91).
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };

    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "items".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM items"),
        }))
        .await
        .expect("send subscribe");
    let ControlMessage::SnapshotBegin(_) = next_control(&mut client).await else {
        panic!("expected snapshot begin");
    };
    match client.recv().await.expect("recv") {
        Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {}
        other => panic!("expected snapshot patch, got {other:?}"),
    }
    let ControlMessage::SnapshotEnd(_) = next_control(&mut client).await else {
        panic!("expected snapshot end");
    };

    // Set up a source with one matching event.
    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (2, 'world')")
        .expect("execute dml");

    // Count AuthRetrying events from the callback.
    let retry_count = Arc::new(AtomicU32::new(0));
    let retry_count_clone = Arc::clone(&retry_count);

    // Ingest the source. The first event triggers auth errors, so ingest
    // loops in AuthRetrying until we stop it. Stop after the first retry by
    // using an abort handle.
    let ingest_handle = tokio::spawn({
        let manager = Arc::clone(&manager);
        async move {
            let _ = manager
                .ingest(&mut source, &mut |event| {
                    if matches!(event, ReconnectEvent::AuthRetrying { .. }) {
                        retry_count_clone.fetch_add(1, Ordering::Relaxed);
                    }
                })
                .await;
        }
    });

    // Wait long enough for at least one retry cycle to fire.
    tokio::time::sleep(Duration::from_millis(600)).await;

    assert!(
        retry_count.load(Ordering::Relaxed) >= 1,
        "ingest must emit at least one AuthRetrying event when auth is unreachable"
    );

    // The client should have received DeliveryPaused exactly once.
    let frame = tokio::time::timeout(Duration::from_millis(200), next_control(&mut client)).await;
    assert!(
        matches!(
            frame,
            Ok(ControlMessage::DeliveryPaused {
                cause: PauseCause::AuthServiceUnreachable,
            })
        ),
        "client must receive DeliveryPaused; got {frame:?}",
    );

    ingest_handle.abort();
}

/// Auth policy whose `may_see` refuses for a reason fixed by the model, the
/// one a changed row's table with no object in the model meets.
struct UnnameableRows;

impl VisibilityPolicy for UnnameableRows {
    type Watcher = Arc<Principal>;
    type Error = OpenFgaError;
    type Backend = Postgres;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_see<R>(
        &self,
        _row: &R,
        _watchers: &[Self::Watcher],
        _verdicts: &mut [Verdict],
    ) -> Result<(), Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Err(OpenFgaError::RowCannotBeNamed)
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}

/// Row-level-security policy whose pool cannot hand out a connection, the
/// outage half of the `RlsAuthError` classification.
struct RlsPoolError;

impl VisibilityPolicy for RlsPoolError {
    type Watcher = Arc<Principal>;
    type Error = RlsAuthError;
    type Backend = Postgres;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_see<R>(
        &self,
        _row: &R,
        _watchers: &[Self::Watcher],
        _verdicts: &mut [Verdict],
    ) -> Result<(), Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Err(RlsAuthError::Pool(
            "the pool cannot hand out a connection".to_owned(),
        ))
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}

/// Row-level-security policy that cannot speak for the table, the refusal
/// half of the `RlsAuthError` classification.
struct RlsUndecidable;

impl VisibilityPolicy for RlsUndecidable {
    type Watcher = Arc<Principal>;
    type Error = RlsAuthError;
    type Backend = Postgres;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_see<R>(
        &self,
        _row: &R,
        _watchers: &[Self::Watcher],
        _verdicts: &mut [Verdict],
    ) -> Result<(), Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Err(RlsAuthError::Undecidable {
            table: "items".to_owned(),
            detail: "the table's rules are not installed".to_owned(),
        })
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}

/// A refusal the model fixes gives the same answer however often it is
/// asked, so the ingest loop ends with it instead of holding the event and
/// retrying without end. Retrying would pause every client's delivery for
/// good while looking like an outage.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refusal_the_model_fixes_ends_ingest_instead_of_retrying() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let fixture = Fixture::acquire().await;
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL).build().expect("materializer"),
        EmptySnapshot,
        UnnameableRows,
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
            .expect("build write target"),
    )
    .build();

    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.clone().serve(server_transport));
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "client-c")
                .with_grant(connetto_core::messages::Grant::new("user:client-c")),
        ))
        .await
        .expect("send handshake");
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "items".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM items"),
        }))
        .await
        .expect("send subscribe");
    let ControlMessage::SnapshotBegin(_) = next_control(&mut client).await else {
        panic!("expected snapshot begin");
    };
    match client.recv().await.expect("recv") {
        Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {}
        other => panic!("expected snapshot patch, got {other:?}"),
    }
    let ControlMessage::SnapshotEnd(_) = next_control(&mut client).await else {
        panic!("expected snapshot end");
    };

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (3, 'unnamed')")
        .expect("execute dml");
    let retries = AtomicU32::new(0);
    let ended = tokio::time::timeout(
        Duration::from_secs(10),
        manager.ingest(&mut source, &mut |event| {
            if matches!(event, ReconnectEvent::AuthRetrying { .. }) {
                retries.fetch_add(1, Ordering::Relaxed);
            }
        }),
    )
    .await
    .expect("ingest ends on the refusal instead of retrying without end");
    assert!(
        matches!(&ended, Err(SessionError::AuthorizationUnanswerable { table, .. }) if table == "items"),
        "the refusal ends ingest naming the table, got {ended:?}"
    );
    assert_eq!(
        retries.load(Ordering::Relaxed),
        0,
        "a fixed refusal is never retried"
    );
}

/// A row-level-security pool that cannot hand out a connection is an outage,
/// so `dispatch_event` reports `AuthUnavailable` and the ingest loop holds
/// the event rather than acking it past the unanswered question.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_level_security_outage_holds_the_event() {
    let fixture = Fixture::acquire().await;
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL).build().expect("materializer"),
        EmptySnapshot,
        RlsPoolError,
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
            .expect("build write target"),
    )
    .build();

    // Connect a client and subscribe so a route is registered; `dispatch_event`
    // only consults the auth policy when at least one watcher waits.
    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.clone().serve(server_transport));
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "client-d")
                .with_grant(connetto_core::messages::Grant::new("user:client-d")),
        ))
        .await
        .expect("send handshake");
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "items".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM items"),
        }))
        .await
        .expect("send subscribe");
    let ControlMessage::SnapshotBegin(_) = next_control(&mut client).await else {
        panic!("expected snapshot begin");
    };
    match client.recv().await.expect("recv") {
        Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {}
        other => panic!("expected snapshot patch, got {other:?}"),
    }
    let ControlMessage::SnapshotEnd(_) = next_control(&mut client).await else {
        panic!("expected snapshot end");
    };

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (7, 'held')")
        .expect("execute dml");
    let event = source
        .next_item()
        .await
        .expect("poll source")
        .and_then(subql::SourceItem::into_event)
        .expect("one event");

    let result = manager.dispatch_event(&event).await;
    assert!(
        matches!(result, Err(SessionError::AuthUnavailable(_))),
        "a pool outage must propagate as auth unavailable so the cursor is held: {result:?}"
    );
    let idle = tokio::time::timeout(Duration::from_millis(200), client.recv()).await;
    assert!(
        idle.is_err() || matches!(idle, Ok(Ok(None))),
        "no live patch must arrive while the rules cannot be asked"
    );
}

/// A table whose row-level rules cannot be spoken for is a fixed refusal, so
/// the ingest loop ends naming the table instead of retrying without end.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_level_security_refusal_ends_ingest_instead_of_retrying() {
    use std::sync::atomic::{AtomicU32, Ordering};

    let fixture = Fixture::acquire().await;
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL).build().expect("materializer"),
        EmptySnapshot,
        RlsUndecidable,
        Arc::new(TestGrantChecker),
        NoConnector,
        pg_write_target::<ConnettoDefaults>(fixture.admin().clone(), PG_DDL)
            .expect("build write target"),
    )
    .build();

    let (server_transport, mut client) = loopback();
    let _server = tokio::spawn(manager.clone().serve(server_transport));
    client
        .send_control(ControlMessage::Handshake(
            Handshake::new(PROTOCOL_VERSION, "client-e")
                .with_grant(connetto_core::messages::Grant::new("user:client-e")),
        ))
        .await
        .expect("send handshake");
    let ControlMessage::Challenge(_) = next_control(&mut client).await else {
        panic!("expected the leading challenge");
    };
    let ControlMessage::HandshakeAck(_) = next_control(&mut client).await else {
        panic!("expected handshake ack");
    };
    client
        .send_control(ControlMessage::Subscribe(Subscribe {
            sub_id: "items".to_owned(),
            spec: SubscriptionSpec::new("SELECT * FROM items"),
        }))
        .await
        .expect("send subscribe");
    let ControlMessage::SnapshotBegin(_) = next_control(&mut client).await else {
        panic!("expected snapshot begin");
    };
    match client.recv().await.expect("recv") {
        Some(IncomingFrame::Bulk(BulkMessage::SnapshotPatch(_))) => {}
        other => panic!("expected snapshot patch, got {other:?}"),
    }
    let ControlMessage::SnapshotEnd(_) = next_control(&mut client).await else {
        panic!("expected snapshot end");
    };

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (8, 'undecided')")
        .expect("execute dml");
    let retries = AtomicU32::new(0);
    let ended = tokio::time::timeout(
        Duration::from_secs(10),
        manager.ingest(&mut source, &mut |event| {
            if matches!(event, ReconnectEvent::AuthRetrying { .. }) {
                retries.fetch_add(1, Ordering::Relaxed);
            }
        }),
    )
    .await
    .expect("ingest ends on the refusal instead of retrying without end");
    assert!(
        matches!(&ended, Err(SessionError::AuthorizationUnanswerable { table, .. }) if table == "items"),
        "the refusal ends ingest naming the table, got {ended:?}"
    );
    assert_eq!(
        retries.load(Ordering::Relaxed),
        0,
        "a refusal the deployment fixes is never retried"
    );
}

/// Auth policy whose first `may_see` fails as an outage and whose later calls
/// allow every watcher.
struct FailsFirstSee {
    failed: std::sync::atomic::AtomicBool,
}

impl VisibilityPolicy for FailsFirstSee {
    type Watcher = Arc<Principal>;
    type Error = io::Error;
    type Backend = Postgres;

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_see<R>(
        &self,
        _row: &R,
        _watchers: &[Self::Watcher],
        verdicts: &mut [Verdict],
    ) -> Result<(), Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        if !self.failed.swap(true, std::sync::atomic::Ordering::Relaxed) {
            return Err(io::Error::other("auth service unreachable"));
        }
        verdicts.fill(Verdict::Allow);
        Ok(())
    }

    #[expect(
        clippy::unused_async_trait_impl,
        reason = "the trait method is async and this body finishes without awaiting"
    )]
    async fn may_write<R>(
        &self,
        _write: RowWrite<'_, R>,
        _watcher: &Self::Watcher,
    ) -> Result<Verdict, Self::Error>
    where
        R: RowView<Backend = Postgres> + Sync + ?Sized,
    {
        Ok(Verdict::Allow)
    }
}

/// An authorization outage while rows fan out holds the event, and the retry
/// answers the held question without folding the event a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_auth_outage_retry_folds_the_held_event_once() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS items CASCADE").await;
    fixture.exec(PG_DDL).await;
    let pool = fixture.admin().clone();
    let manager = ManagerBuilder::new(
        Materializer::builder(PG_DDL)
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), PG_DDL).expect("snapshot source"),
        FailsFirstSee {
            failed: std::sync::atomic::AtomicBool::new(false),
        },
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, PG_DDL).expect("build write target"),
    )
    .build();
    let connect = || {
        let (server, client) = loopback();
        tokio::spawn(manager.clone().serve(server));
        Client::new(client)
    };
    let mut rows = connect();
    rows.handshake_with("rows", "user:rows").await;
    rows.subscribe("items", "SELECT * FROM items").await;
    rows.expect_snapshot("items").await;
    let mut counter = connect();
    counter.handshake_with("counter", "user:counter").await;
    counter
        .subscribe("count", "SELECT COUNT(*) FROM items")
        .await;
    let ControlMessage::AggregateUpdate(seed) = counter.next_control().await else {
        panic!("expected the count seed");
    };
    assert_eq!(seed.result_json.as_deref(), Some("0"));

    let mut source = PgSqliteEmuSource::open_in_memory(PG_DDL).expect("open emu source");
    source
        .execute_sql("INSERT INTO items (id, data) VALUES (1, 'held')")
        .expect("execute dml");
    let insert = source
        .next_item()
        .await
        .expect("poll source")
        .and_then(subql::SourceItem::into_event)
        .expect("one event");
    fixture
        .exec("INSERT INTO items (id, data) VALUES (1, 'held')")
        .await;
    let insert = committed_at(insert, fixture.commit_now().await);
    tokio::time::timeout(
        Duration::from_secs(10),
        manager.ingest(&mut OneEvent(Some(insert)), &mut |_| {}),
    )
    .await
    .expect("ingest drains the source")
    .expect("ingest succeeds after the outage");

    let counts = aggregate_updates(&mut counter).await;
    assert_eq!(counts, [Some("1".to_owned())]);
    let live = rows.wait_for_live(Duration::from_secs(5)).await;
    assert_eq!(live.sub_id, "items");
}

/// A `timestamptz` column, the value Postgres holds that the SQLite wire
/// cannot carry, so the fold keeps no patch for its event.
const STAMPS_PG: &str = "CREATE TABLE stamps (id INT PRIMARY KEY, at TIMESTAMPTZ NOT NULL);";

/// An authorization outage that lands on the refusal's own verdict holds the
/// unencodable event like a row patch, the retry answers the held question,
/// the matched caller is refused exactly once, the fold stays single, and a
/// later change still arrives.
#[expect(
    clippy::too_many_lines,
    reason = "The outage, the held retry and the refusal answer form one scenario."
)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_auth_outage_during_the_refusal_verdict_refuses_once_and_folds_once() {
    let fixture = Fixture::acquire().await;
    fixture.exec("DROP TABLE IF EXISTS stamps CASCADE").await;
    fixture.exec(STAMPS_PG).await;
    let pool = fixture.admin().clone();
    let manager = ManagerBuilder::new(
        Materializer::builder(STAMPS_PG)
            .with_read_connector(PgReadConnector::with_session_setup(pool.clone()))
            .build()
            .expect("materializer"),
        PgSnapshotSource::from_ddl(pool.clone(), STAMPS_PG).expect("snapshot source"),
        FailsFirstSee {
            failed: std::sync::atomic::AtomicBool::new(false),
        },
        Arc::new(TestGrantChecker),
        PgReadConnector::with_session_setup(pool.clone()),
        pg_write_target::<ConnettoDefaults>(pool, STAMPS_PG).expect("build write target"),
    )
    .build();
    let connect = || {
        let (server, client) = loopback();
        tokio::spawn(manager.clone().serve(server));
        Client::new(client)
    };
    let mut rows = connect();
    rows.handshake_with("rows", "user:rows").await;
    rows.subscribe("stamps", "SELECT * FROM stamps").await;
    rows.expect_snapshot("stamps").await;
    let mut counter = connect();
    counter.handshake_with("counter", "user:counter").await;
    counter
        .subscribe("count", "SELECT COUNT(*) FROM stamps")
        .await;
    let ControlMessage::AggregateUpdate(seed) = counter.next_control().await else {
        panic!("expected the count seed");
    };
    assert_eq!(seed.result_json.as_deref(), Some("0"));

    // The event the replica cannot store, stamped the way a committed row arrives.
    let mut source = PgSqliteEmuSource::open_in_memory(STAMPS_PG).expect("open emu source");
    source
        .execute_sql("INSERT INTO stamps (id, at) VALUES (1, 'infinity')")
        .expect("execute dml");
    let insert = source
        .next_item()
        .await
        .expect("poll source")
        .and_then(subql::SourceItem::into_event)
        .expect("one event");
    fixture
        .exec("INSERT INTO stamps (id, at) VALUES (1, 'infinity')")
        .await;
    let insert = committed_at(insert, fixture.commit_now().await);

    let retries = Arc::new(std::sync::atomic::AtomicU32::new(0));
    tokio::time::timeout(
        Duration::from_secs(10),
        manager.ingest(&mut OneEvent(Some(insert)), &mut |event| {
            if matches!(event, ReconnectEvent::AuthRetrying { .. }) {
                retries.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }),
    )
    .await
    .expect("ingest drains the source")
    .expect("ingest succeeds after the outage");
    assert_eq!(
        retries.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the held event asked its verdict once more after the outage"
    );

    // The outage that hit the verdict paused delivery before the answer, and
    // the matched caller is refused once behind the recovery, nothing more.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut paused = false;
    let mut resumed = false;
    let mut refusals = 0;
    while !(refusals > 0 && resumed) {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        let frame = tokio::time::timeout(remaining, rows.recv())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "the rows client never finished the outage answer, \
                     paused={paused} resumed={resumed} refusals={refusals}"
                )
            })
            .expect("the connection stayed open");
        match frame {
            IncomingFrame::Control(ControlMessage::DeliveryPaused { cause }) => {
                assert_eq!(
                    cause,
                    PauseCause::AuthServiceUnreachable,
                    "the outage that holds the event is the unreachable authorization service"
                );
                paused = true;
            }
            IncomingFrame::Control(ControlMessage::DeliveryResumed) => {
                resumed = true;
            }
            IncomingFrame::Control(ControlMessage::NonFatalError(fatal)) => {
                refusals += 1;
                assert_eq!(
                    fatal.related_to.as_deref(),
                    Some("stamps"),
                    "the refusal names the subscription the event matched"
                );
                assert_eq!(fatal.detail, SUBSCRIPTION_REFUSED);
            }
            other => panic!("the refused caller met an unexpected frame {other:?}"),
        }
    }
    assert!(
        paused,
        "the outage must pause delivery before the refusal answers"
    );
    assert_eq!(refusals, 1, "the matched caller is refused exactly once");

    // The fold happened once, so the count stands at 1, and a later change
    // still moves it. The first commit still sits in the source's queue, so
    // the drain takes every item, not just the first.
    source
        .execute_sql("INSERT INTO stamps (id, at) VALUES (2, '2026-01-02 00:00:00+00')")
        .expect("execute dml");
    let mut later_events = Vec::new();
    while let Some(item) = source.next_item().await.expect("poll source") {
        if let subql::SourceItem::Event(event) = item {
            later_events.push(event);
        }
    }
    let [later] = <[PgChangeEvent; 1]>::try_from(later_events).expect("one row per insert");
    fixture
        .exec("INSERT INTO stamps (id, at) VALUES (2, '2026-01-02 00:00:00+00')")
        .await;
    let later = committed_at(later, fixture.commit_now().await);
    manager
        .dispatch_event(&later)
        .await
        .expect("dispatch the later change");
    let counts = aggregate_updates(&mut counter).await;
    assert_eq!(
        counts,
        [Some("1".to_owned()), Some("2".to_owned())],
        "the held event folded once and the later change still arrived"
    );
    let later_live = rows.try_live(Duration::from_millis(500)).await;
    assert!(
        later_live.is_none(),
        "the refused subscription receives nothing more, and the rows client met {later_live:?}"
    );
}
