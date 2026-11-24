//! Stored account deletion leaves the live replica and its work available.

#![cfg(all(target_family = "wasm", target_os = "unknown"))]

use std::rc::Rc;

use connetto_client::{
    ClientBuilder, ClientError, ConnettoConnection, ContentPlace, FirstThen, Gate, Grant,
    HeldCredential, Located, ReplicaPlace, SyncSchema,
};
use connetto_core::schema::{SchemaBundle, SharedLayout};
use connetto_core::test_support::FakeTransport;
use connetto_core::traits::ReplicaKeyStore;
use connetto_web::RelayHub;
use connetto_web::auth::{
    AccountStore, BuilderKeyStore, IdbKeyStore, StoredForgetOutcome, StoredForgetRefusal,
    request_forget_stored,
};
use connetto_web::storage::ReplicaStorage;
use connetto_web::workers::{LogoutConfig, serve_logout_requests};
use diesel::prelude::*;
use wasm_bindgen::JsCast;
use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

wasm_bindgen_test_configure!(run_in_dedicated_worker);

diesel::table! {
    /// Account-private rows.
    items (id) {
        /// Row identity.
        id -> Integer,
        /// Optional label.
        label -> Nullable<Text>,
    }
}

diesel::table! {
    /// Device-shared rows.
    shared_items (id) {
        /// Row identity.
        id -> Integer,
        /// Optional label.
        label -> Nullable<Text>,
    }
}

fn bundle() -> SchemaBundle {
    const LOCAL: &str = "CREATE TABLE items (id INTEGER PRIMARY KEY, label TEXT)";
    const SHARED: &str =
        "CREATE TABLE connetto_shared.shared_items (id INTEGER PRIMARY KEY, label TEXT)";
    SchemaBundle::new(
        "-- connetto:major 1",
        "",
        connetto_core::ReplicaCopy::new(
            format!("{LOCAL}; CREATE TABLE shared_items (id INTEGER PRIMARY KEY, label TEXT)"),
            Vec::<(String, String)>::new(),
            Vec::<String>::new(),
        )
        .with_shared_tables(["shared_items"])
        .with_shared_layout(SharedLayout::new(
            format!("{LOCAL}; {SHARED}"),
            LOCAL,
            SHARED,
            "",
        )),
        None::<&str>,
    )
    .unwrap()
}

struct Place<'storage>(&'storage ReplicaStorage);

impl ReplicaPlace for Place<'_> {
    fn locate(&self, name: &str) -> Result<Located, ClientError> {
        Ok(Located::new(
            name,
            self.0.db_url(name),
            self.0.exists(name),
            ContentPlace::InMemory,
        ))
    }
}

async fn open(
    storage: &ReplicaStorage,
    keys: &Rc<IdbKeyStore>,
    owner: &str,
) -> ConnettoConnection<FakeTransport> {
    for name in [
        connetto_client::replica_db_name("connetto", owner).unwrap(),
        connetto_client::shared_store_name("connetto"),
    ] {
        connetto_web::auth::provision_replica_key(keys.as_ref(), &name)
            .await
            .unwrap();
    }
    let schema = SyncSchema::new(bundle()).with_shared_store(true);
    ClientBuilder::new(
        schema,
        FirstThen::new(FakeTransport::accepting_but_silent(), || {
            core::future::ready(Err::<FakeTransport, _>("offline"))
        }),
    )
    .signed_in(HeldCredential::new(Grant::new("test"), owner).unwrap())
    .durable(
        Place(storage),
        BuilderKeyStore::new(Rc::clone(keys)),
        BuilderKeyStore::new(Rc::clone(keys)),
    )
    .with_gate(Gate::off())
    .connect_driven()
    .await
    .unwrap()
}

#[wasm_bindgen_test]
async fn deleting_a_closed_account_refuses_pending_work_and_force_preserves_the_live_account() {
    let storage = ReplicaStorage::install().await;
    storage.reserve(12).await.unwrap();
    let keys = unlocked_keys().await;
    let alice = connetto_client::encode_identity(&"stored-delete-alice").unwrap();
    let bob = connetto_client::encode_identity(&"stored-delete-bob").unwrap();
    let alice_name = connetto_client::replica_db_name("connetto", &"stored-delete-alice").unwrap();
    let bob_name = connetto_client::replica_db_name("connetto", &"stored-delete-bob").unwrap();
    for name in [
        &alice_name,
        &bob_name,
        "stored-delete-auth",
        "connetto-shared",
    ] {
        storage.delete_db(name).unwrap();
    }
    let pending = strand_closed_account(&storage, &keys).await;
    let mut live = open(&storage, &keys, "stored-delete-alice").await;
    diesel::insert_into(items::table)
        .values((items::id.eq(8), items::label.eq("live")))
        .execute(live.conn())
        .unwrap();
    assert_eq!(
        shared_items::table
            .select(shared_items::id)
            .load::<i32>(live.conn())
            .unwrap(),
        [7]
    );
    live.push().await.unwrap();
    let live_pending = live.unsynced();
    let alice_key = keys.load(&alice_name).await.unwrap().unwrap();
    let bob_key = keys.load(&bob_name).await.unwrap().unwrap();
    keys.store(
        &connetto_client::share_keys::share_key_record(&bob_name),
        &bob_key,
    )
    .await
    .unwrap();
    {
        let accounts = AccountStore::open(&storage.db_url("stored-delete-auth")).unwrap();
        accounts.remember(&bob).unwrap();
        accounts.remember(&alice).unwrap();
    }
    let hub = start_hub(live, &keys, &alice_name, &alice);
    assert_eq!(
        request_forget_stored(&alice, false).await.unwrap(),
        StoredForgetOutcome::Refused(StoredForgetRefusal::LiveAccount)
    );
    assert_eq!(
        request_forget_stored("unknown", false).await.unwrap(),
        StoredForgetOutcome::Refused(StoredForgetRefusal::UnknownAccount)
    );
    let refused = request_forget_stored(&bob, false).await.unwrap();
    let StoredForgetOutcome::Refused(StoredForgetRefusal::Pending(work)) = refused else {
        panic!("queued work must refuse deletion")
    };
    assert_eq!(work.unsynced, pending);
    assert_eq!(work.content_files, 1);
    assert!(storage.list().contains(&bob_name));
    assert_eq!(
        keys.load(&bob_name).await.unwrap().unwrap().as_bytes(),
        bob_key.as_bytes()
    );
    assert!(matches!(
        request_forget_stored(&bob, true).await.unwrap(),
        StoredForgetOutcome::Done { .. }
    ));
    assert!(!storage.list().contains(&bob_name));
    assert!(keys.load(&bob_name).await.unwrap().is_none());
    assert!(
        keys.load(&connetto_client::share_keys::share_key_record(&bob_name))
            .await
            .unwrap()
            .is_none()
    );
    assert!(storage.list().contains(&alice_name));
    assert_eq!(
        keys.load(&alice_name).await.unwrap().unwrap().as_bytes(),
        alice_key.as_bytes()
    );
    assert_eq!(hub.unsynced().await.unwrap().unsynced, live_pending);
    let accounts = AccountStore::open(&storage.db_url("stored-delete-auth")).unwrap();
    assert_eq!(accounts.accounts().unwrap(), [alice]);
    assert_live_rows(&hub).await;
}

async fn unlocked_keys() -> Rc<IdbKeyStore> {
    let keys = Rc::new(IdbKeyStore::open().await.unwrap());
    let scope: web_sys::WorkerGlobalScope = js_sys::global().unchecked_into();
    let raw: js_sys::Object = js_sys::Uint8Array::from(&[0x42; 32][..]).unchecked_into();
    let usages = js_sys::Array::of1(&wasm_bindgen::JsValue::from_str("deriveBits"));
    let promise = scope
        .crypto()
        .unwrap()
        .subtle()
        .import_key_with_str("raw", &raw, "HKDF", false, &usages)
        .unwrap();
    let derived = wasm_bindgen_futures::JsFuture::from(promise)
        .await
        .unwrap()
        .unchecked_into::<web_sys::CryptoKey>();
    keys.adopt_derived(derived, &[0x17; 16]).await.unwrap();
    keys
}

async fn strand_closed_account(
    storage: &ReplicaStorage,
    keys: &Rc<IdbKeyStore>,
) -> Vec<connetto_client::UnsyncedWrite> {
    let mut closed = open(storage, keys, "stored-delete-bob").await;
    diesel::insert_into(items::table)
        .values((items::id.eq(7), items::label.eq("closed")))
        .execute(closed.conn())
        .unwrap();
    diesel::insert_into(shared_items::table)
        .values((
            shared_items::id.eq(7),
            shared_items::label.eq("closed shared"),
        ))
        .execute(closed.conn())
        .unwrap();
    let content = connetto_file_client::ContentArchive::new(
        connetto_file_client::BrowserStore::ephemeral(),
        [0x71; 32],
    );
    content.install(&mut closed).unwrap();
    let manifest = content
        .chunk_file(
            &b"queued file"[..],
            connetto_file_client::MimeClass::Generic,
        )
        .await
        .unwrap();
    content
        .commit_staged(&mut closed, &manifest, |connection, file| {
            diesel::update(items::table.filter(items::id.eq(7)))
                .set(items::label.eq(file.to_string()))
                .execute(connection)?;
            Ok(())
        })
        .unwrap();
    closed.push().await.unwrap();
    let pending = closed.unsynced();
    assert_ne!(pending, Vec::<connetto_client::UnsyncedWrite>::new());
    pending
}

async fn assert_live_rows(hub: &RelayHub) {
    let channel = web_sys::MessageChannel::new().unwrap();
    hub.attach(connetto_web::MessageTransport::<web_sys::MessagePort>::new(
        channel.port1(),
    ));
    let mut tab = ClientBuilder::new(
        SyncSchema::new(bundle()).relay_mirror(),
        FirstThen::new(
            connetto_web::MessageTransport::<web_sys::MessagePort>::new(channel.port2()),
            || {
                core::future::ready(
                    Err::<connetto_web::MessageTransport<web_sys::MessagePort>, _>("closed"),
                )
            },
        ),
    )
    .with_client_id("stored-delete-tab")
    .connect_driven()
    .await
    .unwrap();
    tab.subscribe_spec(
        "shared",
        connetto_core::messages::SubscriptionSpec::new("SELECT * FROM shared_items"),
    )
    .await
    .unwrap();
    loop {
        if matches!(
            tab.pump_one().await.unwrap(),
            connetto_client::ClientEvent::SnapshotEnd { .. }
        ) {
            break;
        }
    }
    assert_eq!(
        shared_items::table
            .select(shared_items::id)
            .load::<i32>(tab.conn())
            .unwrap(),
        Vec::<i32>::new()
    );
    tab.subscribe_spec(
        "local",
        connetto_core::messages::SubscriptionSpec::new("SELECT * FROM items"),
    )
    .await
    .unwrap();
    loop {
        if matches!(
            tab.pump_one().await.unwrap(),
            connetto_client::ClientEvent::SnapshotEnd { .. }
        ) {
            break;
        }
    }
    assert_eq!(
        items::table
            .select(items::id)
            .load::<i32>(tab.conn())
            .unwrap(),
        [8]
    );
}

fn start_hub(
    live: ConnettoConnection<FakeTransport>,
    keys: &Rc<IdbKeyStore>,
    alice_name: &str,
    alice: &str,
) -> RelayHub {
    let (hub, pump, _notices) = RelayHub::new(live, ":memory:").unwrap();
    wasm_bindgen_futures::spawn_local(async move {
        pump.await.unwrap();
    });
    serve_logout_requests(
        LogoutConfig {
            auth_base_url: "http://127.0.0.1:1".to_owned(),
            login_base_url: None,
            provider: "unused".to_owned(),
            redirect_uri: "http://127.0.0.1:1/unused".to_owned(),
            auth_db_name: "stored-delete-auth".to_owned(),
            replica_db_name: alice_name.to_owned(),
            replica_db_prefix: "connetto".to_owned(),
            key_store: Rc::clone(keys),
            content_namespace: None,
            content_namespace_seed: None,
            account: Some(alice.to_owned()),
        },
        hub.clone(),
    )
    .unwrap();
    hub
}
