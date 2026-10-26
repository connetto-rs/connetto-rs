use std::cell::{Cell, RefCell};
use std::rc::Rc;

use connetto_client::ConnettoConnection;
use connetto_client::reconnect::{Sleeper, TransportFactory};
use connetto_core::messages::SubscriptionSpec;
use connetto_file_client::{BrowserStore, ContentArchive};
use tokio::sync::mpsc::UnboundedReceiver;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use wasm_bindgen_futures::spawn_local;
use web_sys::{BroadcastChannel, MessageEvent};

use super::super::helpers::content_store_namespace;
use super::BootError;
use super::replica::BootReplicaSpec;
use crate::builder::WebConfig;
use crate::relay::HubReconnect;
use crate::{BrowserSocket, HubNotice, RelayHub, locks};

thread_local! {
    static DB_ALIVE: RefCell<Option<locks::HeldLock>> = const { RefCell::new(None) };
}

struct ConnectGate {
    open: Rc<Cell<bool>>,
    _channel: BroadcastChannel,
    _listener: Closure<dyn FnMut(MessageEvent)>,
}

fn install_connect_gate(name: &'static str) -> Result<Rc<ConnectGate>, BootError> {
    let channel =
        BroadcastChannel::new(name).map_err(|err| super::super::IntakeError::ChannelOpen {
            operation: "connect gate",
            detail: format!("{err:?}"),
        })?;
    let open = Rc::new(Cell::new(false));
    let listener = {
        let open = Rc::clone(&open);
        Closure::<dyn FnMut(MessageEvent)>::new(move |_event: MessageEvent| {
            open.set(true);
        })
    };
    channel.set_onmessage(Some(listener.as_ref().unchecked_ref()));
    Ok(Rc::new(ConnectGate {
        open,
        _channel: channel,
        _listener: listener,
    }))
}
/// Install storage and custody, carry out any outstanding data wipe, and
/// reserve this boot's database slots.
///
/// Returns the storage handle, the key store, and whether a credential was
/// already enrolled.
pub(super) async fn prepare_boot_storage(
    config: &WebConfig,
) -> Result<
    (
        crate::storage::ReplicaStorage,
        std::rc::Rc<crate::auth::IdbKeyStore>,
        bool,
    ),
    BootError,
> {
    let storage = crate::storage::ReplicaStorage::install().await;
    // Encrypted regardless of auth, and the per-replica key also lives here.
    let key_store = std::rc::Rc::new(
        crate::auth::IdbKeyStore::open()
            .await
            .map_err(BootError::KeyStore)?,
    );
    // Wipes run before custody so a pending deletion applies even when the unlock is refused or unconfigured.
    apply_pending_wipes(&storage, &key_store, config.replica_db_prefix).await?;
    let was_enrolled = super::replica::setup_custody(config, &key_store).await?;
    // After wipes (which free slots) and before login (which reads the account index).
    storage
        .reserve(super::BOOT_SLOTS)
        .await
        .map_err(BootError::SlotReservation)?;
    Ok((storage, key_store, was_enrolled))
}

pub(super) async fn apply_pending_wipes(
    storage: &crate::storage::ReplicaStorage,
    key_store: &crate::auth::IdbKeyStore,
    prefix: Option<&str>,
) -> Result<(), BootError> {
    for pending in crate::storage::pending_wipes()
        .await
        .map_err(BootError::KeyStore)?
    {
        apply_pending_wipe(storage, key_store, prefix, &pending).await?;
    }
    Ok(())
}

async fn apply_pending_wipe(
    storage: &crate::storage::ReplicaStorage,
    key_store: &crate::auth::IdbKeyStore,
    prefix: Option<&str>,
    pending: &crate::storage::PendingWipe,
) -> Result<(), BootError> {
    let progress = if pending.replica_deleted() {
        // The replica and key went on an earlier boot, and a fresh login may
        // already own the name, so only the orphaned content is retried here.
        match pending.content_namespace.as_deref() {
            Some(namespace) => crate::storage::remove_content_namespace(namespace).await?,
            None => crate::storage::WipeProgress::Complete,
        }
    } else {
        // The last identity out of the shared store takes it along, before
        // anything opens it (R11 decision 7).
        if pending.shared() == crate::storage::SharedWipe::LastOut
            && let Some(prefix) = prefix
        {
            crate::storage::remove_shared_store(storage, key_store, prefix).await?;
        }
        crate::storage::wipe_replica(
            storage,
            key_store,
            &pending.replica,
            pending.content_namespace.as_deref(),
            &crate::auth::PendingWork::default(),
            true,
        )
        .await?
    };
    // A departure still owed keeps the record until this boot's worker,
    // once its own connection is open, takes the identity out of the store.
    let departing = pending.shared() == crate::storage::SharedWipe::Depart;
    match (progress, pending.replica_deleted()) {
        (crate::storage::WipeProgress::Complete, _) if !departing => {
            crate::storage::acknowledge_pending_wipe(pending)
                .await
                .map_err(BootError::KeyStore)?;
        }
        (
            crate::storage::WipeProgress::Complete | crate::storage::WipeProgress::ContentPending,
            false,
        ) => {
            crate::storage::defer_pending_content_wipe(pending)
                .await
                .map_err(BootError::KeyStore)?;
        }
        (
            crate::storage::WipeProgress::Complete | crate::storage::WipeProgress::ContentPending,
            true,
        ) => {}
    }
    tracing::info!(replica = %pending.replica, "db worker: advanced a pending data wipe");
    Ok(())
}

/// Take out of the shared store every identity whose departure failed before
/// its replica was wiped, now that this boot's own connection holds the
/// store attached. A departure that fails again stays owed to a later boot.
pub(super) async fn retry_shared_departures(worker: &mut ConnettoConnection<BrowserSocket>) {
    let pending = match crate::storage::pending_wipes().await {
        Ok(pending) => pending,
        Err(err) => {
            tracing::warn!(error = %err, "db worker: listing owed shared departures failed");
            return;
        }
    };
    for wipe in pending.iter().filter(|wipe| {
        wipe.replica_deleted() && wipe.shared() == crate::storage::SharedWipe::Depart
    }) {
        if let Err(err) = worker.retire_shared_identity(&wipe.replica) {
            tracing::warn!(error = %err, replica = %wipe.replica, "db worker: an owed shared departure failed again");
            continue;
        }
        if let Err(err) = crate::storage::settle_shared_departure(wipe).await {
            tracing::warn!(error = %err, replica = %wipe.replica, "db worker: recording a shared departure failed");
        }
    }
}

pub(super) async fn hold_alive_lock() {
    let alive = locks::hold_lock(super::super::DB_ALIVE_LOCK).await;
    DB_ALIVE.with(|cell| cell.borrow_mut().replace(alive));
}

pub(super) async fn start_boot_services<Id>(
    config: &WebConfig,
    spec: &BootReplicaSpec<Id>,
    worker: ConnettoConnection<BrowserSocket>,
    content_root_key: Option<[u8; 32]>,
) -> Result<(RelayHub, Option<bool>), BootError> {
    let ws_url = config.ws_url;
    let connect_gate = match config.connect_gate {
        Some(name) => Some(install_connect_gate(name)?),
        None => None,
    };
    let reconnect = HubReconnect {
        factory: move || {
            let connect_gate = connect_gate.clone();
            async move {
                if connect_gate.as_ref().is_some_and(|gate| !gate.open.get()) {
                    return Err("connect gate closed".to_owned());
                }
                BrowserSocket::connect(ws_url)
                    .await
                    .map_err(|err| err.to_string())
            }
        },
        sleeper: super::super::intake::sleep,
        policy: config.policy.clone(),
        upstream: config
            .upstream_subscriptions()
            .map(|(sub_id, query)| (sub_id.to_owned(), SubscriptionSpec::new(query)))
            .collect(),
    };
    let (content, content_persistent, content_wipe_namespace) = setup_content_store(
        config,
        spec.identified,
        &spec.replica_db_name,
        content_root_key,
    )
    .await?;
    let hub_meta = crate::storage::hub_meta_db_name(&spec.replica_db_name);
    let (hub, notices) =
        start_relay_hub(worker, &hub_meta, reconnect, content, config.content_http)?;
    install_dead_tab_reaper(hub.clone(), notices);
    install_tab_services(
        config,
        &hub,
        &spec.replica_db_name,
        content_wipe_namespace,
        spec.active_account.as_deref(),
    )?;
    super::super::intake::install_hello_intake(hub.clone())?;
    Ok((hub, content_persistent))
}

async fn setup_content_store(
    config: &WebConfig,
    identified: bool,
    replica_db_name: &str,
    content_root_key: Option<[u8; 32]>,
) -> Result<
    (
        Option<ContentArchive<BrowserStore>>,
        Option<bool>,
        Option<String>,
    ),
    BootError,
> {
    let Some(seed) = config.content_namespace else {
        return Ok((None, None, None));
    };
    let namespace = content_store_namespace(seed, replica_db_name);
    let root_key = content_root_key.ok_or(BootError::ContentKey)?;
    let store = if identified {
        let scope: web_sys::DedicatedWorkerGlobalScope = js_sys::global()
            .dyn_into()
            .map_err(|value: js_sys::Object| BootError::NotWorkerScope(format!("{value:?}")))?;
        BrowserStore::install(&scope, &namespace).await?
    } else {
        BrowserStore::ephemeral()
    };
    let persistent = store.is_persistent();
    let wipe_namespace = identified.then_some(namespace);
    let archive = config.content_heal_lost.iter().fold(
        ContentArchive::new(store, root_key),
        |archive, (query, column)| archive.with_heal_lost(query, column),
    );
    Ok((Some(archive), Some(persistent), wipe_namespace))
}

fn start_relay_hub<F, S>(
    worker: ConnettoConnection<BrowserSocket>,
    hub_meta: &str,
    reconnect: HubReconnect<F, S>,
    content: Option<ContentArchive<BrowserStore>>,
    content_http: connetto_file_client::BrowserHttp,
) -> Result<(RelayHub, UnboundedReceiver<HubNotice>), BootError>
where
    F: TransportFactory<Transport = BrowserSocket> + 'static,
    F::Error: core::fmt::Display,
    S: Sleeper + Clone + 'static,
{
    let (hub, pump, notices) =
        RelayHub::with_reconnect_archive(worker, hub_meta, reconnect, content, content_http)?;
    spawn_local(async move {
        if let Err(err) = pump.await {
            tracing::error!(error = %err, "relay hub ended");
        }
    });
    Ok((hub, notices))
}

fn install_dead_tab_reaper(hub: RelayHub, mut notices: UnboundedReceiver<HubNotice>) {
    spawn_local(async move {
        while let Some(HubNotice::Handshake { tab, client_id }) = notices.recv().await {
            let hub = hub.clone();
            spawn_local(async move {
                let name = locks::tab_lock_name(&client_id);
                if !locks::lock_is_held(&name).await {
                    return;
                }
                locks::wait_until_free(&name).await;
                hub.kill(tab);
            });
        }
    });
}

fn install_tab_services(
    config: &WebConfig,
    hub: &RelayHub,
    replica_db_name: &str,
    content_wipe_namespace: Option<String>,
    active_account: Option<&str>,
) -> Result<(), BootError> {
    if let Some(connetto_client::builder::sign_in::SignInKind::Provider {
        origin,
        login_origin,
        provider,
        ..
    }) = &config.sign_in
    {
        super::super::logout::serve_logout_requests(
            super::super::logout::LogoutConfig {
                auth_base_url: origin.clone(),
                login_base_url: login_origin.clone(),
                provider: provider.clone(),
                redirect_uri: config.redirect_uri.clone().unwrap_or_default(),
                auth_db_name: config.auth_db_name.to_owned(),
                replica_db_name: replica_db_name.to_owned(),
                content_namespace: content_wipe_namespace,
                account: active_account.map(ToOwned::to_owned),
            },
            hub.clone(),
        )?;
    }
    super::super::archive_channel::serve_export_requests(hub.clone())?;
    super::super::archive_channel::serve_import_requests(hub.clone())?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use connetto_client::{REPLICA_PREFIX, shared_store_name};
    use connetto_core::traits::ReplicaKeyStore as _;
    use diesel::Connection as _;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    use super::apply_pending_wipes;
    use crate::auth::{IdbKeyStore, PendingWork, provision_replica_key};
    use crate::storage::{
        PendingWipe, ReplicaStorage, SharedWipe, mark_wipe_pending, pending_wipes, replica_entry,
        settle_shared_departure, take_pending_wipes,
    };

    wasm_bindgen_test_configure!(run_in_dedicated_worker);

    /// A database under `name` with its key minted, as an open leaves it.
    async fn seed(storage: &ReplicaStorage, keys: &IdbKeyStore, name: &str) {
        provision_replica_key(keys, name)
            .await
            .expect("mint the key");
        drop(
            diesel::SqliteConnection::establish(&storage.db_url(name))
                .expect("create the database"),
        );
        assert!(storage.exists(name), "the database is here to begin with");
    }

    #[wasm_bindgen_test]
    async fn the_last_identity_out_takes_the_shared_store_and_its_key() {
        let storage = ReplicaStorage::install().await;
        let keys = IdbKeyStore::open().await.expect("open the key store");
        take_pending_wipes().await.expect("drain");
        let prefix = "r11-last-out";
        let shared = replica_entry(prefix, &shared_store_name(REPLICA_PREFIX));
        let replica = replica_entry(prefix, "alice");
        seed(&storage, &keys, &shared).await;
        seed(&storage, &keys, &replica).await;
        let wipe = PendingWipe::new(replica.clone(), None).with_shared(SharedWipe::LastOut);
        mark_wipe_pending(&wipe, &PendingWork::default(), false)
            .await
            .expect("mark");
        apply_pending_wipes(&storage, &keys, Some(prefix))
            .await
            .expect("the boot applies the wipe");
        assert!(!storage.exists(&shared), "the shared store is gone");
        assert_eq!(keys.load(&shared).await.expect("load"), None, "and its key");
        assert!(!storage.exists(&replica), "with the replica");
    }

    #[wasm_bindgen_test]
    async fn an_owed_departure_keeps_its_wipe_until_it_is_settled() {
        let storage = ReplicaStorage::install().await;
        let keys = IdbKeyStore::open().await.expect("open the key store");
        take_pending_wipes().await.expect("drain");
        let prefix = "r11-depart";
        let shared = replica_entry(prefix, &shared_store_name(REPLICA_PREFIX));
        let replica = replica_entry(prefix, "bob");
        seed(&storage, &keys, &shared).await;
        seed(&storage, &keys, &replica).await;
        let wipe = PendingWipe::new(replica.clone(), None).with_shared(SharedWipe::Depart);
        mark_wipe_pending(&wipe, &PendingWork::default(), false)
            .await
            .expect("mark");
        apply_pending_wipes(&storage, &keys, Some(prefix))
            .await
            .expect("the boot applies the wipe");
        assert!(!storage.exists(&replica), "the replica is wiped");
        assert!(storage.exists(&shared), "the store stays for the departure");
        let owed = pending_wipes().await.expect("list");
        assert_eq!(owed.len(), 1, "the wipe stays until the departure lands");
        settle_shared_departure(&owed[0]).await.expect("settle");
        apply_pending_wipes(&storage, &keys, Some(prefix))
            .await
            .expect("the next boot");
        assert!(
            pending_wipes().await.expect("list").is_empty(),
            "a settled wipe is acknowledged"
        );
    }
}
