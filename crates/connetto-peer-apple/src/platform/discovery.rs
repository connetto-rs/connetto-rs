//! Peer discovery through Network framework, the listener's Bonjour
//! advertisement beside a browser over the service type (R112 decision 5).
//!
//! The browser is configured before it starts and from then on touched only
//! on its own serial queue, where its handlers report to the session's sink.

use std::cell::RefCell;
use std::ffi::{CStr, CString, c_char};
use std::io;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::Arc;

use block2::{Block, RcBlock};
use connetto_peer::{
    DNS_SD_SERVICE, Dial, DialFuture, DnsSd, DnsSdEvent, DnsSdSession, DnsSdSink, Node,
};
use dispatch2::{DispatchQueue, DispatchRetained};
use objc2_network::{
    NWBrowseDescriptor, NWBrowseResult, NWBrowser, NWError, NWTxtRecord, nw_browser_state_t,
    nw_endpoint_type_t, nw_txt_record_find_key_t,
};
use parking_lot::Mutex;

use super::network::{
    Advertiser, Endpoint, Queued, link_endpoint, parameters, recast, state_error,
};

/// The browse results handler with its result pointers nullable, as Network
/// passes them, and its `bool` spelled `u8`.
type ResultsHandler = Block<'static, fn(*mut NWBrowseResult, *mut NWBrowseResult, u8)>;

/// A TXT applier with its value pointer nullable and its `bool` spelled `u8`.
type TxtApplier =
    Block<'static, fn(NonNull<c_char>, nw_txt_record_find_key_t, *const u8, usize) -> u8>;

unsafe extern "C-unwind" {
    /// `nw_browser_set_browse_results_changed_handler`, whose generated
    /// binding marks the results non-null where Network passes NULL for an
    /// added or removed one.
    #[link_name = "nw_browser_set_browse_results_changed_handler"]
    fn set_results_handler(browser: &NWBrowser, handler: Option<&ResultsHandler>);
}

/// The listener a device serves through, whose advertisement discovery
/// registers while the listener stands.
#[derive(Debug, Clone, Default)]
pub struct AdvertiseSlot(Arc<Mutex<Option<Advertiser>>>);

impl AdvertiseSlot {
    /// Register the next advertisement through `advertiser`.
    pub fn fill(&self, advertiser: Advertiser) {
        *self.0.lock() = Some(advertiser);
    }

    /// No listener serves, so the next advertisement is refused.
    pub fn clear(&self) {
        *self.0.lock() = None;
    }
}

/// Discovery through Network framework, advertising the listener in the
/// slot and dialling each found instance's endpoint.
#[derive(Debug, Clone)]
pub struct Network {
    slot: AdvertiseSlot,
}

impl Network {
    /// Discovery advertising through the listener `slot` holds.
    pub fn new(slot: AdvertiseSlot) -> Self {
        Self { slot }
    }
}

impl DnsSd for Network {
    fn open(&self, events: DnsSdSink) -> io::Result<Box<dyn DnsSdSession>> {
        let service = CString::new(DNS_SD_SERVICE).map_err(io::Error::other)?;
        let queue = DispatchQueue::new(Some(c"connetto.peer.browser"), None);
        let browser = {
            let descriptor = NWBrowseDescriptor::new_bonjour_service(&service, Some(c"local."));
            // The fingerprint rides in the TXT record, which a browse leaves
            // out unless asked.
            descriptor.set_include_txt_record(true);
            let browser = NWBrowser::new(&descriptor, Some(&parameters()));
            let sink = Rc::new(RefCell::new(events));
            let reporting = Rc::clone(&sink);
            let state = RcBlock::new(move |state: nw_browser_state_t, error: *mut NWError| {
                let event = if state == nw_browser_state_t::failed {
                    DnsSdEvent::Restart
                } else if state == nw_browser_state_t::waiting {
                    DnsSdEvent::Fault(state_error(error, "the browser waits").to_string())
                } else {
                    return;
                };
                (reporting.borrow_mut())(event);
            });
            let results = RcBlock::new(
                move |old: *mut NWBrowseResult, new: *mut NWBrowseResult, _batch_complete: u8| {
                    // SAFETY: Network passes a live result or NULL for the
                    // block's call.
                    let old = unsafe { old.as_ref() };
                    // SAFETY: as for the old result.
                    let new = unsafe { new.as_ref() };
                    let event = match (old, new) {
                        (_, Some(new)) => resolved(new),
                        (Some(old), None) => name_of(old).map(|name| DnsSdEvent::Removed { name }),
                        (None, None) => None,
                    };
                    if let Some(event) = event {
                        (sink.borrow_mut())(event);
                    }
                },
            );
            browser.set_state_changed_handler(Some(&state));
            // SAFETY: the handler's signature matches
            // `nw_browser_browse_results_changed_handler_t` with its pointers
            // nullable and its `bool` spelled `u8`.
            unsafe { set_results_handler(&browser, Some(&results)) };
            // SAFETY: the queue is this browser's alone and serial, so its
            // handlers' state is only ever touched there.
            unsafe { browser.set_queue(&queue) };
            browser.start();
            Queued(browser)
        };
        Ok(Box::new(NetworkSession {
            browser,
            queue,
            slot: self.slot.clone(),
            advertised: None,
        }))
    }
}

/// One browse and the advertisement it registered, both ended when the
/// session drops.
struct NetworkSession {
    browser: Queued<NWBrowser>,
    queue: DispatchRetained<DispatchQueue>,
    slot: AdvertiseSlot,
    /// The instance name advertised and the listener advertising it.
    advertised: Option<(String, Advertiser)>,
}

impl DnsSdSession for NetworkSession {
    fn register(&mut self, name: &str, _port: u16, txt: &[(&str, &str)]) -> io::Result<()> {
        // The listener advertises the port it accepts on, the node's own.
        let advertiser = self.slot.0.lock().clone().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotConnected,
                "no listener serves to advertise",
            )
        })?;
        advertiser.advertise(DNS_SD_SERVICE, name, txt)?;
        self.advertised = Some((name.to_owned(), advertiser));
        Ok(())
    }

    fn unregister(&mut self, name: &str) {
        if self
            .advertised
            .as_ref()
            .is_some_and(|(advertised, _)| advertised == name)
            && let Some((_, advertiser)) = self.advertised.take()
        {
            advertiser.withdraw();
        }
    }
}

impl Drop for NetworkSession {
    fn drop(&mut self) {
        if let Some((_, advertiser)) = self.advertised.take() {
            advertiser.withdraw();
        }
        let browser = self.browser.clone();
        self.queue.exec_async(move || browser.on_queue().cancel());
    }
}

/// The Bonjour instance name a browse result carries, when its endpoint is a
/// service.
fn name_of(result: &NWBrowseResult) -> Option<String> {
    let endpoint = result.endpoint();
    if endpoint.r#type() != nw_endpoint_type_t::bonjour_service {
        return None;
    }
    let name = endpoint.bonjour_service_name();
    // SAFETY: a service endpoint's name is NUL-terminated and lives as long
    // as the endpoint.
    let name = unsafe { CStr::from_ptr(name.as_ptr()) };
    Some(name.to_string_lossy().into_owned())
}

/// The resolve a browse result stands for, its TXT pairs and its endpoint.
fn resolved(result: &NWBrowseResult) -> Option<DnsSdEvent> {
    let name = name_of(result)?;
    let txt = result
        .txt_record_object()
        .map(|record| txt_pairs(&record))
        .unwrap_or_default();
    Some(DnsSdEvent::Resolved {
        name,
        txt,
        reach: Arc::new(Found(Endpoint(result.endpoint()))),
    })
}

/// Every key of a TXT record with its value, a key without a value carrying
/// none.
fn txt_pairs(record: &NWTxtRecord) -> Vec<(String, Option<Vec<u8>>)> {
    let pairs = Rc::new(RefCell::new(Vec::new()));
    let into = Rc::clone(&pairs);
    let each = RcBlock::new(
        move |key: NonNull<c_char>,
              found: nw_txt_record_find_key_t,
              value: *const u8,
              len: usize| {
            // SAFETY: a TXT key is NUL-terminated and lives for the block's
            // call.
            let key = unsafe { CStr::from_ptr(key.as_ptr()) };
            let value = (found == nw_txt_record_find_key_t::empty_value
                || found == nw_txt_record_find_key_t::non_empty_value)
                .then(|| {
                    if value.is_null() || len == 0 {
                        Vec::new()
                    } else {
                        // SAFETY: Network passes `len` live bytes for the
                        // block's call.
                        unsafe { std::slice::from_raw_parts(value, len) }.to_vec()
                    }
                });
            into.borrow_mut()
                .push((key.to_string_lossy().into_owned(), value));
            1u8
        },
    );
    let applier: &TxtApplier = &each;
    // SAFETY: the applier differs from `nw_txt_record_applier_t` only in its
    // `bool` spelled `u8` and its value pointer nullable, and it returns 1.
    record.apply(unsafe { recast(applier) });
    pairs.take()
}

/// A found instance's endpoint, dialled through Network framework.
struct Found(Endpoint);

impl std::fmt::Debug for Found {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("Found").finish_non_exhaustive()
    }
}

impl Dial for Found {
    fn dial<'a>(&'a self, node: &'a Node) -> DialFuture<'a> {
        Box::pin(link_endpoint(node, &self.0))
    }
}

#[cfg(all(test, target_os = "macos"))]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};
    use std::sync::Arc;
    use std::time::Duration;

    use connetto_core::device_cert::AttestationLevel;
    use connetto_peer::test_support::{DAY, Deployment, events, node, whole_second};
    use connetto_peer::{CloseReason, Discovery, DiscoveryEvent, Fingerprint};
    use tokio::sync::mpsc;

    use super::{AdvertiseSlot, Network};
    use crate::platform::network::Listener;

    const WAIT: Duration = Duration::from_secs(30);

    /// Wait until `rx` reports `wanted`, failing at the bound.
    async fn await_event(rx: &mut mpsc::UnboundedReceiver<DiscoveryEvent>, wanted: DiscoveryEvent) {
        tokio::time::timeout(WAIT, async {
            while rx.recv().await.expect("discovery reports") != wanted {}
        })
        .await
        .expect("the event arrives within the bound");
    }

    /// Two devices advertise through their listeners and browse through
    /// Network framework, one links the other by its fingerprint over the
    /// found endpoint, and the other's withdrawal reports it gone.
    #[tokio::test(flavor = "multi_thread")]
    async fn devices_find_link_and_lose_each_other_through_network_framework() {
        let now = whole_second();
        let deployment = Deployment::new(44, now);
        let issuer = deployment.add_issuer(now, [1; 16]);
        let alice = deployment.device(
            &issuer,
            "alice",
            now,
            DAY,
            [1; 16],
            AttestationLevel::Unproven,
        );
        let bob = deployment.device(
            &issuer,
            "bob",
            now,
            DAY,
            [2; 16],
            AttestationLevel::Unproven,
        );
        let (a_tx, _a_rx) = events();
        let (b_tx, _b_rx) = events();
        let a = node(deployment.root_der(), a_tx);
        let b = node(deployment.root_der(), b_tx);
        a.present(alice.identity());
        b.present(bob.identity());
        let any = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0));

        let mut serving = Vec::new();
        let mut found = Vec::new();
        for (node, peer) in [(&a, &alice), (&b, &bob)] {
            let listener = Listener::start(node.clone(), any)
                .await
                .expect("the listener is ready");
            let slot = AdvertiseSlot::default();
            slot.fill(listener.advertiser());
            let (found_tx, found_rx) = mpsc::unbounded_channel();
            let discovery = Discovery::new(node.clone(), false, found_tx)
                .with_dns_sd(Arc::new(Network::new(slot)));
            discovery.serve(listener.port(), Fingerprint::of(&peer.leaf));
            serving.push((listener, discovery));
            found.push(found_rx);
        }
        let bob_fp = Fingerprint::of(&bob.leaf);
        await_event(
            &mut found[0],
            DiscoveryEvent::Found {
                fingerprint: bob_fp,
            },
        )
        .await;

        let linked = serving[0]
            .1
            .found_links()
            .link(bob_fp)
            .await
            .expect("the found peer links");
        assert_eq!(linked, bob.identity);
        let links = a.linked();
        assert!(
            links
                .iter()
                .any(|link| link.peer == bob.identity && link.remote.is_some()),
            "the link names the address the found endpoint reached"
        );

        serving[1].1.stop();
        await_event(
            &mut found[0],
            DiscoveryEvent::Gone {
                fingerprint: bob_fp,
            },
        )
        .await;

        a.stop(CloseReason::Closed);
        b.stop(CloseReason::Closed);
    }
}
