use core::time::Duration;

use connetto_client::{ClientBuilder, ClientEvent, ReconnectPolicy, SyncTuning, TransportFactory};
use connetto_core::messages::{
    BulkMessage, ControlMessage, CourierAuditOutcome, CourierAuditRequest,
};
use connetto_core::traits::{IncomingFrame, Transport};
use connetto_core::{LoopbackError, LoopbackTransport};
use futures_util::FutureExt;
use tokio::sync::{mpsc, oneshot};

use super::{
    AUTHOR, DDL, DialPlan, Observer, WITHIN, accepting, answer, barrier, bounded, request,
};

struct SendFailureTransport {
    inner: LoopbackTransport,
    failure: Option<oneshot::Sender<CourierAuditRequest>>,
}

impl Transport for SendFailureTransport {
    type Error = LoopbackError;

    async fn send_control(&mut self, message: ControlMessage) -> Result<(), Self::Error> {
        let message = match message {
            ControlMessage::CourierAuditRequest(request) => {
                if let Some(failure) = self.failure.take() {
                    failure.send(request).expect("failed request observer");
                    return Err(LoopbackError::Disconnected);
                }
                ControlMessage::CourierAuditRequest(request)
            }
            message => message,
        };
        self.inner.send_control(message).await
    }

    async fn send_bulk(&mut self, message: BulkMessage) -> Result<(), Self::Error> {
        self.inner.send_bulk(message).await
    }

    async fn recv(&mut self) -> Result<Option<IncomingFrame>, Self::Error> {
        self.inner.recv().await
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.inner.close().await
    }
}

struct SendFailureDialPlan {
    inner: DialPlan,
    failure: Option<oneshot::Sender<CourierAuditRequest>>,
}

impl TransportFactory for SendFailureDialPlan {
    type Transport = SendFailureTransport;
    type Error = <DialPlan as TransportFactory>::Error;

    async fn connect(&mut self) -> Result<Self::Transport, Self::Error> {
        Ok(SendFailureTransport {
            inner: self.inner.connect().await?,
            failure: self.failure.take(),
        })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn send_failure_reissues_the_retained_caller_with_a_fresh_route_and_original_bound() {
    let (mut server, port) = accepting().await;
    let (ports, receiver) = mpsc::channel(4);
    let (attempts, mut observed) = mpsc::channel(4);
    let (failure, failed) = oneshot::channel();
    ports.try_send((1, port)).expect("initial dial slot");
    let dialer = SendFailureDialPlan {
        inner: DialPlan {
            ports: receiver,
            attempts,
            next: 1,
        },
        failure: Some(failure),
    };
    let (running, pump) = Box::pin(bounded(
        "send failure initial handshake",
        ClientBuilder::new(super::super::support::bundle(DDL), dialer)
            .with_tuning(SyncTuning::default().with_probe_after(Duration::from_secs(3600)))
            .with_reconnect(
                ReconnectPolicy::new()
                    .with_initial_backoff(Duration::from_millis(10))
                    .with_max_backoff(Duration::from_millis(10)),
            )
            .with_sleeper(tokio::time::sleep)
            .signed_in(super::super::support::held("alice"))
            .connect_with_pump(),
    ))
    .await
    .expect("initial connection");
    assert_eq!(bounded("initial dial 1", observed.recv()).await, Some(1));
    let pump = tokio::spawn(pump);
    let client = running.client().clone();
    let mut events = client.events();
    barrier(&mut server, &mut events, 301).await;

    let mut wait: Observer<'_> =
        Box::pin(client.observe_courier_audit(&AUTHOR, &[121, 122], WITHIN));
    let first = tokio::select! {
        biased;
        result = &mut wait => panic!("observation completed before send failure, got {result:?}"),
        first = bounded("failed audit request", failed) => first.expect("request send failed"),
    };
    assert!(first.is_well_formed());
    assert_eq!(first.author, AUTHOR);
    assert_eq!(first.seqs, [121, 122]);
    assert_eq!(bounded(&first.request_id, observed.recv()).await, Some(2));
    assert!(wait.as_mut().now_or_never().is_none());

    let (mut recovered_server, port) = accepting().await;
    ports.try_send((2, port)).expect("recovery dial response 2");
    let second = request(&mut recovered_server, &mut wait).await;
    assert_ne!(first.request_id, second.request_id);
    assert_eq!(first.author, second.author);
    assert_eq!(first.seqs, second.seqs);
    assert!(second.remaining_ms < first.remaining_ms);

    answer(
        &mut recovered_server,
        &first.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    let recovery_events = barrier(&mut recovered_server, &mut events, 302).await;
    assert!(
        recovery_events
            .iter()
            .any(|event| matches!(event, ClientEvent::Reconnecting { .. }))
    );
    assert!(recovery_events.contains(&ClientEvent::Reconnected));
    assert!(wait.as_mut().now_or_never().is_none());
    answer(
        &mut recovered_server,
        &second.request_id,
        CourierAuditOutcome::Confirmed,
    )
    .await;
    bounded(&second.request_id, wait)
        .await
        .expect("retained caller receives confirmation");
    bounded("close send failure client", client.close()).await;
    bounded("send failure pump exit", pump)
        .await
        .expect("pump exits cleanly");
}
