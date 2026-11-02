#![doc = include_str!("../../../docs/architecture/README.md")]

// Included only so rustdoc checks the chapters' symbol links. Their file links
// target the repository, so the rendered pages are not a reading surface.
macro_rules! chapter {
    ($name:ident, $file:literal) => {
        #[doc = include_str!(concat!("../../../docs/architecture/", $file))]
        pub mod $name {}
    };
}

chapter!(overview, "00-overview.md");
chapter!(pieces, "01-pieces.md");
chapter!(protocol, "02-protocol.md");
chapter!(sync_pipeline, "03-sync-pipeline.md");
chapter!(subscriptions, "04-subscriptions.md");
chapter!(aggregate_queries, "05-aggregate-queries.md");
chapter!(reconnect, "06-reconnect.md");
chapter!(authorization, "08-authorization.md");
chapter!(wasm, "09-wasm.md");
chapter!(subscription_materializer, "10-subscription-materializer.md");
chapter!(authentication, "11-authentication.md");
chapter!(
    identity_session_capability,
    "12-identity-session-capability.md"
);
chapter!(client_connection, "13-client-connection.md");
chapter!(at_rest_encryption, "14-at-rest-encryption.md");
chapter!(replica_retention, "15-replica-retention.md");
chapter!(server_capacity, "16-server-capacity.md");
chapter!(fan_out, "17-fan-out.md");
chapter!(file_handling, "18-file-handling.md");
chapter!(device_to_device, "19-device-to-device.md");
chapter!(deployment, "20-deployment.md");
chapter!(clocks, "21-clocks.md");
chapter!(subql, "subql.md");
