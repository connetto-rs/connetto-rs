//! Build-time schema pipeline for the shared demo support crate.
//!
//! The crate shares the deployment's synced schema and policies and the
//! demos' local tier, so its `client` feature runs the schema step over those
//! three. A build without that feature holds no schema. The tier history in
//! `tier_history` is written by `connetto-schema export-tier --schema
//! examples/deployment/schema.sql --tier examples/demo-support/frontend.sql
//! --dir examples/demo-support/tier_history`.

fn main() {
    #[cfg(feature = "client")]
    connetto_schema::emit::<String, connetto_schema::TextIds>(
        std::path::Path::new("../deployment/schema.sql"),
        std::path::Path::new("../deployment/policies.sql"),
        Some(&connetto_schema::TierSource::new(
            "frontend.sql",
            "tier_history",
        )),
        &[],
    )
    .expect("translate the demo's schema");
}
