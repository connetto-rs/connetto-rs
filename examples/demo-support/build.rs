//! Build-time schema pipeline for the shared demo support crate.
//!
//! The crate shares the deployment's synced schema and policies and the
//! demos' local tier, so it runs the schema step over those three.

fn main() {
    connetto_schema::emit::<String, connetto_schema::TextIds>(
        std::path::Path::new("../deployment/schema.sql"),
        std::path::Path::new("../deployment/policies.sql"),
        Some(std::path::Path::new("frontend.sql")),
        &[],
    )
    .expect("translate the demo's schema");
}
