//! Build-time schema pipeline for this demo.
//!
//! The demo shares the deployment's synced schema, policies and roles and
//! adds its own local tier, so it runs the shared schema step over those four. The
//! tier history lives in `tier_history` beside the source, and the build
//! step only reads that directory. Its exports are written by
//! `demo-schema export-tier`, run from the workspace root as `cargo run
//! -p connetto-demo-tools --bin demo-schema -- export-tier --schema
//! examples/deployment/schema.sql --tier examples/wasm-smoke/frontend.sql
//! --dir examples/wasm-smoke/tier_history`.

fn main() {
    connetto_translate::emit::<String, connetto_translate::TextIds>(
        [
            std::path::Path::new("../deployment/schema.sql"),
            std::path::Path::new("../deployment/policies.sql"),
            std::path::Path::new("../deployment/roles.sql"),
        ],
        "connetto_reader",
        connetto_translate::KindRoles::defaults(),
        connetto_translate::UnsetSettings::ALWAYS_BOUND,
        Some(&connetto_translate::TierSource::new(
            "frontend.sql",
            "tier_history",
        )),
        &[],
    )
    .expect("translate the demo's schema");
}
