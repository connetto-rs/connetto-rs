//! Tauri's build step, the deployment root a `device-identity` build ships and
//! the addresses an Android build dials. The schema comes from
//! `connetto-demo-support`, which translates it with the demos' local tier.

/// The path of the deployment root's `root.der`, which `connetto-demo-stack`
/// mints and names to the program it runs.
const DEVICE_ROOT: &str = "CONNETTO_DEMO_BUILD_DEVICE_ROOT";

/// The addresses an Android build bakes in, since an app launched there reads
/// no environment.
const ANDROID_ENDPOINTS: [&str; 2] = ["CONNETTO_DEMO_BUILD_WS", "CONNETTO_DEMO_BUILD_AUTH_ORIGIN"];

/// Refuses an Android build that would dial the loopback default, which only a
/// phone behind `adb reverse` reaches.
fn require_android_endpoints() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("android") {
        return;
    }
    for name in ANDROID_ENDPOINTS {
        println!("cargo:rerun-if-env-changed={name}");
    }
    let missing: Vec<&str> = ANDROID_ENDPOINTS
        .into_iter()
        .filter(|name| std::env::var_os(name).is_none_or(|value| value.is_empty()))
        .collect();
    assert!(
        missing.is_empty(),
        "an Android build dials only the addresses baked into it, and this one has no {}. Set \
         CONNETTO_DEMO_BUILD_WS and CONNETTO_DEMO_BUILD_AUTH_ORIGIN to the CONNETTO_DEMO_WS and \
         CONNETTO_DEMO_AUTH_ORIGIN connetto-demo-stack prints, ws://127.0.0.1:7777/sync and \
         http://127.0.0.1:7777 for a phone behind adb reverse",
        missing.join(" and ")
    );
}

fn main() {
    require_android_endpoints();
    if std::env::var_os("CARGO_FEATURE_DEVICE_IDENTITY").is_some() {
        println!("cargo:rerun-if-env-changed={DEVICE_ROOT}");
        // The Tauri CLI hands Xcode's build only CARGO_, RUST_ and TAURI
        // variables, so an iOS build reads the root the demo stack mints in
        // the cargo target directory, a relative one taken from the repository
        // root as the stack takes it.
        let target = std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into());
        let minted = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../..")
            .join(target)
            .join("demo-device-ca/root.der");
        let Some(path) = std::env::var_os(DEVICE_ROOT)
            .map(std::path::PathBuf::from)
            .or_else(|| minted.exists().then_some(minted))
        else {
            panic!("the device-identity feature ships a deployment root: set {DEVICE_ROOT}");
        };
        println!(
            "cargo:rerun-if-changed={}",
            std::path::Path::new(&path).display()
        );
        let root = std::fs::read(&path).expect("read the deployment root");
        let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
        std::fs::write(out.join("device-root.der"), root).expect("write the deployment root");
    }

    tauri_build::build();
}
