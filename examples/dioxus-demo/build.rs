//! The deployment root a build with the `device-identity` feature ships and
//! the addresses an Android build dials.

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
        let Some(path) = std::env::var_os(DEVICE_ROOT) else {
            panic!(
                "the device-identity feature ships a deployment root: set {DEVICE_ROOT} to the \
                 root.der connetto-demo-stack minted under target/demo-device-ca"
            );
        };
        println!(
            "cargo:rerun-if-changed={}",
            std::path::Path::new(&path).display()
        );
        let root = std::fs::read(&path).expect("read the deployment root");
        let out = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("OUT_DIR"));
        std::fs::write(out.join("device-root.der"), root).expect("write the deployment root");
    }
}
