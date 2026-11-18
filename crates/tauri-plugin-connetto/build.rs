//! Generates the permissions an application grants the plugin's commands.

const COMMANDS: &[&str] = &["subscribe", "stop", "ack"];

fn main() {
    tauri_plugin::Builder::new(COMMANDS).build();
}
