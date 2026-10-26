//! The `connetto-schema` binary over a throwaway history directory: its
//! exit codes, its output, and the exports it writes or refuses.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The binary under test.
fn command() -> Command {
    Command::new(env!("CARGO_BIN_EXE_connetto-schema"))
}

/// The schema and tier sources the command translates, written under `dir`.
fn sources(dir: &Path) -> (PathBuf, PathBuf) {
    let schema = dir.join("schema.sql");
    let tier = dir.join("tier.sql");
    std::fs::write(
        &schema,
        "-- connetto:major 1\nCREATE TABLE orders (id INT PRIMARY KEY, label TEXT);",
    )
    .expect("the schema source writes");
    std::fs::write(&tier, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT);")
        .expect("the tier source writes");
    (schema, tier)
}

/// A run without a command prints the usage and fails.
#[test]
fn a_run_without_a_command_prints_the_usage_and_fails() {
    let out = command().output().expect("the command runs");
    assert_eq!(out.status.code(), Some(1), "FAILURE is exit code 1");
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.contains("connetto-schema export-tier --schema"),
        "the usage names the command: {stderr}"
    );
    assert!(out.stdout.is_empty(), "the failure says nothing to stdout");
}

/// A run with an unknown command prints the command's name and fails.
#[test]
fn a_run_with_an_unknown_command_prints_the_name_and_fails() {
    let out = command()
        .arg("frobnicate")
        .output()
        .expect("the command runs");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.contains("unknown command \"frobnicate\""),
        "the unknown command is named: {stderr}"
    );
    assert!(
        stderr.contains("connetto-schema export-tier --schema"),
        "the usage follows the refusal: {stderr}"
    );
}

/// A run with an unknown argument prints the argument's name and fails.
#[test]
fn a_run_with_an_unknown_argument_prints_the_name_and_fails() {
    let out = command()
        .args(["export-tier", "--bogus"])
        .output()
        .expect("the command runs");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.contains("unknown argument \"--bogus\""),
        "the unknown argument is named: {stderr}"
    );
}

/// A run missing a required flag prints the usage and fails.
#[test]
fn a_run_missing_a_required_flag_prints_the_usage_and_fails() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (schema, tier) = sources(dir.path());
    let out = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&schema)
        .arg("--tier")
        .arg(&tier)
        .output()
        .expect("the command runs");
    assert_eq!(out.status.code(), Some(1));
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.contains("connetto-schema export-tier --schema"),
        "the usage names every required flag: {stderr}"
    );
}

/// A write run lands the export for the schema's major and says where.
#[test]
fn a_write_run_lands_the_export_for_the_schema_major() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (schema, tier) = sources(dir.path());
    let history = dir.path().join("tier_history");
    let out = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&schema)
        .arg("--tier")
        .arg(&tier)
        .arg("--dir")
        .arg(&history)
        .output()
        .expect("the command runs");
    assert!(out.status.success(), "the write run succeeds: {out:?}");
    let stdout = String::from_utf8(out.stdout).expect("utf-8");
    assert_eq!(
        stdout,
        format!("wrote {}\n", history.join("1.sql").display()),
        "the write run names the export it landed"
    );
    let export = std::fs::read_to_string(history.join("1.sql")).expect("the export lands");
    assert!(
        export.contains("CREATE TABLE notes"),
        "the export holds the translated tier: {export}"
    );
}

/// A check run over a fresh export succeeds and says nothing.
#[test]
fn a_check_run_over_a_fresh_export_succeeds_silently() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (schema, tier) = sources(dir.path());
    let history = dir.path().join("tier_history");
    let write = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&schema)
        .arg("--tier")
        .arg(&tier)
        .arg("--dir")
        .arg(&history)
        .output()
        .expect("the write run runs");
    assert!(write.status.success(), "the write run succeeds: {write:?}");
    let out = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&schema)
        .arg("--tier")
        .arg(&tier)
        .arg("--dir")
        .arg(&history)
        .arg("--check")
        .output()
        .expect("the check run runs");
    assert!(out.status.success(), "a fresh export checks clean: {out:?}");
    assert!(out.stdout.is_empty(), "the check run says nothing");
    let before = std::fs::read_to_string(history.join("1.sql")).expect("the export reads");
    assert_eq!(
        std::fs::read_to_string(history.join("1.sql")).expect("the export reads again"),
        before,
        "the check run writes nothing"
    );
}

/// A check run over a drifted export fails and names the error.
#[test]
fn a_check_run_over_a_drifted_export_fails() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (schema, tier) = sources(dir.path());
    let history = dir.path().join("tier_history");
    std::fs::create_dir_all(&history).expect("the history directory writes");
    std::fs::write(
        history.join("1.sql"),
        "an export the sources no longer compute",
    )
    .expect("the drifted export writes");
    let out = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&schema)
        .arg("--tier")
        .arg(&tier)
        .arg("--dir")
        .arg(&history)
        .arg("--check")
        .output()
        .expect("the command runs");
    assert_eq!(
        out.status.code(),
        Some(1),
        "a drifted export fails the check"
    );
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.starts_with("connetto-schema:"),
        "the failure names the error: {stderr}"
    );
    assert!(out.stdout.is_empty(), "the failure says nothing to stdout");
}

/// A run over a missing schema file fails and names the error.
#[test]
fn a_run_over_a_missing_schema_file_fails() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let (_schema, tier) = sources(dir.path());
    let absent = dir.path().join("absent.sql");
    let out = command()
        .arg("export-tier")
        .arg("--schema")
        .arg(&absent)
        .arg("--tier")
        .arg(&tier)
        .arg("--dir")
        .arg(dir.path().join("tier_history"))
        .output()
        .expect("the command runs");
    assert_eq!(out.status.code(), Some(1), "a missing source fails the run");
    let stderr = String::from_utf8(out.stderr).expect("utf-8");
    assert!(
        stderr.starts_with("connetto-schema:"),
        "the failure names the error: {stderr}"
    );
}
