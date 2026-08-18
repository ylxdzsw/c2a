#![allow(dead_code)]

use std::{
    fs,
    os::unix::fs::{MetadataExt, PermissionsExt, symlink},
    path::PathBuf,
    process::Command,
};

#[path = "../src/error.rs"]
mod error;
#[path = "../src/paths.rs"]
mod paths;
#[path = "../src/provider.rs"]
mod provider;
#[path = "../src/storage.rs"]
mod storage;

use paths::Paths;
use storage::{CodexCredentials, CopilotCredentials};

fn temp_dir() -> PathBuf {
    let mut random = [0u8; 16];
    getrandom::fill(&mut random).unwrap();
    let path = std::env::temp_dir().join(format!(
        "c2a-test-{}-{}",
        std::process::id(),
        random
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    ));
    fs::create_dir(&path).unwrap();
    path
}

fn paths(root: PathBuf) -> Paths {
    let dir = root.join("c2a");
    Paths {
        codex: dir.join("codex.json"),
        copilot: dir.join("copilot.json"),
        audit: dir.join("audit.jsonl"),
        dir,
    }
}

#[test]
fn unmatched_cli_shapes_exit_two() {
    for arguments in [
        Vec::<&str>::new(),
        vec!["login"],
        vec!["codex", "login", "extra"],
        vec!["codex", "serve", "one", "two"],
        vec!["unknown", "status"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_c2a"))
            .args(arguments)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        assert!(String::from_utf8_lossy(&output.stderr).contains("usage: c2a"));
    }
}

#[test]
fn help_and_version_succeed() {
    for arguments in [
        vec!["--help"],
        vec!["help", "codex"],
        vec!["copilot", "--help"],
        vec!["--version"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_c2a"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(!output.stdout.is_empty());
    }
}

#[test]
fn credentials_are_private_minimal_and_provider_specific() {
    let root = temp_dir();
    let paths = paths(root.clone());
    let credentials = CodexCredentials {
        version: 1,
        access_token: "access".into(),
        refresh_token: "refresh".into(),
    };
    storage::store_codex(&paths, &credentials).unwrap();

    assert_eq!(fs::metadata(&paths.dir).unwrap().mode() & 0o777, 0o700);
    assert_eq!(fs::metadata(&paths.codex).unwrap().mode() & 0o777, 0o600);
    let value: serde_json::Value =
        serde_json::from_slice(&fs::read(&paths.codex).unwrap()).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 3);
    assert_eq!(
        storage::load_codex(&paths).unwrap().unwrap().access_token,
        "access"
    );
    assert_eq!(fs::read_dir(&paths.dir).unwrap().count(), 1);

    fs::remove_dir_all(root).unwrap();
}

#[test]
fn credential_symlinks_are_rejected() {
    let root = temp_dir();
    let paths = paths(root.clone());
    paths.ensure_dir().unwrap();
    let target = root.join("target");
    fs::write(&target, "secret").unwrap();
    symlink(&target, &paths.codex).unwrap();
    assert!(storage::load_codex(&paths).is_err());
    assert_eq!(fs::read_to_string(target).unwrap(), "secret");
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn insecure_credential_permissions_are_rejected() {
    let root = temp_dir();
    let paths = paths(root.clone());
    paths.ensure_dir().unwrap();
    fs::write(
        &paths.codex,
        r#"{"version":1,"access_token":"access","refresh_token":"refresh"}"#,
    )
    .unwrap();
    fs::set_permissions(&paths.codex, fs::Permissions::from_mode(0o644)).unwrap();
    assert!(storage::load_codex(&paths).is_err());
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn corrupt_provider_credentials_can_be_overwritten_independently() {
    let root = temp_dir();
    let paths = paths(root.clone());
    paths.ensure_dir().unwrap();
    fs::write(&paths.codex, "{bad").unwrap();
    fs::set_permissions(&paths.codex, fs::Permissions::from_mode(0o600)).unwrap();
    storage::store_copilot(
        &paths,
        &CopilotCredentials {
            version: 1,
            access_token: "github".into(),
            expires_at: None,
            refresh_token: None,
            refresh_expires_at: None,
        },
    )
    .unwrap();
    assert!(storage::load_codex(&paths).is_err());
    assert_eq!(
        storage::load_copilot(&paths).unwrap().unwrap().access_token,
        "github"
    );
    storage::store_codex(
        &paths,
        &CodexCredentials {
            version: 1,
            access_token: "new-access".into(),
            refresh_token: "new-refresh".into(),
        },
    )
    .unwrap();
    assert_eq!(
        storage::load_codex(&paths).unwrap().unwrap().access_token,
        "new-access"
    );
    fs::remove_dir_all(root).unwrap();
}
