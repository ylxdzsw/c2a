#![allow(dead_code)]

use std::{
    fs,
    os::unix::fs::{MetadataExt, symlink},
    path::PathBuf,
    process::Command,
};

#[path = "../src/error.rs"]
mod error;
#[path = "../src/paths.rs"]
mod paths;
#[path = "../src/storage.rs"]
mod storage;

use paths::Paths;
use storage::Credentials;

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
        auth: dir.join("auth.json"),
        lock: dir.join("auth.lock"),
        audit: dir.join("audit.jsonl"),
        dir,
    }
}

#[test]
fn unmatched_cli_shapes_exit_two() {
    for arguments in [Vec::<&str>::new(), vec!["login", "extra"], vec!["unknown"]] {
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
    for arguments in [vec!["--help"], vec!["help", "serve"], vec!["--version"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_c2a"))
            .args(arguments)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(!output.stdout.is_empty());
    }
}

#[test]
fn credentials_are_atomic_private_and_minimal() {
    let root = temp_dir();
    let paths = paths(root.clone());
    let credentials = Credentials {
        version: 1,
        access_token: "access".into(),
        refresh_token: "refresh".into(),
    };
    storage::store(&paths, &credentials).unwrap();

    assert_eq!(fs::metadata(&paths.dir).unwrap().mode() & 0o777, 0o700);
    assert_eq!(fs::metadata(&paths.auth).unwrap().mode() & 0o777, 0o600);
    let value: serde_json::Value = serde_json::from_slice(&fs::read(&paths.auth).unwrap()).unwrap();
    assert_eq!(value.as_object().unwrap().len(), 3);
    assert_eq!(
        storage::load(&paths).unwrap().unwrap().access_token,
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
    symlink(&target, &paths.auth).unwrap();
    assert!(storage::load(&paths).is_err());
    assert_eq!(fs::read_to_string(target).unwrap(), "secret");
    fs::remove_dir_all(root).unwrap();
}
