use std::{
    env,
    fs::{self, File, Metadata},
    os::unix::fs::{MetadataExt, PermissionsExt},
    path::{Path, PathBuf},
};

use crate::error::{Error, Result};

#[derive(Clone, Debug)]
pub struct Paths {
    pub dir: PathBuf,
    pub auth: PathBuf,
    pub lock: PathBuf,
    pub audit: PathBuf,
}

impl Paths {
    pub fn new() -> Result<Self> {
        let base = if let Some(value) = env::var_os("XDG_STATE_HOME") {
            let path = PathBuf::from(value);
            if !path.is_absolute() {
                return Err(Error::message("XDG_STATE_HOME must be absolute"));
            }
            path
        } else {
            let home = env::var_os("HOME")
                .ok_or_else(|| Error::message("no absolute XDG or HOME state path is available"))?;
            let path = PathBuf::from(home);
            if !path.is_absolute() {
                return Err(Error::message("HOME must be absolute"));
            }
            path.join(".local/state")
        };
        let dir = base.join("c2a");
        Ok(Self {
            auth: dir.join("auth.json"),
            lock: dir.join("auth.lock"),
            audit: dir.join("audit.jsonl"),
            dir,
        })
    }

    pub fn ensure_dir(&self) -> Result<()> {
        if self.dir.exists() {
            check_path(&self.dir, true, 0o700)?;
        } else {
            fs::create_dir_all(&self.dir)?;
            fs::set_permissions(&self.dir, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }
}

pub fn check_path(path: &Path, directory: bool, mode: u32) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        return Err(Error::message(format!(
            "refusing symlink: {}",
            path.display()
        )));
    }
    check_metadata(path, &metadata, directory, mode)
}

pub fn check_file(file: &File, path: &Path, mode: u32) -> Result<()> {
    check_metadata(path, &file.metadata()?, false, mode)
}

fn check_metadata(path: &Path, metadata: &Metadata, directory: bool, mode: u32) -> Result<()> {
    if directory && !metadata.is_dir() {
        return Err(Error::message(format!(
            "not a directory: {}",
            path.display()
        )));
    }
    if !directory && !metadata.is_file() {
        return Err(Error::message(format!(
            "not a regular file: {}",
            path.display()
        )));
    }
    if metadata.uid() != unsafe { libc::geteuid() } {
        return Err(Error::message(format!("wrong owner: {}", path.display())));
    }
    if metadata.permissions().mode() & 0o077 != 0 || metadata.permissions().mode() & 0o777 != mode {
        return Err(Error::message(format!(
            "insecure permissions: {}",
            path.display()
        )));
    }
    Ok(())
}
