use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        io::AsRawFd,
    },
};

use serde::{Deserialize, Serialize};

use crate::{
    error::{Error, Result},
    paths::{Paths, check_file, check_path},
};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Credentials {
    pub version: u8,
    pub access_token: String,
    pub refresh_token: String,
}

pub struct Lock {
    _file: File,
}

impl Lock {
    pub fn acquire(paths: &Paths) -> Result<Self> {
        paths.ensure_dir()?;
        let created = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&paths.lock);
        let file = match created {
            Ok(file) => {
                file.set_permissions(fs::Permissions::from_mode(0o600))?;
                file
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
                .open(&paths.lock)?,
            Err(error) => return Err(error.into()),
        };
        check_file(&file, &paths.lock, 0o600)?;
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } != 0 {
            return Err(Error::Io(std::io::Error::last_os_error()));
        }
        Ok(Self { _file: file })
    }
}

pub fn load(paths: &Paths) -> Result<Option<Credentials>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&paths.auth)
    {
        Ok(v) => v,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    check_file(&file, &paths.auth, 0o600)?;
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let value: Credentials = serde_json::from_str(&text)?;
    if value.version != 1 || value.access_token.is_empty() || value.refresh_token.is_empty() {
        return Err(Error::message("invalid c2a credentials"));
    }
    Ok(Some(value))
}

pub fn store(paths: &Paths, credentials: &Credentials) -> Result<()> {
    paths.ensure_dir()?;
    match fs::symlink_metadata(&paths.auth) {
        Ok(_) => check_path(&paths.auth, false, 0o600)?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let name = format!(".auth.{}", random_hex(16)?);
    let temp = paths.dir.join(name);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(&temp)?;
    file.set_permissions(fs::Permissions::from_mode(0o600))?;
    let bytes = serde_json::to_vec(credentials)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temp, &paths.auth)?;
    let dir = File::open(&paths.dir)?;
    dir.sync_all()?;
    Ok(())
}

pub fn remove(paths: &Paths) -> Result<()> {
    match fs::symlink_metadata(&paths.auth) {
        Ok(_) => {
            check_path(&paths.auth, false, 0o600)?;
            fs::remove_file(&paths.auth)?;
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn random_hex(bytes: usize) -> Result<String> {
    let mut value = vec![0u8; bytes];
    getrandom::fill(&mut value).map_err(|e| Error::message(e.to_string()))?;
    Ok(value.iter().map(|v| format!("{v:02x}")).collect())
}
