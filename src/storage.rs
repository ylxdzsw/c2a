use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    os::unix::{
        fs::{OpenOptionsExt, PermissionsExt},
        io::AsRawFd,
    },
};

use serde::{Deserialize, Serialize, de::DeserializeOwned};

use crate::{
    error::{Error, Result},
    paths::{Paths, check_file},
    provider::Provider,
};

const CREDENTIAL_LIMIT: u64 = 64 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CodexCredentials {
    pub version: u8,
    pub access_token: String,
    pub refresh_token: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CopilotCredentials {
    pub version: u8,
    pub access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_expires_at: Option<i64>,
}

pub struct CredentialLock {
    file: File,
    provider: Provider,
}

impl CredentialLock {
    pub fn load_codex(&mut self) -> Result<Option<CodexCredentials>> {
        if self.provider != Provider::Codex {
            return Err(Error::message("credential provider mismatch"));
        }
        let value = read_json(&mut self.file)?;
        validate_codex(value)
    }

    pub fn load_copilot(&mut self) -> Result<Option<CopilotCredentials>> {
        if self.provider != Provider::Copilot {
            return Err(Error::message("credential provider mismatch"));
        }
        let value = read_json(&mut self.file)?;
        validate_copilot(value)
    }

    pub fn store<T: Serialize>(&mut self, credentials: &T) -> Result<()> {
        let bytes = serde_json::to_vec(credentials)?;
        if bytes.len() as u64 > CREDENTIAL_LIMIT {
            return Err(Error::message("credentials exceed 64 KiB"));
        }
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&bytes)?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn clear(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.file.sync_all()?;
        Ok(())
    }
}

pub fn load_codex(paths: &Paths) -> Result<Option<CodexCredentials>> {
    let mut file = open(paths, Provider::Codex)?;
    flock(&file, libc::LOCK_SH)?;
    let result = validate_codex(read_json(&mut file)?);
    flock(&file, libc::LOCK_UN)?;
    result
}

pub fn load_copilot(paths: &Paths) -> Result<Option<CopilotCredentials>> {
    let mut file = open(paths, Provider::Copilot)?;
    flock(&file, libc::LOCK_SH)?;
    let result = validate_copilot(read_json(&mut file)?);
    flock(&file, libc::LOCK_UN)?;
    result
}

pub fn store_codex(paths: &Paths, credentials: &CodexCredentials) -> Result<()> {
    let mut lock = lock(paths, Provider::Codex)?;
    lock.store(credentials)
}

pub fn store_copilot(paths: &Paths, credentials: &CopilotCredentials) -> Result<()> {
    let mut lock = lock(paths, Provider::Copilot)?;
    lock.store(credentials)
}

pub fn clear(paths: &Paths, provider: Provider) -> Result<()> {
    let mut lock = lock(paths, provider)?;
    lock.clear()
}

pub fn lock(paths: &Paths, provider: Provider) -> Result<CredentialLock> {
    let file = open(paths, provider)?;
    flock(&file, libc::LOCK_EX)?;
    Ok(CredentialLock { file, provider })
}

fn open(paths: &Paths, provider: Provider) -> Result<File> {
    paths.ensure_dir()?;
    let path = paths.credentials(provider);
    let created = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .mode(0o600)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .open(path);
    let file = match created {
        Ok(file) => {
            file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
            file
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(path)?,
        Err(error) => return Err(error.into()),
    };
    check_file(&file, path, 0o600)?;
    Ok(file)
}

fn flock(file: &File, operation: libc::c_int) -> Result<()> {
    if unsafe { libc::flock(file.as_raw_fd(), operation) } != 0 {
        return Err(Error::Io(std::io::Error::last_os_error()));
    }
    Ok(())
}

fn read_json<T: DeserializeOwned>(file: &mut File) -> Result<Option<T>> {
    file.seek(SeekFrom::Start(0))?;
    let mut bytes = Vec::new();
    file.take(CREDENTIAL_LIMIT + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > CREDENTIAL_LIMIT {
        return Err(Error::message("credentials exceed 64 KiB"));
    }
    if bytes.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn validate_codex(value: Option<CodexCredentials>) -> Result<Option<CodexCredentials>> {
    if let Some(credentials) = &value
        && (credentials.version != 1
            || credentials.access_token.is_empty()
            || credentials.refresh_token.is_empty())
    {
        return Err(Error::message(
            "invalid Codex credentials; run `c2a codex login`",
        ));
    }
    Ok(value)
}

fn validate_copilot(value: Option<CopilotCredentials>) -> Result<Option<CopilotCredentials>> {
    if let Some(credentials) = &value
        && (credentials.version != 1
            || credentials.access_token.is_empty()
            || credentials.refresh_token.as_deref() == Some("")
            || credentials.expires_at.is_some_and(|value| value <= 0)
            || credentials
                .refresh_expires_at
                .is_some_and(|value| value <= 0)
            || (credentials.refresh_expires_at.is_some() && credentials.refresh_token.is_none()))
    {
        return Err(Error::message(
            "invalid Copilot credentials; run `c2a copilot login`",
        ));
    }
    Ok(value)
}
