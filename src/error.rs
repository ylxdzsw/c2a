use std::io;

use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Http(#[from] reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Serialize)]
pub struct ApiError<'a> {
    pub error: ApiErrorBody<'a>,
}

#[derive(Debug, Serialize)]
pub struct ApiErrorBody<'a> {
    pub message: &'a str,
    pub r#type: &'static str,
    pub request_id: &'a str,
}

impl Error {
    pub fn message(value: impl Into<String>) -> Self {
        Self::Message(value.into())
    }
}
