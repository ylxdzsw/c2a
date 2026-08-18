use std::{ffi::OsStr, fmt};

pub const USER_AGENT: &str = concat!("c2a/", env!("CARGO_PKG_VERSION"));

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Provider {
    Codex,
    Copilot,
}

impl Provider {
    pub fn parse(value: &OsStr) -> Option<Self> {
        if value == "codex" {
            Some(Self::Codex)
        } else if value == "copilot" {
            Some(Self::Copilot)
        } else {
            None
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Codex => "codex",
            Self::Copilot => "copilot",
        }
    }
}

impl fmt::Display for Provider {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}
