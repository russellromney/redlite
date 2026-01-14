//! Database backend selection for CLI and configuration

/// Backend selection for database
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// SQLite via rusqlite (default, sync)
    Sqlite,
    /// Turso (Rust-native SQLite rewrite with MVCC)
    #[cfg(feature = "turso")]
    Turso,
}

impl Default for Backend {
    fn default() -> Self {
        Backend::Sqlite
    }
}

impl Backend {
    /// Parse from string (for CLI/env var)
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "sqlite" | "rusqlite" => Some(Backend::Sqlite),
            #[cfg(feature = "turso")]
            "turso" => Some(Backend::Turso),
            _ => None,
        }
    }

    /// Check if this backend is available (feature enabled)
    pub fn is_available(&self) -> bool {
        match self {
            Backend::Sqlite => true,
            #[cfg(feature = "turso")]
            Backend::Turso => true,
        }
    }
}
