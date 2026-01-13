use thiserror::Error;

#[derive(Error, Debug)]
pub enum KvError {
    #[error("key not found")]
    NotFound,

    #[error("wrong type for key")]
    WrongType,

    #[error("value is not an integer")]
    NotInteger,

    #[error("value is not a float")]
    NotFloat,

    #[error("syntax error")]
    SyntaxError,

    #[error("index out of range")]
    OutOfRange,

    #[error("no such key")]
    NoSuchKey,

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("invalid data")]
    InvalidData,

    #[error("BUSYGROUP Consumer Group name already exists")]
    BusyGroup,

    #[error("NOGROUP No such consumer group")]
    NoGroup,

    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, KvError>;
