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

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, KvError>;
