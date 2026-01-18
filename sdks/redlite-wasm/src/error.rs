use thiserror::Error;

#[derive(Error, Debug)]
pub enum WasmError {
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

    #[error("invalid expire time")]
    InvalidExpireTime,

    #[error("invalid cursor")]
    InvalidCursor,

    #[error("invalid data")]
    InvalidData,

    #[error("{0}")]
    InvalidArgument(String),

    #[error("BUSYGROUP Consumer Group name already exists")]
    BusyGroup,

    #[error("NOGROUP No such consumer group")]
    NoGroup,

    #[error("sqlite error: {0}")]
    Sqlite(String),

    #[error("{0}")]
    Other(String),

    #[error("{0} not yet implemented")]
    NotImplemented(String),
}

impl From<sqlite_wasm_rs::Error> for WasmError {
    fn from(e: sqlite_wasm_rs::Error) -> Self {
        WasmError::Sqlite(e.to_string())
    }
}

// Note: wasm-bindgen already provides a blanket impl `From<E> for JsError where E: StdError`
// so we don't need to implement it manually since WasmError derives thiserror::Error

pub type Result<T> = std::result::Result<T, WasmError>;
