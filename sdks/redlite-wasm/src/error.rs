use thiserror::Error;
use wasm_bindgen::JsError;

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
}

impl From<sqlite_wasm_rs::Error> for WasmError {
    fn from(e: sqlite_wasm_rs::Error) -> Self {
        WasmError::Sqlite(e.to_string())
    }
}

impl From<WasmError> for JsError {
    fn from(e: WasmError) -> Self {
        JsError::new(&e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, WasmError>;
