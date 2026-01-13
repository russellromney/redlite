use std::fmt;

#[derive(Debug, Clone)]
pub enum ClientError {
    Connection(String),
    Operation(String),
    Timeout(String),
    KeyNotFound(String),
    TypeError(String),
    OutOfRange(String),
    Parse(String),
    Unknown(String),
}

impl fmt::Display for ClientError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ClientError::Connection(msg) => write!(f, "Connection error: {}", msg),
            ClientError::Operation(msg) => write!(f, "Operation error: {}", msg),
            ClientError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            ClientError::KeyNotFound(msg) => write!(f, "Key not found: {}", msg),
            ClientError::TypeError(msg) => write!(f, "Type error: {}", msg),
            ClientError::OutOfRange(msg) => write!(f, "Out of range: {}", msg),
            ClientError::Parse(msg) => write!(f, "Parse error: {}", msg),
            ClientError::Unknown(msg) => write!(f, "Unknown error: {}", msg),
        }
    }
}

impl std::error::Error for ClientError {}

#[derive(Debug)]
pub enum BenchError {
    Client(ClientError),
    Io(std::io::Error),
    Serialization(String),
    Configuration(String),
    NoDataPopulated(String),
    TaskFailed(String),
}

impl fmt::Display for BenchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BenchError::Client(e) => write!(f, "Client error: {}", e),
            BenchError::Io(e) => write!(f, "IO error: {}", e),
            BenchError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
            BenchError::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            BenchError::NoDataPopulated(msg) => write!(f, "No data populated: {}", msg),
            BenchError::TaskFailed(msg) => write!(f, "Task failed: {}", msg),
        }
    }
}

impl std::error::Error for BenchError {}

impl From<ClientError> for BenchError {
    fn from(e: ClientError) -> Self {
        BenchError::Client(e)
    }
}

impl From<std::io::Error> for BenchError {
    fn from(e: std::io::Error) -> Self {
        BenchError::Io(e)
    }
}

impl From<tokio::task::JoinError> for BenchError {
    fn from(e: tokio::task::JoinError) -> Self {
        BenchError::TaskFailed(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, BenchError>;
pub type ClientResult<T> = std::result::Result<T, ClientError>;
