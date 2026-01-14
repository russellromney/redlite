#[derive(Debug, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Option<Vec<RespValue>>),
}

impl RespValue {
    pub fn ok() -> Self {
        RespValue::SimpleString("OK".to_string())
    }

    pub fn pong() -> Self {
        RespValue::SimpleString("PONG".to_string())
    }

    pub fn null() -> Self {
        RespValue::BulkString(None)
    }

    pub fn error(msg: impl Into<String>) -> Self {
        RespValue::Error(format!("ERR {}", msg.into()))
    }

    pub fn wrong_type() -> Self {
        RespValue::Error(
            "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
        )
    }

    pub fn from_bytes(bytes: Vec<u8>) -> Self {
        RespValue::BulkString(Some(bytes))
    }

    pub fn from_string(s: String) -> Self {
        RespValue::BulkString(Some(s.into_bytes()))
    }

    pub fn encode(&self) -> Vec<u8> {
        match self {
            RespValue::SimpleString(s) => format!("+{}\r\n", s).into_bytes(),
            RespValue::Error(s) => format!("-{}\r\n", s).into_bytes(),
            RespValue::Integer(n) => format!(":{}\r\n", n).into_bytes(),
            RespValue::BulkString(None) => b"$-1\r\n".to_vec(),
            RespValue::BulkString(Some(data)) => {
                let mut buf = format!("${}\r\n", data.len()).into_bytes();
                buf.extend_from_slice(data);
                buf.extend_from_slice(b"\r\n");
                buf
            }
            RespValue::Array(None) => b"*-1\r\n".to_vec(),
            RespValue::Array(Some(items)) => {
                let mut buf = format!("*{}\r\n", items.len()).into_bytes();
                for item in items {
                    buf.extend(item.encode());
                }
                buf
            }
        }
    }
}

impl From<i64> for RespValue {
    fn from(n: i64) -> Self {
        RespValue::Integer(n)
    }
}

impl From<bool> for RespValue {
    fn from(b: bool) -> Self {
        RespValue::Integer(if b { 1 } else { 0 })
    }
}

impl From<String> for RespValue {
    fn from(s: String) -> Self {
        RespValue::from_string(s)
    }
}

impl From<Vec<u8>> for RespValue {
    fn from(bytes: Vec<u8>) -> Self {
        RespValue::from_bytes(bytes)
    }
}

impl From<Option<Vec<u8>>> for RespValue {
    fn from(opt: Option<Vec<u8>>) -> Self {
        match opt {
            Some(bytes) => RespValue::BulkString(Some(bytes)),
            None => RespValue::BulkString(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_simple_string() {
        let v = RespValue::SimpleString("OK".to_string());
        assert_eq!(v.encode(), b"+OK\r\n");
    }

    #[test]
    fn test_encode_error() {
        let v = RespValue::Error("ERR something went wrong".to_string());
        assert_eq!(v.encode(), b"-ERR something went wrong\r\n");
    }

    #[test]
    fn test_encode_integer() {
        let v = RespValue::Integer(42);
        assert_eq!(v.encode(), b":42\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let v = RespValue::BulkString(Some(b"hello".to_vec()));
        assert_eq!(v.encode(), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_null_bulk_string() {
        let v = RespValue::BulkString(None);
        assert_eq!(v.encode(), b"$-1\r\n");
    }

    #[test]
    fn test_encode_array() {
        let v = RespValue::Array(Some(vec![
            RespValue::BulkString(Some(b"foo".to_vec())),
            RespValue::BulkString(Some(b"bar".to_vec())),
        ]));
        assert_eq!(v.encode(), b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }
}
