use std::io;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

pub struct RespReader<R> {
    reader: BufReader<R>,
}

impl<R: AsyncReadExt + Unpin> RespReader<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
        }
    }

    /// Read a Redis command (array of bulk strings)
    pub async fn read_command(&mut self) -> io::Result<Option<Vec<Vec<u8>>>> {
        let mut line = String::new();
        let n = self.reader.read_line(&mut line).await?;
        if n == 0 {
            return Ok(None); // EOF
        }

        if !line.starts_with('*') {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "expected array"));
        }

        let count: usize = line[1..].trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid array length")
        })?;

        let mut args = Vec::with_capacity(count);
        for _ in 0..count {
            let arg = self.read_bulk_string().await?;
            args.push(arg);
        }

        Ok(Some(args))
    }

    async fn read_bulk_string(&mut self) -> io::Result<Vec<u8>> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;

        if !line.starts_with('$') {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "expected bulk string",
            ));
        }

        let len: i64 = line[1..].trim().parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "invalid bulk string length")
        })?;

        if len == -1 {
            return Ok(vec![]); // Null bulk string
        }

        let len = len as usize;
        let mut data = vec![0u8; len + 2]; // +2 for \r\n
        self.reader.read_exact(&mut data).await?;

        data.truncate(len); // Remove \r\n
        Ok(data)
    }
}
