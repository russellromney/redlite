#![no_main]

use libfuzzer_sys::fuzz_target;
use std::io::Cursor;

/// Synchronous RESP parser for fuzzing (mirrors the async RespReader logic)
fn parse_resp_command(data: &[u8]) -> Result<Option<Vec<Vec<u8>>>, &'static str> {
    let mut cursor = Cursor::new(data);

    // Read first line to get array marker
    let mut line = String::new();
    use std::io::BufRead;
    if std::io::BufReader::new(&mut cursor).read_line(&mut line).is_err() {
        return Err("failed to read line");
    }

    if line.is_empty() {
        return Ok(None); // EOF
    }

    if !line.starts_with('*') {
        return Err("expected array");
    }

    let count_str = line[1..].trim();
    let count: usize = count_str.parse().map_err(|_| "invalid array length")?;

    // Prevent excessive allocation
    if count > 1024 {
        return Err("array too large");
    }

    let mut args = Vec::with_capacity(count);

    for _ in 0..count {
        let arg = read_bulk_string(&mut cursor)?;
        args.push(arg);
    }

    Ok(Some(args))
}

fn read_bulk_string(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>, &'static str> {
    use std::io::{BufRead, Read};

    let mut line = String::new();
    if std::io::BufReader::new(&mut *cursor).read_line(&mut line).is_err() {
        return Err("failed to read bulk string header");
    }

    if !line.starts_with('$') {
        return Err("expected bulk string");
    }

    let len_str = line[1..].trim();
    let len: i64 = len_str.parse().map_err(|_| "invalid bulk string length")?;

    if len == -1 {
        return Ok(vec![]); // Null bulk string
    }

    if len < 0 || len > 512 * 1024 * 1024 {
        return Err("invalid bulk string length");
    }

    let len = len as usize;
    let mut data = vec![0u8; len + 2]; // +2 for \r\n

    // Read from current position in underlying data
    let pos = cursor.position() as usize;
    let remaining = &cursor.get_ref()[pos..];

    if remaining.len() < len + 2 {
        return Err("not enough data for bulk string");
    }

    data.copy_from_slice(&remaining[..len + 2]);
    cursor.set_position((pos + len + 2) as u64);

    data.truncate(len); // Remove \r\n
    Ok(data)
}

fuzz_target!(|data: &[u8]| {
    // Attempt to parse the data as a RESP command
    // The parser should never panic, only return errors
    let _ = parse_resp_command(data);
});
