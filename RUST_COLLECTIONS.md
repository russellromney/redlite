# Redlite Collections Implementation

## Hash Commands

```rust
// src/commands/hashes.rs

use std::collections::HashMap;

impl Db {
    /// HGET key field
    pub fn hget(&self, key: &str, field: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<Vec<u8>, _> = conn.query_row(
            "SELECT h.value FROM hashes h
             JOIN keys k ON k.id = h.key_id
             WHERE k.db = ?1 AND k.key = ?2 AND h.field = ?3
               AND (k.expire_at IS NULL OR k.expire_at > ?4)",
            params![db, key, field, now],
            |row| row.get(0),
        );

        match result {
            Ok(value) => Ok(Some(value)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// HSET key field value [field value ...]
    pub fn hset(&self, key: &str, fields: &[(&str, &[u8])]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let key_id = self.ensure_key(&conn, key, KeyType::Hash)?;

        let mut created = 0i64;
        for (field, value) in fields {
            // Check if field exists
            let existing: bool = conn
                .query_row(
                    "SELECT 1 FROM hashes WHERE key_id = ?1 AND field = ?2",
                    params![key_id, field],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            conn.execute(
                "INSERT INTO hashes (key_id, field, value) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key_id, field) DO UPDATE SET value = excluded.value",
                params![key_id, field, value],
            )?;

            if !existing {
                created += 1;
            }
        }

        Ok(created)
    }

    /// HMGET key field [field ...]
    pub fn hmget(&self, key: &str, fields: &[&str]) -> Result<Vec<Option<Vec<u8>>>> {
        fields.iter().map(|f| self.hget(key, f)).collect()
    }

    /// HGETALL key
    pub fn hgetall(&self, key: &str) -> Result<HashMap<String, Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT h.field, h.value FROM hashes h
             JOIN keys k ON k.id = h.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)",
        )?;

        let mut result = HashMap::new();
        let rows = stmt.query_map(params![db, key, now], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Vec<u8>>(1)?))
        })?;

        for row in rows {
            let (field, value) = row?;
            result.insert(field, value);
        }

        Ok(result)
    }

    /// HDEL key field [field ...]
    pub fn hdel(&self, key: &str, fields: &[&str]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();

        let key_id = match self.get_key_id(&conn, key, KeyType::Hash)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut deleted = 0i64;
        for field in fields {
            let count = conn.execute(
                "DELETE FROM hashes WHERE key_id = ?1 AND field = ?2",
                params![key_id, field],
            )?;
            deleted += count as i64;
        }

        Ok(deleted)
    }

    /// HEXISTS key field
    pub fn hexists(&self, key: &str, field: &str) -> Result<bool> {
        Ok(self.hget(key, field)?.is_some())
    }

    /// HKEYS key
    pub fn hkeys(&self, key: &str) -> Result<Vec<String>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT h.field FROM hashes h
             JOIN keys k ON k.id = h.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)",
        )?;

        let keys: Vec<String> = stmt
            .query_map(params![db, key, now], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(keys)
    }

    /// HVALS key
    pub fn hvals(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT h.value FROM hashes h
             JOIN keys k ON k.id = h.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)",
        )?;

        let values: Vec<Vec<u8>> = stmt
            .query_map(params![db, key, now], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(values)
    }

    /// HLEN key
    pub fn hlen(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM hashes h
                 JOIN keys k ON k.id = h.key_id
                 WHERE k.db = ?1 AND k.key = ?2
                   AND (k.expire_at IS NULL OR k.expire_at > ?3)",
                params![db, key, now],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count)
    }

    /// HINCRBY key field delta
    pub fn hincrby(&self, key: &str, field: &str, delta: i64) -> Result<i64> {
        let current = self.hget(key, field)?;

        let val: i64 = match current {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|_| KvError::NotInteger)?;
                s.parse().map_err(|_| KvError::NotInteger)?
            }
            None => 0,
        };

        let new_val = val + delta;
        self.hset(key, &[(field, new_val.to_string().as_bytes())])?;
        Ok(new_val)
    }

    /// HINCRBYFLOAT key field delta
    pub fn hincrbyfloat(&self, key: &str, field: &str, delta: f64) -> Result<f64> {
        let current = self.hget(key, field)?;

        let val: f64 = match current {
            Some(bytes) => {
                let s = std::str::from_utf8(&bytes).map_err(|_| KvError::NotFloat)?;
                s.parse().map_err(|_| KvError::NotFloat)?
            }
            None => 0.0,
        };

        let new_val = val + delta;
        self.hset(key, &[(field, new_val.to_string().as_bytes())])?;
        Ok(new_val)
    }

    /// HSETNX key field value
    pub fn hsetnx(&self, key: &str, field: &str, value: &[u8]) -> Result<bool> {
        if self.hexists(key, field)? {
            return Ok(false);
        }
        self.hset(key, &[(field, value)])?;
        Ok(true)
    }
}
```

## List Commands

```rust
// src/commands/lists.rs

const POS_GAP: i64 = 1_000_000;

impl Db {
    /// LPUSH key value [value ...]
    pub fn lpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        self.list_push(key, values, true)
    }

    /// RPUSH key value [value ...]
    pub fn rpush(&self, key: &str, values: &[&[u8]]) -> Result<i64> {
        self.list_push(key, values, false)
    }

    fn list_push(&self, key: &str, values: &[&[u8]], left: bool) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let key_id = self.ensure_key(&conn, key, KeyType::List)?;

        // Get edge position
        let edge_pos: Option<i64> = if left {
            conn.query_row(
                "SELECT MIN(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get(0),
            ).ok().flatten()
        } else {
            conn.query_row(
                "SELECT MAX(pos) FROM lists WHERE key_id = ?1",
                params![key_id],
                |row| row.get(0),
            ).ok().flatten()
        };

        let start_pos = edge_pos.unwrap_or(0);

        for (i, value) in values.iter().enumerate() {
            let pos = if left {
                start_pos - ((values.len() - i) as i64) * POS_GAP
            } else {
                start_pos + ((i + 1) as i64) * POS_GAP
            };

            conn.execute(
                "INSERT INTO lists (key_id, pos, value) VALUES (?1, ?2, ?3)",
                params![key_id, pos, value],
            )?;
        }

        // Return new length
        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        Ok(length)
    }

    /// LPOP key
    pub fn lpop(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.list_pop(key, true)
    }

    /// RPOP key
    pub fn rpop(&self, key: &str) -> Result<Option<Vec<u8>>> {
        self.list_pop(key, false)
    }

    fn list_pop(&self, key: &str, left: bool) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();

        let key_id = match self.get_key_id(&conn, key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let order = if left { "ASC" } else { "DESC" };
        let sql = format!(
            "SELECT rowid, value FROM lists WHERE key_id = ?1 ORDER BY pos {} LIMIT 1",
            order
        );

        let result: std::result::Result<(i64, Vec<u8>), _> =
            conn.query_row(&sql, params![key_id], |row| Ok((row.get(0)?, row.get(1)?)));

        match result {
            Ok((rowid, value)) => {
                conn.execute("DELETE FROM lists WHERE rowid = ?1", params![rowid])?;
                Ok(Some(value))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// LLEN key
    pub fn llen(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM lists l
                 JOIN keys k ON k.id = l.key_id
                 WHERE k.db = ?1 AND k.key = ?2
                   AND (k.expire_at IS NULL OR k.expire_at > ?3)",
                params![db, key, now],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count)
    }

    /// LRANGE key start stop
    pub fn lrange(&self, key: &str, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let length = {
            let l: i64 = conn
                .query_row(
                    "SELECT COUNT(*) FROM lists l
                     JOIN keys k ON k.id = l.key_id
                     WHERE k.db = ?1 AND k.key = ?2
                       AND (k.expire_at IS NULL OR k.expire_at > ?3)",
                    params![db, key, now],
                    |row| row.get(0),
                )
                .unwrap_or(0);
            l
        };

        // Normalize indices
        let start = if start < 0 { (length + start).max(0) } else { start.min(length) };
        let stop = if stop < 0 { (length + stop).max(0) } else { stop.min(length - 1) };

        if start > stop || length == 0 {
            return Ok(vec![]);
        }

        let limit = stop - start + 1;

        let mut stmt = conn.prepare(
            "SELECT value FROM lists l
             JOIN keys k ON k.id = l.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
             ORDER BY l.pos
             LIMIT ?4 OFFSET ?5",
        )?;

        let values: Vec<Vec<u8>> = stmt
            .query_map(params![db, key, now, limit, start], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(values)
    }

    /// LINDEX key index
    pub fn lindex(&self, key: &str, index: i64) -> Result<Option<Vec<u8>>> {
        let values = self.lrange(key, index, index)?;
        Ok(values.into_iter().next())
    }

    /// LSET key index value
    pub fn lset(&self, key: &str, index: i64, value: &[u8]) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let length = self.llen(key)?;
        let index = if index < 0 { length + index } else { index };

        if index < 0 || index >= length {
            return Err(KvError::OutOfRange);
        }

        // Get the rowid at this index
        let rowid: i64 = conn.query_row(
            "SELECT l.rowid FROM lists l
             JOIN keys k ON k.id = l.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
             ORDER BY l.pos
             LIMIT 1 OFFSET ?4",
            params![db, key, now, index],
            |row| row.get(0),
        )?;

        conn.execute(
            "UPDATE lists SET value = ?1 WHERE rowid = ?2",
            params![value, rowid],
        )?;

        Ok(())
    }

    /// LTRIM key start stop
    pub fn ltrim(&self, key: &str, start: i64, stop: i64) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        
        let key_id = match self.get_key_id(&conn, key, KeyType::List)? {
            Some(id) => id,
            None => return Ok(()),
        };

        let length: i64 = conn.query_row(
            "SELECT COUNT(*) FROM lists WHERE key_id = ?1",
            params![key_id],
            |row| row.get(0),
        )?;

        let start = if start < 0 { (length + start).max(0) } else { start };
        let stop = if stop < 0 { (length + stop).max(-1) } else { stop };

        // Get positions to keep
        let positions: Vec<i64> = {
            let mut stmt = conn.prepare(
                "SELECT pos FROM lists WHERE key_id = ?1 ORDER BY pos LIMIT ?2 OFFSET ?3"
            )?;
            stmt.query_map(params![key_id, stop - start + 1, start], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect()
        };

        if positions.is_empty() {
            // Delete all
            conn.execute("DELETE FROM lists WHERE key_id = ?1", params![key_id])?;
        } else {
            let min_pos = positions.first().unwrap();
            let max_pos = positions.last().unwrap();
            conn.execute(
                "DELETE FROM lists WHERE key_id = ?1 AND (pos < ?2 OR pos > ?3)",
                params![key_id, min_pos, max_pos],
            )?;
        }

        Ok(())
    }
}
```

## Set Commands

```rust
// src/commands/sets.rs

impl Db {
    /// SADD key member [member ...]
    pub fn sadd(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let key_id = self.ensure_key(&conn, key, KeyType::Set)?;

        let mut added = 0i64;
        for member in members {
            let result = conn.execute(
                "INSERT OR IGNORE INTO sets (key_id, member) VALUES (?1, ?2)",
                params![key_id, member],
            )?;
            added += result as i64;
        }

        Ok(added)
    }

    /// SREM key member [member ...]
    pub fn srem(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();

        let key_id = match self.get_key_id(&conn, key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut removed = 0i64;
        for member in members {
            let result = conn.execute(
                "DELETE FROM sets WHERE key_id = ?1 AND member = ?2",
                params![key_id, member],
            )?;
            removed += result as i64;
        }

        Ok(removed)
    }

    /// SMEMBERS key
    pub fn smembers(&self, key: &str) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let mut stmt = conn.prepare(
            "SELECT s.member FROM sets s
             JOIN keys k ON k.id = s.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)",
        )?;

        let members: Vec<Vec<u8>> = stmt
            .query_map(params![db, key, now], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(members)
    }

    /// SISMEMBER key member
    pub fn sismember(&self, key: &str, member: &[u8]) -> Result<bool> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let exists: bool = conn
            .query_row(
                "SELECT 1 FROM sets s
                 JOIN keys k ON k.id = s.key_id
                 WHERE k.db = ?1 AND k.key = ?2 AND s.member = ?3
                   AND (k.expire_at IS NULL OR k.expire_at > ?4)",
                params![db, key, member, now],
                |_| Ok(true),
            )
            .unwrap_or(false);

        Ok(exists)
    }

    /// SCARD key
    pub fn scard(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sets s
                 JOIN keys k ON k.id = s.key_id
                 WHERE k.db = ?1 AND k.key = ?2
                   AND (k.expire_at IS NULL OR k.expire_at > ?3)",
                params![db, key, now],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count)
    }

    /// SPOP key
    pub fn spop(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();

        let key_id = match self.get_key_id(&conn, key, KeyType::Set)? {
            Some(id) => id,
            None => return Ok(None),
        };

        let result: std::result::Result<(i64, Vec<u8>), _> = conn.query_row(
            "SELECT rowid, member FROM sets WHERE key_id = ?1 ORDER BY RANDOM() LIMIT 1",
            params![key_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        );

        match result {
            Ok((rowid, member)) => {
                conn.execute("DELETE FROM sets WHERE rowid = ?1", params![rowid])?;
                Ok(Some(member))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// SRANDMEMBER key [count]
    pub fn srandmember(&self, key: &str, count: Option<i64>) -> Result<Vec<Vec<u8>>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();
        let count = count.unwrap_or(1).abs();

        let mut stmt = conn.prepare(
            "SELECT s.member FROM sets s
             JOIN keys k ON k.id = s.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
             ORDER BY RANDOM()
             LIMIT ?4",
        )?;

        let members: Vec<Vec<u8>> = stmt
            .query_map(params![db, key, now, count], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        Ok(members)
    }

    /// SINTER key [key ...]
    pub fn sinter(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result = self.smembers(keys[0])?;

        for key in &keys[1..] {
            let members = self.smembers(key)?;
            result.retain(|m| members.contains(m));
        }

        Ok(result)
    }

    /// SUNION key [key ...]
    pub fn sunion(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        let mut result: Vec<Vec<u8>> = vec![];

        for key in keys {
            let members = self.smembers(key)?;
            for member in members {
                if !result.contains(&member) {
                    result.push(member);
                }
            }
        }

        Ok(result)
    }

    /// SDIFF key [key ...]
    pub fn sdiff(&self, keys: &[&str]) -> Result<Vec<Vec<u8>>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }

        let mut result = self.smembers(keys[0])?;

        for key in &keys[1..] {
            let members = self.smembers(key)?;
            result.retain(|m| !members.contains(m));
        }

        Ok(result)
    }

    /// SMOVE source destination member
    pub fn smove(&self, source: &str, destination: &str, member: &[u8]) -> Result<bool> {
        // Check if member exists in source
        if !self.sismember(source, member)? {
            return Ok(false);
        }

        // Remove from source
        self.srem(source, &[member])?;

        // Add to destination
        self.sadd(destination, &[member])?;

        Ok(true)
    }
}
```

## Sorted Set Commands

```rust
// src/commands/zsets.rs

use crate::types::ZMember;

impl Db {
    /// ZADD key score member [score member ...]
    pub fn zadd(&self, key: &str, members: &[ZMember]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let key_id = self.ensure_key(&conn, key, KeyType::ZSet)?;

        let mut added = 0i64;
        for m in members {
            let existing: bool = conn
                .query_row(
                    "SELECT 1 FROM zsets WHERE key_id = ?1 AND member = ?2",
                    params![key_id, &m.member],
                    |_| Ok(true),
                )
                .unwrap_or(false);

            conn.execute(
                "INSERT INTO zsets (key_id, member, score) VALUES (?1, ?2, ?3)
                 ON CONFLICT(key_id, member) DO UPDATE SET score = excluded.score",
                params![key_id, &m.member, m.score],
            )?;

            if !existing {
                added += 1;
            }
        }

        Ok(added)
    }

    /// ZREM key member [member ...]
    pub fn zrem(&self, key: &str, members: &[&[u8]]) -> Result<i64> {
        let conn = self.conn.lock().unwrap();

        let key_id = match self.get_key_id(&conn, key, KeyType::ZSet)? {
            Some(id) => id,
            None => return Ok(0),
        };

        let mut removed = 0i64;
        for member in members {
            let result = conn.execute(
                "DELETE FROM zsets WHERE key_id = ?1 AND member = ?2",
                params![key_id, member],
            )?;
            removed += result as i64;
        }

        Ok(removed)
    }

    /// ZSCORE key member
    pub fn zscore(&self, key: &str, member: &[u8]) -> Result<Option<f64>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let result: std::result::Result<f64, _> = conn.query_row(
            "SELECT z.score FROM zsets z
             JOIN keys k ON k.id = z.key_id
             WHERE k.db = ?1 AND k.key = ?2 AND z.member = ?3
               AND (k.expire_at IS NULL OR k.expire_at > ?4)",
            params![db, key, member, now],
            |row| row.get(0),
        );

        match result {
            Ok(score) => Ok(Some(score)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// ZRANK key member
    pub fn zrank(&self, key: &str, member: &[u8]) -> Result<Option<i64>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        // Get all members in order and find position
        let mut stmt = conn.prepare(
            "SELECT z.member FROM zsets z
             JOIN keys k ON k.id = z.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
             ORDER BY z.score, z.member",
        )?;

        let members: Vec<Vec<u8>> = stmt
            .query_map(params![db, key, now], |row| row.get(0))?
            .filter_map(|r| r.ok())
            .collect();

        for (i, m) in members.iter().enumerate() {
            if m == member {
                return Ok(Some(i as i64));
            }
        }

        Ok(None)
    }

    /// ZREVRANK key member
    pub fn zrevrank(&self, key: &str, member: &[u8]) -> Result<Option<i64>> {
        let rank = self.zrank(key, member)?;
        let card = self.zcard(key)?;
        Ok(rank.map(|r| card - r - 1))
    }

    /// ZCARD key
    pub fn zcard(&self, key: &str) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM zsets z
                 JOIN keys k ON k.id = z.key_id
                 WHERE k.db = ?1 AND k.key = ?2
                   AND (k.expire_at IS NULL OR k.expire_at > ?3)",
                params![db, key, now],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count)
    }

    /// ZRANGE key start stop [WITHSCORES]
    pub fn zrange(&self, key: &str, start: i64, stop: i64, with_scores: bool) -> Result<Vec<ZMember>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let length = self.zcard(key)?;

        let start = if start < 0 { (length + start).max(0) } else { start.min(length) };
        let stop = if stop < 0 { (length + stop).max(0) } else { stop.min(length - 1) };

        if start > stop {
            return Ok(vec![]);
        }

        let limit = stop - start + 1;

        let mut stmt = conn.prepare(
            "SELECT z.member, z.score FROM zsets z
             JOIN keys k ON k.id = z.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
             ORDER BY z.score, z.member
             LIMIT ?4 OFFSET ?5",
        )?;

        let members: Vec<ZMember> = stmt
            .query_map(params![db, key, now, limit, start], |row| {
                Ok(ZMember {
                    member: row.get(0)?,
                    score: row.get(1)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(members)
    }

    /// ZRANGEBYSCORE key min max [LIMIT offset count]
    pub fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        offset: Option<i64>,
        count: Option<i64>,
    ) -> Result<Vec<ZMember>> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let offset = offset.unwrap_or(0);
        let limit = count.unwrap_or(i64::MAX);

        let mut stmt = conn.prepare(
            "SELECT z.member, z.score FROM zsets z
             JOIN keys k ON k.id = z.key_id
             WHERE k.db = ?1 AND k.key = ?2
               AND (k.expire_at IS NULL OR k.expire_at > ?3)
               AND z.score >= ?4 AND z.score <= ?5
             ORDER BY z.score, z.member
             LIMIT ?6 OFFSET ?7",
        )?;

        let members: Vec<ZMember> = stmt
            .query_map(params![db, key, now, min, max, limit, offset], |row| {
                Ok(ZMember {
                    member: row.get(0)?,
                    score: row.get(1)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(members)
    }

    /// ZCOUNT key min max
    pub fn zcount(&self, key: &str, min: f64, max: f64) -> Result<i64> {
        let conn = self.conn.lock().unwrap();
        let db = self.current_db();
        let now = Self::now_ms();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM zsets z
                 JOIN keys k ON k.id = z.key_id
                 WHERE k.db = ?1 AND k.key = ?2
                   AND (k.expire_at IS NULL OR k.expire_at > ?3)
                   AND z.score >= ?4 AND z.score <= ?5",
                params![db, key, now, min, max],
                |row| row.get(0),
            )
            .unwrap_or(0);

        Ok(count)
    }

    /// ZINCRBY key increment member
    pub fn zincrby(&self, key: &str, delta: f64, member: &[u8]) -> Result<f64> {
        let current = self.zscore(key, member)?.unwrap_or(0.0);
        let new_score = current + delta;

        self.zadd(key, &[ZMember {
            score: new_score,
            member: member.to_vec(),
        }])?;

        Ok(new_score)
    }

    /// ZREMRANGEBYRANK key start stop
    pub fn zremrangebyrank(&self, key: &str, start: i64, stop: i64) -> Result<i64> {
        let members = self.zrange(key, start, stop, false)?;
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.member.as_slice()).collect();
        self.zrem(key, &member_refs)
    }

    /// ZREMRANGEBYSCORE key min max
    pub fn zremrangebyscore(&self, key: &str, min: f64, max: f64) -> Result<i64> {
        let members = self.zrangebyscore(key, min, max, None, None)?;
        let member_refs: Vec<&[u8]> = members.iter().map(|m| m.member.as_slice()).collect();
        self.zrem(key, &member_refs)
    }
}
```
