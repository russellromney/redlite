use crate::error::{Result, WasmError};
use crate::schema::SCHEMA_CORE;
use crate::types::KeyType;
use sqlite_wasm_rs as ffi;
use std::ffi::{CStr, CString};
use std::ptr;

/// Database connection wrapper for sqlite-wasm-rs
pub struct DbCore {
    db: *mut ffi::sqlite3,
    selected_db: i32,
}

impl DbCore {
    /// Create a new in-memory database
    pub fn new() -> Result<Self> {
        let mut db = ptr::null_mut();
        let path = CString::new(":memory:").unwrap();

        let ret = unsafe {
            ffi::sqlite3_open_v2(
                path.as_ptr(),
                &mut db,
                ffi::SQLITE_OPEN_READWRITE | ffi::SQLITE_OPEN_CREATE,
                ptr::null(),
            )
        };

        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("Failed to open database: {}", ret)));
        }

        let mut core = Self { db, selected_db: 0 };
        core.init_schema()?;
        Ok(core)
    }

    /// Initialize the database schema
    fn init_schema(&mut self) -> Result<()> {
        self.execute_batch(SCHEMA_CORE)?;
        // Enable foreign keys
        self.execute_batch("PRAGMA foreign_keys = ON;")?;
        Ok(())
    }

    /// Execute multiple SQL statements
    pub fn execute_batch(&mut self, sql: &str) -> Result<()> {
        let sql_cstr = CString::new(sql).map_err(|e| WasmError::Other(e.to_string()))?;
        let ret = unsafe {
            ffi::sqlite3_exec(
                self.db,
                sql_cstr.as_ptr(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            )
        };

        if ret != ffi::SQLITE_OK {
            return Err(self.get_error());
        }
        Ok(())
    }

    /// Get the current error message
    fn get_error(&self) -> WasmError {
        unsafe {
            let msg = ffi::sqlite3_errmsg(self.db);
            if msg.is_null() {
                WasmError::Sqlite("Unknown SQLite error".to_string())
            } else {
                let msg_str = CStr::from_ptr(msg).to_string_lossy().to_string();
                WasmError::Sqlite(msg_str)
            }
        }
    }

    /// Get the current selected database (0-15)
    pub fn selected_db(&self) -> i32 {
        self.selected_db
    }

    /// Select a database (0-15)
    pub fn select(&mut self, db: i32) -> Result<()> {
        if db < 0 || db > 15 {
            return Err(WasmError::InvalidArgument("db must be 0-15".to_string()));
        }
        self.selected_db = db;
        Ok(())
    }

    /// Get current timestamp in milliseconds
    pub fn now_ms() -> i64 {
        js_sys::Date::now() as i64
    }

    // ========================================================================
    // Key Management
    // ========================================================================

    /// Get key ID, checking for expiration
    pub fn get_key_id(&self, key: &str) -> Result<Option<i64>> {
        let sql = "SELECT id, expire_at FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.prepare(sql)?;
        stmt.bind_int(1, self.selected_db)?;
        stmt.bind_text(2, key)?;

        if stmt.step()? {
            let expire_at: Option<i64> = stmt.column_int64_opt(1);
            if let Some(exp) = expire_at {
                if exp <= Self::now_ms() {
                    // Key has expired, delete it
                    drop(stmt);
                    self.delete_key_by_name(key)?;
                    return Ok(None);
                }
            }
            Ok(Some(stmt.column_int64(0)))
        } else {
            Ok(None)
        }
    }

    /// Get key ID with type checking
    pub fn get_key_id_typed(&self, key: &str, expected_type: KeyType) -> Result<Option<i64>> {
        let sql = "SELECT id, type, expire_at FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.prepare(sql)?;
        stmt.bind_int(1, self.selected_db)?;
        stmt.bind_text(2, key)?;

        if stmt.step()? {
            let expire_at: Option<i64> = stmt.column_int64_opt(2);
            if let Some(exp) = expire_at {
                if exp <= Self::now_ms() {
                    drop(stmt);
                    self.delete_key_by_name(key)?;
                    return Ok(None);
                }
            }

            let key_type = stmt.column_int(1);
            if key_type != expected_type as i32 {
                return Err(WasmError::WrongType);
            }
            Ok(Some(stmt.column_int64(0)))
        } else {
            Ok(None)
        }
    }

    /// Create or update a key entry
    pub fn upsert_key(&self, key: &str, key_type: KeyType, ttl_ms: Option<i64>) -> Result<i64> {
        let now = Self::now_ms();
        let expire_at = ttl_ms.map(|ttl| now + ttl);

        let sql = r#"
            INSERT INTO keys (db, key, type, expire_at, version, created_at, updated_at)
            VALUES (?1, ?2, ?3, ?4, 0, ?5, ?5)
            ON CONFLICT(db, key) DO UPDATE SET
                type = excluded.type,
                expire_at = excluded.expire_at,
                version = version + 1,
                updated_at = excluded.updated_at
            RETURNING id
        "#;

        let mut stmt = self.prepare(sql)?;
        stmt.bind_int(1, self.selected_db)?;
        stmt.bind_text(2, key)?;
        stmt.bind_int(3, key_type as i32)?;
        if let Some(exp) = expire_at {
            stmt.bind_int64(4, exp)?;
        } else {
            stmt.bind_null(4)?;
        }
        stmt.bind_int64(5, now)?;

        if stmt.step()? {
            Ok(stmt.column_int64(0))
        } else {
            Err(WasmError::Sqlite("Failed to upsert key".to_string()))
        }
    }

    /// Delete a key by name
    pub fn delete_key_by_name(&self, key: &str) -> Result<bool> {
        let sql = "DELETE FROM keys WHERE db = ?1 AND key = ?2";
        let mut stmt = self.prepare(sql)?;
        stmt.bind_int(1, self.selected_db)?;
        stmt.bind_text(2, key)?;
        stmt.step()?;
        Ok(self.changes() > 0)
    }

    /// Get number of changes from last statement
    fn changes(&self) -> i32 {
        unsafe { ffi::sqlite3_changes(self.db) }
    }

    /// Get last insert rowid
    pub fn last_insert_rowid(&self) -> i64 {
        unsafe { ffi::sqlite3_last_insert_rowid(self.db) }
    }

    // ========================================================================
    // Statement preparation
    // ========================================================================

    /// Prepare a SQL statement
    pub fn prepare(&self, sql: &str) -> Result<Statement> {
        let sql_cstr = CString::new(sql).map_err(|e| WasmError::Other(e.to_string()))?;
        let mut stmt = ptr::null_mut();

        let ret = unsafe {
            ffi::sqlite3_prepare_v2(
                self.db,
                sql_cstr.as_ptr(),
                sql.len() as i32 + 1,
                &mut stmt,
                ptr::null_mut(),
            )
        };

        if ret != ffi::SQLITE_OK {
            return Err(self.get_error());
        }

        Ok(Statement { stmt })
    }
}

impl Drop for DbCore {
    fn drop(&mut self) {
        if !self.db.is_null() {
            unsafe {
                ffi::sqlite3_close(self.db);
            }
        }
    }
}

/// Prepared statement wrapper
pub struct Statement {
    stmt: *mut ffi::sqlite3_stmt,
}

impl Statement {
    /// Bind an integer parameter
    pub fn bind_int(&mut self, idx: i32, val: i32) -> Result<()> {
        let ret = unsafe { ffi::sqlite3_bind_int(self.stmt, idx, val) };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_int failed: {}", ret)));
        }
        Ok(())
    }

    /// Bind a 64-bit integer parameter
    pub fn bind_int64(&mut self, idx: i32, val: i64) -> Result<()> {
        let ret = unsafe { ffi::sqlite3_bind_int64(self.stmt, idx, val) };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_int64 failed: {}", ret)));
        }
        Ok(())
    }

    /// Bind a text parameter
    pub fn bind_text(&mut self, idx: i32, val: &str) -> Result<()> {
        let val_cstr = CString::new(val).map_err(|e| WasmError::Other(e.to_string()))?;
        let ret = unsafe {
            ffi::sqlite3_bind_text(
                self.stmt,
                idx,
                val_cstr.as_ptr(),
                val.len() as i32,
                ffi::SQLITE_TRANSIENT(),
            )
        };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_text failed: {}", ret)));
        }
        Ok(())
    }

    /// Bind a blob parameter
    pub fn bind_blob(&mut self, idx: i32, val: &[u8]) -> Result<()> {
        let ret = unsafe {
            ffi::sqlite3_bind_blob(
                self.stmt,
                idx,
                val.as_ptr() as *const _,
                val.len() as i32,
                ffi::SQLITE_TRANSIENT(),
            )
        };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_blob failed: {}", ret)));
        }
        Ok(())
    }

    /// Bind a null parameter
    pub fn bind_null(&mut self, idx: i32) -> Result<()> {
        let ret = unsafe { ffi::sqlite3_bind_null(self.stmt, idx) };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_null failed: {}", ret)));
        }
        Ok(())
    }

    /// Bind a double parameter
    pub fn bind_double(&mut self, idx: i32, val: f64) -> Result<()> {
        let ret = unsafe { ffi::sqlite3_bind_double(self.stmt, idx, val) };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("bind_double failed: {}", ret)));
        }
        Ok(())
    }

    /// Step to the next row, returns true if there is a row
    pub fn step(&mut self) -> Result<bool> {
        let ret = unsafe { ffi::sqlite3_step(self.stmt) };
        match ret {
            ffi::SQLITE_ROW => Ok(true),
            ffi::SQLITE_DONE => Ok(false),
            _ => Err(WasmError::Sqlite(format!("step failed: {}", ret))),
        }
    }

    /// Reset the statement for re-use
    pub fn reset(&mut self) -> Result<()> {
        let ret = unsafe { ffi::sqlite3_reset(self.stmt) };
        if ret != ffi::SQLITE_OK {
            return Err(WasmError::Sqlite(format!("reset failed: {}", ret)));
        }
        Ok(())
    }

    /// Get an integer column value
    pub fn column_int(&self, idx: i32) -> i32 {
        unsafe { ffi::sqlite3_column_int(self.stmt, idx) }
    }

    /// Get a 64-bit integer column value
    pub fn column_int64(&self, idx: i32) -> i64 {
        unsafe { ffi::sqlite3_column_int64(self.stmt, idx) }
    }

    /// Get an optional 64-bit integer column value
    pub fn column_int64_opt(&self, idx: i32) -> Option<i64> {
        let col_type = unsafe { ffi::sqlite3_column_type(self.stmt, idx) };
        if col_type == ffi::SQLITE_NULL {
            None
        } else {
            Some(self.column_int64(idx))
        }
    }

    /// Get a double column value
    pub fn column_double(&self, idx: i32) -> f64 {
        unsafe { ffi::sqlite3_column_double(self.stmt, idx) }
    }

    /// Get a text column value
    pub fn column_text(&self, idx: i32) -> String {
        unsafe {
            let ptr = ffi::sqlite3_column_text(self.stmt, idx);
            if ptr.is_null() {
                String::new()
            } else {
                CStr::from_ptr(ptr as *const _).to_string_lossy().to_string()
            }
        }
    }

    /// Get a blob column value
    pub fn column_blob(&self, idx: i32) -> Vec<u8> {
        unsafe {
            let ptr = ffi::sqlite3_column_blob(self.stmt, idx);
            let len = ffi::sqlite3_column_bytes(self.stmt, idx);
            if ptr.is_null() || len == 0 {
                Vec::new()
            } else {
                std::slice::from_raw_parts(ptr as *const u8, len as usize).to_vec()
            }
        }
    }

    /// Check if column is null
    pub fn column_is_null(&self, idx: i32) -> bool {
        unsafe { ffi::sqlite3_column_type(self.stmt, idx) == ffi::SQLITE_NULL }
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        if !self.stmt.is_null() {
            unsafe {
                ffi::sqlite3_finalize(self.stmt);
            }
        }
    }
}
