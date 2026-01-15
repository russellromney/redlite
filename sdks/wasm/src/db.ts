/**
 * Core database layer using sql.js (SQLite compiled to WASM)
 */

import type { Database, SqlJsStatic } from 'sql.js';
import { KeyType } from './types';

/** Schema SQL for core tables */
const SCHEMA_CORE = `
-- Core key metadata
CREATE TABLE IF NOT EXISTS keys (
    id INTEGER PRIMARY KEY,
    db INTEGER NOT NULL DEFAULT 0,
    key TEXT NOT NULL,
    type INTEGER NOT NULL,
    expire_at INTEGER,
    version INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
    updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_keys_db_key ON keys(db, key);
CREATE INDEX IF NOT EXISTS idx_keys_expire ON keys(expire_at) WHERE expire_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_keys_type ON keys(db, type);

-- Strings
CREATE TABLE IF NOT EXISTS strings (
    key_id INTEGER PRIMARY KEY REFERENCES keys(id) ON DELETE CASCADE,
    value BLOB NOT NULL
);

-- Hashes
CREATE TABLE IF NOT EXISTS hashes (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    field TEXT NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, field)
);

-- Lists (integer positions with gap-based insertion)
CREATE TABLE IF NOT EXISTS lists (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    pos INTEGER NOT NULL,
    value BLOB NOT NULL,
    PRIMARY KEY (key_id, pos)
);
CREATE INDEX IF NOT EXISTS idx_lists_key_pos ON lists(key_id, pos);

-- Sets
CREATE TABLE IF NOT EXISTS sets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    PRIMARY KEY (key_id, member)
);

-- Sorted Sets
CREATE TABLE IF NOT EXISTS zsets (
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member BLOB NOT NULL,
    score REAL NOT NULL,
    PRIMARY KEY (key_id, member)
);
CREATE INDEX IF NOT EXISTS idx_zsets_score ON zsets(key_id, score, member);

-- Streams
CREATE TABLE IF NOT EXISTS streams (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    entry_ms INTEGER NOT NULL,
    entry_seq INTEGER NOT NULL,
    data BLOB NOT NULL,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_streams_key_entry ON streams(key_id, entry_ms, entry_seq);

-- Stream consumer groups
CREATE TABLE IF NOT EXISTS stream_groups (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    last_ms INTEGER NOT NULL DEFAULT 0,
    last_seq INTEGER NOT NULL DEFAULT 0,
    UNIQUE(key_id, name)
);

-- Stream pending entries
CREATE TABLE IF NOT EXISTS stream_pending (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    entry_id INTEGER NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    consumer TEXT NOT NULL,
    delivered_at INTEGER NOT NULL,
    delivery_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(group_id, entry_id)
);
CREATE INDEX IF NOT EXISTS idx_stream_pending_consumer ON stream_pending(group_id, consumer);

-- Stream consumers
CREATE TABLE IF NOT EXISTS stream_consumers (
    id INTEGER PRIMARY KEY,
    group_id INTEGER NOT NULL REFERENCES stream_groups(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    seen_time INTEGER NOT NULL DEFAULT 0,
    UNIQUE(group_id, name)
);
`;

const SCHEMA_HISTORY = `
-- History tracking configuration
CREATE TABLE IF NOT EXISTS history_config (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'key')),
    target TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    retention_type TEXT CHECK(retention_type IN ('unlimited', 'time', 'count')),
    retention_value INTEGER,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
    UNIQUE(level, target)
);
CREATE INDEX IF NOT EXISTS idx_history_config_level_target ON history_config(level, target);

-- Versioned key history
CREATE TABLE IF NOT EXISTS key_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    key_type INTEGER NOT NULL,
    version_num INTEGER NOT NULL,
    operation TEXT NOT NULL,
    timestamp_ms INTEGER NOT NULL,
    data_snapshot BLOB,
    expire_at INTEGER,
    UNIQUE(key_id, version_num)
);
CREATE INDEX IF NOT EXISTS idx_history_key_time ON key_history(key_id, timestamp_ms DESC);
CREATE INDEX IF NOT EXISTS idx_history_db_key_time ON key_history(db, key, timestamp_ms DESC);
`;

const SCHEMA_FTS = `
-- Full-text search configuration
CREATE TABLE IF NOT EXISTS fts_settings (
    id INTEGER PRIMARY KEY,
    level TEXT NOT NULL CHECK(level IN ('global', 'database', 'pattern', 'key')),
    target TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
    UNIQUE(level, target)
);
CREATE INDEX IF NOT EXISTS idx_fts_settings_level_target ON fts_settings(level, target);

-- FTS5 virtual table
CREATE VIRTUAL TABLE IF NOT EXISTS fts USING fts5(
    key_text,
    content,
    tokenize='porter unicode61'
);

-- Mapping from FTS rowid to key_id
CREATE TABLE IF NOT EXISTS fts_keys (
    rowid INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    db INTEGER NOT NULL,
    key TEXT NOT NULL,
    UNIQUE(db, key)
);
CREATE INDEX IF NOT EXISTS idx_fts_keys_key_id ON fts_keys(key_id);
`;

const SCHEMA_FT = `
-- RediSearch-compatible index schema
CREATE TABLE IF NOT EXISTS ft_indexes (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    on_type TEXT NOT NULL CHECK(on_type IN ('HASH', 'JSON')),
    prefixes TEXT,
    schema TEXT NOT NULL,
    language TEXT DEFAULT 'english',
    score_field TEXT,
    payload_field TEXT,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000)
);
CREATE INDEX IF NOT EXISTS idx_ft_indexes_name ON ft_indexes(name);

CREATE TABLE IF NOT EXISTS ft_aliases (
    alias TEXT PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_ft_aliases_index_id ON ft_aliases(index_id);

CREATE TABLE IF NOT EXISTS ft_synonyms (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    group_id TEXT NOT NULL,
    term TEXT NOT NULL,
    UNIQUE(index_id, group_id, term)
);
CREATE INDEX IF NOT EXISTS idx_ft_synonyms_index ON ft_synonyms(index_id);
CREATE INDEX IF NOT EXISTS idx_ft_synonyms_group ON ft_synonyms(index_id, group_id);

CREATE TABLE IF NOT EXISTS ft_suggestions (
    id INTEGER PRIMARY KEY,
    key TEXT NOT NULL,
    string TEXT NOT NULL,
    score REAL NOT NULL DEFAULT 1.0,
    payload TEXT,
    UNIQUE(key, string)
);
CREATE INDEX IF NOT EXISTS idx_ft_suggestions_key ON ft_suggestions(key);
CREATE INDEX IF NOT EXISTS idx_ft_suggestions_key_score ON ft_suggestions(key, score DESC);

CREATE TABLE IF NOT EXISTS ft_indexed_docs (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    score REAL DEFAULT 1.0,
    payload TEXT,
    UNIQUE(index_id, key_id)
);
CREATE INDEX IF NOT EXISTS idx_ft_indexed_docs_index ON ft_indexed_docs(index_id);
CREATE INDEX IF NOT EXISTS idx_ft_indexed_docs_key ON ft_indexed_docs(key_id);

CREATE TABLE IF NOT EXISTS ft_numeric_fields (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    doc_id INTEGER NOT NULL REFERENCES ft_indexed_docs(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    value REAL NOT NULL,
    UNIQUE(index_id, doc_id, field_name)
);
CREATE INDEX IF NOT EXISTS idx_ft_numeric_fields_index ON ft_numeric_fields(index_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ft_numeric_fields_value ON ft_numeric_fields(index_id, field_name, value);

CREATE TABLE IF NOT EXISTS ft_tag_fields (
    id INTEGER PRIMARY KEY,
    index_id INTEGER NOT NULL REFERENCES ft_indexes(id) ON DELETE CASCADE,
    doc_id INTEGER NOT NULL REFERENCES ft_indexed_docs(id) ON DELETE CASCADE,
    field_name TEXT NOT NULL,
    tag TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ft_tag_fields_index ON ft_tag_fields(index_id, field_name);
CREATE INDEX IF NOT EXISTS idx_ft_tag_fields_tag ON ft_tag_fields(index_id, field_name, tag);
`;

const SCHEMA_VECTORS = `
-- Vector sets storage (Redis 8 compatible)
CREATE TABLE IF NOT EXISTS vector_sets (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    element TEXT NOT NULL,
    embedding BLOB NOT NULL,
    dimensions INTEGER NOT NULL,
    quantization TEXT DEFAULT 'NOQUANT',
    attributes TEXT,
    created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now') * 1000),
    UNIQUE(key_id, element)
);
CREATE INDEX IF NOT EXISTS idx_vector_sets_key_id ON vector_sets(key_id);
CREATE INDEX IF NOT EXISTS idx_vector_sets_element ON vector_sets(key_id, element);
`;

const SCHEMA_GEO = `
-- Geo data storage
CREATE TABLE IF NOT EXISTS geo_data (
    id INTEGER PRIMARY KEY,
    key_id INTEGER NOT NULL REFERENCES keys(id) ON DELETE CASCADE,
    member TEXT NOT NULL,
    longitude REAL NOT NULL,
    latitude REAL NOT NULL,
    geohash TEXT,
    UNIQUE(key_id, member)
);
CREATE INDEX IF NOT EXISTS idx_geo_data_key ON geo_data(key_id);
`;

/** Load sql.js (WASM SQLite) */
let sqlPromise: Promise<SqlJsStatic> | null = null;

async function loadSqlJs(): Promise<SqlJsStatic> {
  if (sqlPromise) return sqlPromise;

  sqlPromise = (async () => {
    // Dynamic import for sql.js
    const initSqlJs = (await import('sql.js')).default;

    // Load with WASM from CDN (or bundled)
    return initSqlJs({
      locateFile: (file: string) =>
        `https://sql.js.org/dist/${file}`,
    });
  })();

  return sqlPromise;
}

/** Internal database wrapper */
export class DbCore {
  private db: Database;
  private selectedDb: number = 0;
  private autovacuumEnabled: boolean = true;
  private autovacuumIntervalMs: number = 60_000;
  private lastCleanup: number = 0;

  private constructor(db: Database) {
    this.db = db;
  }

  /** Open or create a database from file data */
  static async open(data?: ArrayLike<number>): Promise<DbCore> {
    const SQL = await loadSqlJs();
    const db = data ? new SQL.Database(data) : new SQL.Database();
    const core = new DbCore(db);
    core.migrate();
    return core;
  }

  /** Run schema migrations */
  private migrate(): void {
    this.db.run('PRAGMA foreign_keys = ON');
    this.db.run(SCHEMA_CORE);
    this.db.run(SCHEMA_HISTORY);
    this.db.run(SCHEMA_FTS);
    this.db.run(SCHEMA_FT);
    this.db.run(SCHEMA_VECTORS);
    this.db.run(SCHEMA_GEO);
  }

  /** Export database as binary data (for persistence) */
  export(): Uint8Array {
    return this.db.export();
  }

  /** Close the database */
  close(): void {
    this.db.close();
  }

  /** Get current timestamp in milliseconds */
  nowMs(): number {
    return Date.now();
  }

  /** Select database (0-15) */
  select(db: number): void {
    if (db < 0 || db > 15) {
      throw new Error('ERR invalid DB index');
    }
    this.selectedDb = db;
  }

  /** Get current database number */
  currentDb(): number {
    return this.selectedDb;
  }

  /** Enable/disable autovacuum */
  setAutovacuum(enabled: boolean): void {
    this.autovacuumEnabled = enabled;
  }

  /** Check if autovacuum is enabled */
  isAutovacuumEnabled(): boolean {
    return this.autovacuumEnabled;
  }

  /** Maybe run autovacuum if interval has passed */
  maybeAutovacuum(): void {
    if (!this.autovacuumEnabled) return;

    const now = this.nowMs();
    if (now - this.lastCleanup < this.autovacuumIntervalMs) return;

    this.lastCleanup = now;
    this.db.run(
      'DELETE FROM keys WHERE expire_at IS NOT NULL AND expire_at <= ?',
      [now]
    );
  }

  /** Run a SQL query and return results */
  query<T = unknown>(sql: string, params: unknown[] = []): T[] {
    const stmt = this.db.prepare(sql);
    stmt.bind(params);

    const results: T[] = [];
    while (stmt.step()) {
      results.push(stmt.getAsObject() as T);
    }
    stmt.free();
    return results;
  }

  /** Run a SQL statement (no return value) */
  run(sql: string, params: unknown[] = []): void {
    this.db.run(sql, params);
  }

  /** Run SQL and get single value */
  queryOne<T = unknown>(sql: string, params: unknown[] = []): T | null {
    const results = this.query<T>(sql, params);
    return results.length > 0 ? results[0] : null;
  }

  /** Get or create a key, returning key_id */
  getOrCreateKey(key: string, type: KeyType): number {
    const now = this.nowMs();

    // Check if key exists
    const existing = this.queryOne<{ id: number; type: number }>(
      'SELECT id, type FROM keys WHERE db = ? AND key = ?',
      [this.selectedDb, key]
    );

    if (existing) {
      if (existing.type !== type) {
        throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
      }
      return existing.id;
    }

    // Create new key
    this.run(
      'INSERT INTO keys (db, key, type, created_at, updated_at) VALUES (?, ?, ?, ?, ?)',
      [this.selectedDb, key, type, now, now]
    );

    const result = this.queryOne<{ id: number }>(
      'SELECT last_insert_rowid() as id'
    );
    return result!.id;
  }

  /** Get key_id if exists, null otherwise */
  getKeyId(key: string, expectedType?: KeyType): number | null {
    // Clean expired keys first
    this.maybeAutovacuum();

    const row = this.queryOne<{ id: number; type: number; expire_at: number | null }>(
      'SELECT id, type, expire_at FROM keys WHERE db = ? AND key = ?',
      [this.selectedDb, key]
    );

    if (!row) return null;

    // Check expiration
    if (row.expire_at !== null && row.expire_at <= this.nowMs()) {
      this.deleteKey(key);
      return null;
    }

    // Check type if specified
    if (expectedType !== undefined && row.type !== expectedType) {
      throw new Error('WRONGTYPE Operation against a key holding the wrong kind of value');
    }

    return row.id;
  }

  /** Delete a key and all associated data */
  deleteKey(key: string): boolean {
    const keyId = this.queryOne<{ id: number }>(
      'SELECT id FROM keys WHERE db = ? AND key = ?',
      [this.selectedDb, key]
    );

    if (!keyId) return false;

    // CASCADE will handle related tables
    this.run('DELETE FROM keys WHERE id = ?', [keyId.id]);
    return true;
  }

  /** Update key timestamp */
  touchKey(keyId: number): void {
    const now = this.nowMs();
    this.run('UPDATE keys SET updated_at = ? WHERE id = ?', [now, keyId]);
  }

  /** Increment key version */
  incrementVersion(keyId: number): number {
    this.run(
      'UPDATE keys SET version = version + 1, updated_at = ? WHERE id = ?',
      [this.nowMs(), keyId]
    );
    const result = this.queryOne<{ version: number }>(
      'SELECT version FROM keys WHERE id = ?',
      [keyId]
    );
    return result?.version ?? 0;
  }

  /** Set key expiration */
  setExpire(key: string, expireAt: number | null): boolean {
    const result = this.run(
      'UPDATE keys SET expire_at = ? WHERE db = ? AND key = ?',
      [expireAt, this.selectedDb, key]
    );
    const changes = this.queryOne<{ changes: number }>(
      'SELECT changes() as changes'
    );
    return (changes?.changes ?? 0) > 0;
  }

  /** Get key TTL in milliseconds (-2 if not exists, -1 if no expire) */
  getTTL(key: string): number {
    const row = this.queryOne<{ expire_at: number | null }>(
      'SELECT expire_at FROM keys WHERE db = ? AND key = ?',
      [this.selectedDb, key]
    );

    if (!row) return -2;
    if (row.expire_at === null) return -1;

    const ttl = row.expire_at - this.nowMs();
    return ttl > 0 ? ttl : -2; // Expired = not exists
  }

  /** Get key type */
  getType(key: string): string | null {
    const row = this.queryOne<{ type: number; expire_at: number | null }>(
      'SELECT type, expire_at FROM keys WHERE db = ? AND key = ?',
      [this.selectedDb, key]
    );

    if (!row) return null;
    if (row.expire_at !== null && row.expire_at <= this.nowMs()) {
      this.deleteKey(key);
      return null;
    }

    const types = ['string', 'list', 'set', 'zset', 'hash', 'stream', 'geo', 'vector'];
    return types[row.type] ?? 'unknown';
  }

  /** Check if key exists */
  exists(key: string): boolean {
    return this.getKeyId(key) !== null;
  }

  /** Get all keys matching pattern */
  keys(pattern: string): string[] {
    this.maybeAutovacuum();
    const now = this.nowMs();

    // Convert glob pattern to SQL LIKE pattern
    const sqlPattern = pattern
      .replace(/\*/g, '%')
      .replace(/\?/g, '_')
      .replace(/\[([^\]]+)\]/g, '[$1]');

    const rows = this.query<{ key: string }>(
      `SELECT key FROM keys
       WHERE db = ? AND key LIKE ?
       AND (expire_at IS NULL OR expire_at > ?)`,
      [this.selectedDb, sqlPattern, now]
    );

    return rows.map((r) => r.key);
  }

  /** Flush current database */
  flushDb(): void {
    this.run('DELETE FROM keys WHERE db = ?', [this.selectedDb]);
  }

  /** Get database size */
  dbSize(): number {
    this.maybeAutovacuum();
    const result = this.queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM keys
       WHERE db = ? AND (expire_at IS NULL OR expire_at > ?)`,
      [this.selectedDb, this.nowMs()]
    );
    return result?.count ?? 0;
  }
}
