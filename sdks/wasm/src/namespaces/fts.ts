/**
 * Full-text search namespace for Redlite
 */

import type { DbCore } from '../db';
import { FtsResult, FtsStats } from '../types';

export class FtsNamespace {
  constructor(private db: DbCore) {}

  /** Enable FTS globally, for a database, pattern, or specific key */
  enable(options?: {
    global?: boolean;
    database?: number;
    pattern?: string;
    key?: string;
  }): boolean {
    let level: string;
    let target: string;

    if (options?.global) {
      level = 'global';
      target = '*';
    } else if (options?.database !== undefined) {
      level = 'database';
      target = options.database.toString();
    } else if (options?.pattern) {
      level = 'pattern';
      target = options.pattern;
    } else if (options?.key) {
      level = 'key';
      target = `${this.db.currentDb()}:${options.key}`;
    } else {
      level = 'global';
      target = '*';
    }

    this.db.run(
      `INSERT INTO fts_settings (level, target, enabled)
       VALUES (?, ?, 1)
       ON CONFLICT(level, target) DO UPDATE SET enabled = 1`,
      [level, target]
    );

    return true;
  }

  /** Disable FTS for a scope */
  disable(options?: {
    global?: boolean;
    database?: number;
    pattern?: string;
    key?: string;
  }): boolean {
    let level: string;
    let target: string;

    if (options?.global) {
      level = 'global';
      target = '*';
    } else if (options?.database !== undefined) {
      level = 'database';
      target = options.database.toString();
    } else if (options?.pattern) {
      level = 'pattern';
      target = options.pattern;
    } else if (options?.key) {
      level = 'key';
      target = `${this.db.currentDb()}:${options.key}`;
    } else {
      level = 'global';
      target = '*';
    }

    this.db.run(
      `UPDATE fts_settings SET enabled = 0 WHERE level = ? AND target = ?`,
      [level, target]
    );

    return true;
  }

  /** Check if FTS is enabled for a key */
  isEnabled(key: string): boolean {
    const dbNum = this.db.currentDb();
    const target = `${dbNum}:${key}`;

    // Check key-level
    const keyLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM fts_settings WHERE level = 'key' AND target = ?`,
      [target]
    );
    if (keyLevel) return keyLevel.enabled === 1;

    // Check pattern-level
    const patterns = this.db.query<{ target: string; enabled: number }>(
      `SELECT target, enabled FROM fts_settings WHERE level = 'pattern'`
    );
    for (const p of patterns) {
      const regex = new RegExp(
        '^' + p.target.replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
      );
      if (regex.test(key)) return p.enabled === 1;
    }

    // Check database-level
    const dbLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM fts_settings WHERE level = 'database' AND target = ?`,
      [dbNum.toString()]
    );
    if (dbLevel) return dbLevel.enabled === 1;

    // Check global
    const globalLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM fts_settings WHERE level = 'global' AND target = '*'`
    );
    return globalLevel?.enabled === 1;
  }

  /** Index a key's content for search */
  index(key: string, content: string): boolean {
    if (!this.isEnabled(key)) return false;

    const keyId = this.db.queryOne<{ id: number }>(
      'SELECT id FROM keys WHERE db = ? AND key = ?',
      [this.db.currentDb(), key]
    );

    if (!keyId) return false;

    // Check if already indexed
    const existing = this.db.queryOne<{ rowid: number }>(
      'SELECT rowid FROM fts_keys WHERE key_id = ?',
      [keyId.id]
    );

    if (existing) {
      // Update existing
      this.db.run(
        'UPDATE fts SET content = ? WHERE rowid = ?',
        [content, existing.rowid]
      );
    } else {
      // Insert new
      this.db.run(
        'INSERT INTO fts (key_text, content) VALUES (?, ?)',
        [key, content]
      );

      const rowid = this.db.queryOne<{ id: number }>(
        'SELECT last_insert_rowid() as id'
      );

      this.db.run(
        'INSERT INTO fts_keys (rowid, key_id, db, key) VALUES (?, ?, ?, ?)',
        [rowid!.id, keyId.id, this.db.currentDb(), key]
      );
    }

    return true;
  }

  /** Remove a key from the FTS index */
  unindex(key: string): boolean {
    const ftsKey = this.db.queryOne<{ rowid: number }>(
      'SELECT rowid FROM fts_keys WHERE db = ? AND key = ?',
      [this.db.currentDb(), key]
    );

    if (!ftsKey) return false;

    this.db.run('DELETE FROM fts WHERE rowid = ?', [ftsKey.rowid]);
    this.db.run('DELETE FROM fts_keys WHERE rowid = ?', [ftsKey.rowid]);

    return true;
  }

  /** Search indexed content */
  search(
    query: string,
    options?: {
      limit?: number;
      highlight?: boolean;
      prefix?: string;
    }
  ): FtsResult[] {
    const limit = options?.limit ?? 10;
    const prefix = options?.prefix;

    let sql = `
      SELECT fts_keys.key, bm25(fts) as rank
      ${options?.highlight ? ", snippet(fts, 1, '<b>', '</b>', '...', 32) as snippet" : ''}
      FROM fts
      JOIN fts_keys ON fts.rowid = fts_keys.rowid
      WHERE fts_keys.db = ? AND fts MATCH ?
    `;

    const params: unknown[] = [this.db.currentDb(), query];

    if (prefix) {
      sql += ` AND fts_keys.key LIKE ?`;
      params.push(prefix + '%');
    }

    sql += ` ORDER BY rank LIMIT ?`;
    params.push(limit);

    const rows = this.db.query<{ key: string; rank: number; snippet?: string }>(sql, params);

    return rows.map((r) => ({
      key: r.key,
      rank: Math.abs(r.rank), // BM25 returns negative scores
      snippet: r.snippet,
    }));
  }

  /** Get FTS statistics */
  stats(): FtsStats {
    const docs = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM fts_keys WHERE db = ?',
      [this.db.currentDb()]
    );

    const settings = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM fts_settings'
    );

    return {
      documentsIndexed: docs?.count ?? 0,
      settingsCount: settings?.count ?? 0,
    };
  }
}
