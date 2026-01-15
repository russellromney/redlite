/**
 * History namespace for Redlite (time-travel queries)
 */

import type { DbCore } from '../db';
import { HistoryEntry, HistoryConfig, Retention, HistoryStats } from '../types';

export class HistoryNamespace {
  constructor(private db: DbCore) {}

  /** Enable history tracking */
  enable(options?: {
    global?: boolean;
    database?: number;
    key?: string;
    retention?: Retention;
  }): boolean {
    let level: string;
    let target: string;

    if (options?.global) {
      level = 'global';
      target = '*';
    } else if (options?.database !== undefined) {
      level = 'database';
      target = options.database.toString();
    } else if (options?.key) {
      level = 'key';
      target = `${this.db.currentDb()}:${options.key}`;
    } else {
      level = 'global';
      target = '*';
    }

    const retention = options?.retention ?? { type: 'unlimited' };

    this.db.run(
      `INSERT INTO history_config (level, target, enabled, retention_type, retention_value)
       VALUES (?, ?, 1, ?, ?)
       ON CONFLICT(level, target) DO UPDATE SET
         enabled = 1,
         retention_type = excluded.retention_type,
         retention_value = excluded.retention_value`,
      [level, target, retention.type, retention.value ?? null]
    );

    return true;
  }

  /** Disable history tracking */
  disable(options?: {
    global?: boolean;
    database?: number;
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
    } else if (options?.key) {
      level = 'key';
      target = `${this.db.currentDb()}:${options.key}`;
    } else {
      level = 'global';
      target = '*';
    }

    this.db.run(
      `UPDATE history_config SET enabled = 0 WHERE level = ? AND target = ?`,
      [level, target]
    );

    return true;
  }

  /** Check if history is enabled for a key */
  isEnabled(key: string): boolean {
    const dbNum = this.db.currentDb();
    const target = `${dbNum}:${key}`;

    // Check key-level
    const keyLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM history_config WHERE level = 'key' AND target = ?`,
      [target]
    );
    if (keyLevel) return keyLevel.enabled === 1;

    // Check database-level
    const dbLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM history_config WHERE level = 'database' AND target = ?`,
      [dbNum.toString()]
    );
    if (dbLevel) return dbLevel.enabled === 1;

    // Check global
    const globalLevel = this.db.queryOne<{ enabled: number }>(
      `SELECT enabled FROM history_config WHERE level = 'global' AND target = '*'`
    );
    return globalLevel?.enabled === 1;
  }

  /** Get history entries for a key */
  get(
    key: string,
    options?: {
      version?: number;
      from?: number;
      to?: number;
      limit?: number;
    }
  ): HistoryEntry[] {
    let sql = `
      SELECT version_num, operation, timestamp_ms, data_snapshot, expire_at
      FROM key_history
      WHERE db = ? AND key = ?
    `;
    const params: unknown[] = [this.db.currentDb(), key];

    if (options?.version !== undefined) {
      sql += ` AND version_num = ?`;
      params.push(options.version);
    }

    if (options?.from !== undefined) {
      sql += ` AND timestamp_ms >= ?`;
      params.push(options.from);
    }

    if (options?.to !== undefined) {
      sql += ` AND timestamp_ms <= ?`;
      params.push(options.to);
    }

    sql += ` ORDER BY version_num DESC`;

    if (options?.limit !== undefined) {
      sql += ` LIMIT ?`;
      params.push(options.limit);
    }

    const rows = this.db.query<{
      version_num: number;
      operation: string;
      timestamp_ms: number;
      data_snapshot: Uint8Array | null;
      expire_at: number | null;
    }>(sql, params);

    return rows.map((r) => ({
      version: r.version_num,
      operation: r.operation,
      timestamp: r.timestamp_ms,
      data: r.data_snapshot ? JSON.parse(new TextDecoder().decode(r.data_snapshot)) : null,
      expireAt: r.expire_at ?? undefined,
    }));
  }

  /** List history versions for a key */
  list(key: string, limit?: number): HistoryEntry[] {
    return this.get(key, { limit: limit ?? 100 });
  }

  /** Get specific version of a key */
  version(key: string, version: number): HistoryEntry | null {
    const entries = this.get(key, { version });
    return entries.length > 0 ? entries[0] : null;
  }

  /** Record a history entry (internal use) */
  record(
    keyId: number,
    key: string,
    keyType: number,
    operation: string,
    data: unknown
  ): void {
    if (!this.isEnabled(key)) return;

    // Get next version number
    const lastVersion = this.db.queryOne<{ version_num: number }>(
      `SELECT MAX(version_num) as version_num FROM key_history WHERE key_id = ?`,
      [keyId]
    );
    const nextVersion = (lastVersion?.version_num ?? 0) + 1;

    // Serialize data
    const dataSnapshot = data ? new TextEncoder().encode(JSON.stringify(data)) : null;

    // Get current TTL
    const keyRow = this.db.queryOne<{ expire_at: number | null }>(
      'SELECT expire_at FROM keys WHERE id = ?',
      [keyId]
    );

    this.db.run(
      `INSERT INTO key_history
       (key_id, db, key, key_type, version_num, operation, timestamp_ms, data_snapshot, expire_at)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        keyId,
        this.db.currentDb(),
        key,
        keyType,
        nextVersion,
        operation,
        this.db.nowMs(),
        dataSnapshot,
        keyRow?.expire_at ?? null,
      ]
    );

    // Apply retention policy
    this.applyRetention(keyId);
  }

  /** Apply retention policy to a key's history */
  private applyRetention(keyId: number): void {
    const key = this.db.queryOne<{ key: string }>(
      'SELECT key FROM keys WHERE id = ?',
      [keyId]
    );
    if (!key) return;

    // Get applicable config
    const config = this.getConfig(key.key);
    if (!config || config.retentionType === 'unlimited') return;

    if (config.retentionType === 'count' && config.retentionValue) {
      // Keep only last N versions
      this.db.run(
        `DELETE FROM key_history WHERE key_id = ? AND version_num NOT IN (
           SELECT version_num FROM key_history WHERE key_id = ?
           ORDER BY version_num DESC LIMIT ?
         )`,
        [keyId, keyId, config.retentionValue]
      );
    } else if (config.retentionType === 'time' && config.retentionValue) {
      // Keep only versions within time window
      const cutoff = this.db.nowMs() - config.retentionValue;
      this.db.run(
        `DELETE FROM key_history WHERE key_id = ? AND timestamp_ms < ?`,
        [keyId, cutoff]
      );
    }
  }

  /** Get history config for a key */
  private getConfig(key: string): HistoryConfig | null {
    const dbNum = this.db.currentDb();
    const target = `${dbNum}:${key}`;

    // Check key-level first
    const keyLevel = this.db.queryOne<{
      level: string;
      target: string;
      enabled: number;
      retention_type: string | null;
      retention_value: number | null;
    }>(
      `SELECT * FROM history_config WHERE level = 'key' AND target = ?`,
      [target]
    );

    if (keyLevel) {
      return {
        level: keyLevel.level as HistoryConfig['level'],
        target: keyLevel.target,
        enabled: keyLevel.enabled === 1,
        retentionType: keyLevel.retention_type as HistoryConfig['retentionType'],
        retentionValue: keyLevel.retention_value ?? undefined,
      };
    }

    // Check database-level
    const dbLevel = this.db.queryOne<{
      level: string;
      target: string;
      enabled: number;
      retention_type: string | null;
      retention_value: number | null;
    }>(
      `SELECT * FROM history_config WHERE level = 'database' AND target = ?`,
      [dbNum.toString()]
    );

    if (dbLevel) {
      return {
        level: dbLevel.level as HistoryConfig['level'],
        target: dbLevel.target,
        enabled: dbLevel.enabled === 1,
        retentionType: dbLevel.retention_type as HistoryConfig['retentionType'],
        retentionValue: dbLevel.retention_value ?? undefined,
      };
    }

    // Check global
    const globalLevel = this.db.queryOne<{
      level: string;
      target: string;
      enabled: number;
      retention_type: string | null;
      retention_value: number | null;
    }>(
      `SELECT * FROM history_config WHERE level = 'global' AND target = '*'`
    );

    if (globalLevel) {
      return {
        level: globalLevel.level as HistoryConfig['level'],
        target: globalLevel.target,
        enabled: globalLevel.enabled === 1,
        retentionType: globalLevel.retention_type as HistoryConfig['retentionType'],
        retentionValue: globalLevel.retention_value ?? undefined,
      };
    }

    return null;
  }

  /** Get history statistics */
  stats(): HistoryStats {
    const entries = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM key_history WHERE db = ?',
      [this.db.currentDb()]
    );

    const configs = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM history_config'
    );

    return {
      totalEntries: entries?.count ?? 0,
      configCount: configs?.count ?? 0,
    };
  }
}

// Extend types
declare module '../types' {
  interface HistoryStats {
    totalEntries: number;
    configCount: number;
  }
}
