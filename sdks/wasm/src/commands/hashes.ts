/**
 * Hash commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyType, ScanResult } from '../types';

export class HashCommands {
  constructor(private db: DbCore) {}

  /** HSET - Set hash field(s) */
  hset(key: string, fieldOrMapping: string | Record<string, string>, value?: string): number {
    const keyId = this.db.getOrCreateKey(key, KeyType.Hash);
    let fieldsSet = 0;

    const mapping: Record<string, string> =
      typeof fieldOrMapping === 'string'
        ? { [fieldOrMapping]: value! }
        : fieldOrMapping;

    for (const [field, val] of Object.entries(mapping)) {
      const valueBytes = new TextEncoder().encode(val);
      const existing = this.db.queryOne<{ field: string }>(
        'SELECT field FROM hashes WHERE key_id = ? AND field = ?',
        [keyId, field]
      );

      if (existing) {
        this.db.run(
          'UPDATE hashes SET value = ? WHERE key_id = ? AND field = ?',
          [valueBytes, keyId, field]
        );
      } else {
        this.db.run(
          'INSERT INTO hashes (key_id, field, value) VALUES (?, ?, ?)',
          [keyId, field, valueBytes]
        );
        fieldsSet++;
      }
    }

    this.db.touchKey(keyId);
    return fieldsSet;
  }

  /** HGET - Get hash field value */
  hget(key: string, field: string): string | null {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return null;

    const row = this.db.queryOne<{ value: Uint8Array }>(
      'SELECT value FROM hashes WHERE key_id = ? AND field = ?',
      [keyId, field]
    );

    if (!row) return null;
    return new TextDecoder().decode(row.value);
  }

  /** HGETALL - Get all fields and values */
  hgetall(key: string): Record<string, string> {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return {};

    const rows = this.db.query<{ field: string; value: Uint8Array }>(
      'SELECT field, value FROM hashes WHERE key_id = ?',
      [keyId]
    );

    const result: Record<string, string> = {};
    for (const row of rows) {
      result[row.field] = new TextDecoder().decode(row.value);
    }
    return result;
  }

  /** HDEL - Delete hash field(s) */
  hdel(key: string, ...fields: string[]): number {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return 0;

    let deleted = 0;
    for (const field of fields) {
      this.db.run(
        'DELETE FROM hashes WHERE key_id = ? AND field = ?',
        [keyId, field]
      );
      const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
      deleted += changes?.changes ?? 0;
    }

    // Delete key if hash is empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM hashes WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return deleted;
  }

  /** HEXISTS - Check if hash field exists */
  hexists(key: string, field: string): boolean {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return false;

    const row = this.db.queryOne<{ field: string }>(
      'SELECT field FROM hashes WHERE key_id = ? AND field = ?',
      [keyId, field]
    );
    return row !== null;
  }

  /** HLEN - Get number of fields in hash */
  hlen(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM hashes WHERE key_id = ?',
      [keyId]
    );
    return result?.count ?? 0;
  }

  /** HKEYS - Get all field names */
  hkeys(key: string): string[] {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return [];

    const rows = this.db.query<{ field: string }>(
      'SELECT field FROM hashes WHERE key_id = ?',
      [keyId]
    );
    return rows.map((r) => r.field);
  }

  /** HVALS - Get all values */
  hvals(key: string): string[] {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) return [];

    const rows = this.db.query<{ value: Uint8Array }>(
      'SELECT value FROM hashes WHERE key_id = ?',
      [keyId]
    );
    return rows.map((r) => new TextDecoder().decode(r.value));
  }

  /** HMGET - Get values of multiple fields */
  hmget(key: string, ...fields: string[]): (string | null)[] {
    return fields.map((field) => this.hget(key, field));
  }

  /** HSETNX - Set field only if it doesn't exist */
  hsetnx(key: string, field: string, value: string): boolean {
    if (this.hexists(key, field)) return false;
    this.hset(key, field, value);
    return true;
  }

  /** HINCRBY - Increment hash field integer value */
  hincrby(key: string, field: string, amount: number): number {
    const current = this.hget(key, field);
    let value = 0;

    if (current !== null) {
      value = parseInt(current, 10);
      if (isNaN(value)) {
        throw new Error('ERR hash value is not an integer');
      }
    }

    value += amount;
    this.hset(key, field, value.toString());
    return value;
  }

  /** HINCRBYFLOAT - Increment hash field float value */
  hincrbyfloat(key: string, field: string, amount: number): number {
    const current = this.hget(key, field);
    let value = 0;

    if (current !== null) {
      value = parseFloat(current);
      if (isNaN(value)) {
        throw new Error('ERR hash value is not a valid float');
      }
    }

    value += amount;
    this.hset(key, field, value.toString());
    return value;
  }

  /** HSCAN - Incrementally iterate hash fields */
  hscan(
    key: string,
    cursor: number,
    options?: { match?: string; count?: number }
  ): ScanResult<[string, string]> {
    const keyId = this.db.getKeyId(key, KeyType.Hash);
    if (keyId === null) {
      return { cursor: 0, items: [] };
    }

    const count = options?.count ?? 10;
    const pattern = options?.match ?? '*';

    // Convert glob to SQL LIKE
    const sqlPattern = pattern.replace(/\*/g, '%').replace(/\?/g, '_');

    const rows = this.db.query<{ field: string; value: Uint8Array }>(
      `SELECT field, value FROM hashes
       WHERE key_id = ? AND field LIKE ?
       ORDER BY field
       LIMIT ? OFFSET ?`,
      [keyId, sqlPattern, count, cursor]
    );

    const items: [string, string][] = rows.map((r) => [
      r.field,
      new TextDecoder().decode(r.value),
    ]);

    // Check if there are more
    const nextCursor = cursor + rows.length;
    const hasMore = this.db.queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM hashes
       WHERE key_id = ? AND field LIKE ?`,
      [keyId, sqlPattern]
    );

    return {
      cursor: (hasMore?.count ?? 0) > nextCursor ? nextCursor : 0,
      items,
    };
  }
}
