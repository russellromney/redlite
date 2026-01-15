/**
 * List commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyType } from '../types';

const GAP = 1000000; // Gap between positions for insertion

export class ListCommands {
  constructor(private db: DbCore) {}

  /** LPUSH - Push values to head of list */
  lpush(key: string, ...values: string[]): number {
    const keyId = this.db.getOrCreateKey(key, KeyType.List);

    // Get minimum position
    const minPos = this.db.queryOne<{ pos: number }>(
      'SELECT MIN(pos) as pos FROM lists WHERE key_id = ?',
      [keyId]
    );

    let pos = (minPos?.pos ?? GAP) - GAP;

    for (const value of values.reverse()) {
      const valueBytes = new TextEncoder().encode(value);
      this.db.run(
        'INSERT INTO lists (key_id, pos, value) VALUES (?, ?, ?)',
        [keyId, pos, valueBytes]
      );
      pos -= GAP;
    }

    this.db.touchKey(keyId);
    return this.llen(key);
  }

  /** RPUSH - Push values to tail of list */
  rpush(key: string, ...values: string[]): number {
    const keyId = this.db.getOrCreateKey(key, KeyType.List);

    // Get maximum position
    const maxPos = this.db.queryOne<{ pos: number }>(
      'SELECT MAX(pos) as pos FROM lists WHERE key_id = ?',
      [keyId]
    );

    let pos = (maxPos?.pos ?? 0) + GAP;

    for (const value of values) {
      const valueBytes = new TextEncoder().encode(value);
      this.db.run(
        'INSERT INTO lists (key_id, pos, value) VALUES (?, ?, ?)',
        [keyId, pos, valueBytes]
      );
      pos += GAP;
    }

    this.db.touchKey(keyId);
    return this.llen(key);
  }

  /** LPOP - Pop value(s) from head of list */
  lpop(key: string, count?: number): string | string[] | null {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return count ? [] : null;

    const n = count ?? 1;
    const rows = this.db.query<{ pos: number; value: Uint8Array }>(
      'SELECT pos, value FROM lists WHERE key_id = ? ORDER BY pos ASC LIMIT ?',
      [keyId, n]
    );

    if (rows.length === 0) return count ? [] : null;

    // Delete popped elements
    for (const row of rows) {
      this.db.run('DELETE FROM lists WHERE key_id = ? AND pos = ?', [keyId, row.pos]);
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM lists WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    const values = rows.map((r) => new TextDecoder().decode(r.value));
    return count ? values : values[0];
  }

  /** RPOP - Pop value(s) from tail of list */
  rpop(key: string, count?: number): string | string[] | null {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return count ? [] : null;

    const n = count ?? 1;
    const rows = this.db.query<{ pos: number; value: Uint8Array }>(
      'SELECT pos, value FROM lists WHERE key_id = ? ORDER BY pos DESC LIMIT ?',
      [keyId, n]
    );

    if (rows.length === 0) return count ? [] : null;

    // Delete popped elements
    for (const row of rows) {
      this.db.run('DELETE FROM lists WHERE key_id = ? AND pos = ?', [keyId, row.pos]);
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM lists WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    const values = rows.map((r) => new TextDecoder().decode(r.value));
    return count ? values : values[0];
  }

  /** LRANGE - Get range of elements */
  lrange(key: string, start: number, stop: number): string[] {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return [];

    const len = this.llen(key);
    if (len === 0) return [];

    // Handle negative indices
    if (start < 0) start = Math.max(0, len + start);
    if (stop < 0) stop = len + stop;
    if (start > stop || start >= len) return [];
    stop = Math.min(stop, len - 1);

    const rows = this.db.query<{ value: Uint8Array }>(
      `SELECT value FROM lists WHERE key_id = ?
       ORDER BY pos ASC LIMIT ? OFFSET ?`,
      [keyId, stop - start + 1, start]
    );

    return rows.map((r) => new TextDecoder().decode(r.value));
  }

  /** LLEN - Get list length */
  llen(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM lists WHERE key_id = ?',
      [keyId]
    );
    return result?.count ?? 0;
  }

  /** LINDEX - Get element at index */
  lindex(key: string, index: number): string | null {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return null;

    const len = this.llen(key);
    if (len === 0) return null;

    // Handle negative index
    if (index < 0) index = len + index;
    if (index < 0 || index >= len) return null;

    const row = this.db.queryOne<{ value: Uint8Array }>(
      `SELECT value FROM lists WHERE key_id = ?
       ORDER BY pos ASC LIMIT 1 OFFSET ?`,
      [keyId, index]
    );

    if (!row) return null;
    return new TextDecoder().decode(row.value);
  }

  /** LINSERT - Insert before or after pivot */
  linsert(key: string, where: 'BEFORE' | 'AFTER', pivot: string, value: string): number {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return 0;

    const pivotBytes = new TextEncoder().encode(pivot);
    const valueBytes = new TextEncoder().encode(value);

    // Find pivot position
    const pivotRow = this.db.queryOne<{ pos: number }>(
      'SELECT pos FROM lists WHERE key_id = ? AND value = ?',
      [keyId, pivotBytes]
    );

    if (!pivotRow) return -1;

    let newPos: number;

    if (where.toUpperCase() === 'BEFORE') {
      // Get previous position
      const prev = this.db.queryOne<{ pos: number }>(
        'SELECT pos FROM lists WHERE key_id = ? AND pos < ? ORDER BY pos DESC LIMIT 1',
        [keyId, pivotRow.pos]
      );
      newPos = prev ? Math.floor((prev.pos + pivotRow.pos) / 2) : pivotRow.pos - GAP;
    } else {
      // Get next position
      const next = this.db.queryOne<{ pos: number }>(
        'SELECT pos FROM lists WHERE key_id = ? AND pos > ? ORDER BY pos ASC LIMIT 1',
        [keyId, pivotRow.pos]
      );
      newPos = next ? Math.floor((pivotRow.pos + next.pos) / 2) : pivotRow.pos + GAP;
    }

    this.db.run(
      'INSERT INTO lists (key_id, pos, value) VALUES (?, ?, ?)',
      [keyId, newPos, valueBytes]
    );

    return this.llen(key);
  }

  /** LSET - Set element at index */
  lset(key: string, index: number, value: string): string {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) {
      throw new Error('ERR no such key');
    }

    const len = this.llen(key);
    if (index < 0) index = len + index;
    if (index < 0 || index >= len) {
      throw new Error('ERR index out of range');
    }

    const row = this.db.queryOne<{ pos: number }>(
      `SELECT pos FROM lists WHERE key_id = ?
       ORDER BY pos ASC LIMIT 1 OFFSET ?`,
      [keyId, index]
    );

    if (!row) {
      throw new Error('ERR index out of range');
    }

    const valueBytes = new TextEncoder().encode(value);
    this.db.run(
      'UPDATE lists SET value = ? WHERE key_id = ? AND pos = ?',
      [valueBytes, keyId, row.pos]
    );

    return 'OK';
  }

  /** LREM - Remove elements matching value */
  lrem(key: string, count: number, value: string): number {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return 0;

    const valueBytes = new TextEncoder().encode(value);
    let removed = 0;

    if (count === 0) {
      // Remove all
      this.db.run(
        'DELETE FROM lists WHERE key_id = ? AND value = ?',
        [keyId, valueBytes]
      );
      const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
      removed = changes?.changes ?? 0;
    } else {
      const order = count > 0 ? 'ASC' : 'DESC';
      const limit = Math.abs(count);

      const rows = this.db.query<{ pos: number }>(
        `SELECT pos FROM lists WHERE key_id = ? AND value = ?
         ORDER BY pos ${order} LIMIT ?`,
        [keyId, valueBytes, limit]
      );

      for (const row of rows) {
        this.db.run('DELETE FROM lists WHERE key_id = ? AND pos = ?', [keyId, row.pos]);
        removed++;
      }
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM lists WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return removed;
  }

  /** LTRIM - Trim list to specified range */
  ltrim(key: string, start: number, stop: number): string {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return 'OK';

    const len = this.llen(key);
    if (len === 0) return 'OK';

    // Handle negative indices
    if (start < 0) start = Math.max(0, len + start);
    if (stop < 0) stop = len + stop;

    if (start > stop || start >= len) {
      // Delete all
      this.db.deleteKey(key);
      return 'OK';
    }

    stop = Math.min(stop, len - 1);

    // Get positions to keep
    const keep = this.db.query<{ pos: number }>(
      `SELECT pos FROM lists WHERE key_id = ?
       ORDER BY pos ASC LIMIT ? OFFSET ?`,
      [keyId, stop - start + 1, start]
    );

    if (keep.length === 0) {
      this.db.deleteKey(key);
      return 'OK';
    }

    const keepPositions = keep.map((r) => r.pos);
    const minKeep = Math.min(...keepPositions);
    const maxKeep = Math.max(...keepPositions);

    // Delete elements outside range
    this.db.run(
      'DELETE FROM lists WHERE key_id = ? AND (pos < ? OR pos > ?)',
      [keyId, minKeep, maxKeep]
    );

    return 'OK';
  }

  /** LPUSHX - Push to head only if list exists */
  lpushx(key: string, ...values: string[]): number {
    if (!this.db.exists(key)) return 0;
    return this.lpush(key, ...values);
  }

  /** RPUSHX - Push to tail only if list exists */
  rpushx(key: string, ...values: string[]): number {
    if (!this.db.exists(key)) return 0;
    return this.rpush(key, ...values);
  }

  /** LPOS - Get index of element */
  lpos(
    key: string,
    value: string,
    options?: { rank?: number; count?: number; maxlen?: number }
  ): number | number[] | null {
    const keyId = this.db.getKeyId(key, KeyType.List);
    if (keyId === null) return options?.count ? [] : null;

    const valueBytes = new TextEncoder().encode(value);
    const rank = options?.rank ?? 1;
    const count = options?.count;
    const maxlen = options?.maxlen ?? 0;

    let query: string;
    let params: unknown[];

    if (rank > 0) {
      query = 'SELECT pos FROM lists WHERE key_id = ? ORDER BY pos ASC';
      params = [keyId];
    } else {
      query = 'SELECT pos FROM lists WHERE key_id = ? ORDER BY pos DESC';
      params = [keyId];
    }

    if (maxlen > 0) {
      query += ` LIMIT ${maxlen}`;
    }

    const rows = this.db.query<{ pos: number }>(query, params);
    const indices: number[] = [];
    let matchCount = 0;
    let skipCount = Math.abs(rank) - 1;

    for (let i = 0; i < rows.length; i++) {
      const row = this.db.queryOne<{ value: Uint8Array }>(
        'SELECT value FROM lists WHERE key_id = ? AND pos = ?',
        [keyId, rows[i].pos]
      );

      if (row && this.bytesEqual(row.value, valueBytes)) {
        if (skipCount > 0) {
          skipCount--;
          continue;
        }

        matchCount++;
        indices.push(rank > 0 ? i : rows.length - 1 - i);

        if (count && matchCount >= count) break;
        if (!count) break;
      }
    }

    if (count) return indices;
    return indices.length > 0 ? indices[0] : null;
  }

  /** LMOVE - Move element between lists */
  lmove(src: string, dst: string, srcDir: 'LEFT' | 'RIGHT', dstDir: 'LEFT' | 'RIGHT'): string | null {
    const value = srcDir === 'LEFT' ? this.lpop(src) : this.rpop(src);
    if (value === null || Array.isArray(value)) return null;

    if (dstDir === 'LEFT') {
      this.lpush(dst, value);
    } else {
      this.rpush(dst, value);
    }

    return value;
  }

  private bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    if (a.length !== b.length) return false;
    for (let i = 0; i < a.length; i++) {
      if (a[i] !== b[i]) return false;
    }
    return true;
  }
}
