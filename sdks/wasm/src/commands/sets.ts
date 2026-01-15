/**
 * Set commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyType, ScanResult } from '../types';

export class SetCommands {
  constructor(private db: DbCore) {}

  /** SADD - Add member(s) to set */
  sadd(key: string, ...members: string[]): number {
    const keyId = this.db.getOrCreateKey(key, KeyType.Set);
    let added = 0;

    for (const member of members) {
      const memberBytes = new TextEncoder().encode(member);
      const existing = this.db.queryOne<{ member: Uint8Array }>(
        'SELECT member FROM sets WHERE key_id = ? AND member = ?',
        [keyId, memberBytes]
      );

      if (!existing) {
        this.db.run(
          'INSERT INTO sets (key_id, member) VALUES (?, ?)',
          [keyId, memberBytes]
        );
        added++;
      }
    }

    this.db.touchKey(keyId);
    return added;
  }

  /** SREM - Remove member(s) from set */
  srem(key: string, ...members: string[]): number {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return 0;

    let removed = 0;
    for (const member of members) {
      const memberBytes = new TextEncoder().encode(member);
      this.db.run(
        'DELETE FROM sets WHERE key_id = ? AND member = ?',
        [keyId, memberBytes]
      );
      const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
      removed += changes?.changes ?? 0;
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM sets WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return removed;
  }

  /** SMEMBERS - Get all members */
  smembers(key: string): string[] {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return [];

    const rows = this.db.query<{ member: Uint8Array }>(
      'SELECT member FROM sets WHERE key_id = ?',
      [keyId]
    );

    return rows.map((r) => new TextDecoder().decode(r.member));
  }

  /** SISMEMBER - Check if member exists */
  sismember(key: string, member: string): boolean {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return false;

    const memberBytes = new TextEncoder().encode(member);
    const row = this.db.queryOne<{ member: Uint8Array }>(
      'SELECT member FROM sets WHERE key_id = ? AND member = ?',
      [keyId, memberBytes]
    );
    return row !== null;
  }

  /** SCARD - Get set cardinality */
  scard(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM sets WHERE key_id = ?',
      [keyId]
    );
    return result?.count ?? 0;
  }

  /** SPOP - Remove and return random member(s) */
  spop(key: string, count?: number): string | string[] | null {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return count ? [] : null;

    const n = count ?? 1;
    const rows = this.db.query<{ member: Uint8Array }>(
      'SELECT member FROM sets WHERE key_id = ? ORDER BY RANDOM() LIMIT ?',
      [keyId, n]
    );

    if (rows.length === 0) return count ? [] : null;

    // Delete selected members
    for (const row of rows) {
      this.db.run(
        'DELETE FROM sets WHERE key_id = ? AND member = ?',
        [keyId, row.member]
      );
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM sets WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    const values = rows.map((r) => new TextDecoder().decode(r.member));
    return count ? values : values[0];
  }

  /** SRANDMEMBER - Get random member(s) without removing */
  srandmember(key: string, count?: number): string | string[] | null {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) return count ? [] : null;

    const n = Math.abs(count ?? 1);
    const allowDuplicates = count !== undefined && count < 0;

    if (allowDuplicates) {
      // With negative count, allow duplicates
      const result: string[] = [];
      const members = this.smembers(key);
      if (members.length === 0) return [];

      for (let i = 0; i < n; i++) {
        result.push(members[Math.floor(Math.random() * members.length)]);
      }
      return result;
    }

    const rows = this.db.query<{ member: Uint8Array }>(
      'SELECT member FROM sets WHERE key_id = ? ORDER BY RANDOM() LIMIT ?',
      [keyId, n]
    );

    if (rows.length === 0) return count ? [] : null;

    const values = rows.map((r) => new TextDecoder().decode(r.member));
    return count !== undefined ? values : values[0];
  }

  /** SMOVE - Move member from one set to another */
  smove(src: string, dst: string, member: string): boolean {
    if (!this.sismember(src, member)) return false;

    this.srem(src, member);
    this.sadd(dst, member);
    return true;
  }

  /** SDIFF - Return difference of sets */
  sdiff(...keys: string[]): string[] {
    if (keys.length === 0) return [];

    const first = new Set(this.smembers(keys[0]));
    for (let i = 1; i < keys.length; i++) {
      const members = this.smembers(keys[i]);
      for (const m of members) {
        first.delete(m);
      }
    }
    return Array.from(first);
  }

  /** SDIFFSTORE - Store difference of sets */
  sdiffstore(dest: string, ...keys: string[]): number {
    const diff = this.sdiff(...keys);
    if (diff.length === 0) {
      this.db.deleteKey(dest);
      return 0;
    }
    this.db.deleteKey(dest);
    return this.sadd(dest, ...diff);
  }

  /** SINTER - Return intersection of sets */
  sinter(...keys: string[]): string[] {
    if (keys.length === 0) return [];

    let result = new Set(this.smembers(keys[0]));
    for (let i = 1; i < keys.length; i++) {
      const members = new Set(this.smembers(keys[i]));
      result = new Set([...result].filter((m) => members.has(m)));
    }
    return Array.from(result);
  }

  /** SINTERSTORE - Store intersection of sets */
  sinterstore(dest: string, ...keys: string[]): number {
    const inter = this.sinter(...keys);
    if (inter.length === 0) {
      this.db.deleteKey(dest);
      return 0;
    }
    this.db.deleteKey(dest);
    return this.sadd(dest, ...inter);
  }

  /** SUNION - Return union of sets */
  sunion(...keys: string[]): string[] {
    const result = new Set<string>();
    for (const key of keys) {
      for (const member of this.smembers(key)) {
        result.add(member);
      }
    }
    return Array.from(result);
  }

  /** SUNIONSTORE - Store union of sets */
  sunionstore(dest: string, ...keys: string[]): number {
    const union = this.sunion(...keys);
    if (union.length === 0) {
      this.db.deleteKey(dest);
      return 0;
    }
    this.db.deleteKey(dest);
    return this.sadd(dest, ...union);
  }

  /** SSCAN - Incrementally iterate set members */
  sscan(
    key: string,
    cursor: number,
    options?: { match?: string; count?: number }
  ): ScanResult<string> {
    const keyId = this.db.getKeyId(key, KeyType.Set);
    if (keyId === null) {
      return { cursor: 0, items: [] };
    }

    const count = options?.count ?? 10;
    const pattern = options?.match ?? '*';

    // Get all members and filter
    const allMembers = this.smembers(key);
    const regex = new RegExp(
      '^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
    );

    const filtered = allMembers.filter((m) => regex.test(m));
    const items = filtered.slice(cursor, cursor + count);
    const nextCursor = cursor + items.length;

    return {
      cursor: nextCursor >= filtered.length ? 0 : nextCursor,
      items,
    };
  }
}
