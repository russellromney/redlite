/**
 * Key commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyInfo, ScanResult } from '../types';

export class KeyCommands {
  constructor(private db: DbCore) {}

  /** DEL - Delete key(s) */
  del(...keys: string[]): number {
    let deleted = 0;
    for (const key of keys) {
      if (this.db.deleteKey(key)) deleted++;
    }
    return deleted;
  }

  /** EXISTS - Check if key(s) exist */
  exists(...keys: string[]): number {
    let count = 0;
    for (const key of keys) {
      if (this.db.exists(key)) count++;
    }
    return count;
  }

  /** KEYS - Get keys matching pattern */
  keys(pattern: string): string[] {
    return this.db.keys(pattern);
  }

  /** TYPE - Get type of key */
  type(key: string): string {
    return this.db.getType(key) ?? 'none';
  }

  /** RENAME - Rename a key */
  rename(src: string, dst: string): string {
    if (!this.db.exists(src)) {
      throw new Error('ERR no such key');
    }

    // If dst exists, delete it
    this.db.deleteKey(dst);

    // Update key name
    this.db.run(
      'UPDATE keys SET key = ?, updated_at = ? WHERE db = ? AND key = ?',
      [dst, this.db.nowMs(), this.db.currentDb(), src]
    );

    return 'OK';
  }

  /** RENAMENX - Rename key only if new key doesn't exist */
  renamenx(src: string, dst: string): boolean {
    if (!this.db.exists(src)) {
      throw new Error('ERR no such key');
    }
    if (this.db.exists(dst)) return false;

    this.rename(src, dst);
    return true;
  }

  /** EXPIRE - Set key expiration in seconds */
  expire(key: string, seconds: number): boolean {
    const expireAt = this.db.nowMs() + seconds * 1000;
    return this.db.setExpire(key, expireAt);
  }

  /** EXPIREAT - Set key expiration as Unix timestamp */
  expireat(key: string, timestamp: number): boolean {
    return this.db.setExpire(key, timestamp * 1000);
  }

  /** PEXPIRE - Set key expiration in milliseconds */
  pexpire(key: string, milliseconds: number): boolean {
    const expireAt = this.db.nowMs() + milliseconds;
    return this.db.setExpire(key, expireAt);
  }

  /** PEXPIREAT - Set key expiration as Unix timestamp in milliseconds */
  pexpireat(key: string, timestampMs: number): boolean {
    return this.db.setExpire(key, timestampMs);
  }

  /** TTL - Get TTL in seconds */
  ttl(key: string): number {
    const ms = this.db.getTTL(key);
    if (ms === -2 || ms === -1) return ms;
    return Math.ceil(ms / 1000);
  }

  /** PTTL - Get TTL in milliseconds */
  pttl(key: string): number {
    return this.db.getTTL(key);
  }

  /** PERSIST - Remove expiration from key */
  persist(key: string): boolean {
    if (!this.db.exists(key)) return false;
    return this.db.setExpire(key, null);
  }

  /** SCAN - Incrementally iterate keys */
  scan(
    cursor: number,
    options?: { match?: string; count?: number; type?: string }
  ): ScanResult<string> {
    const count = options?.count ?? 10;
    const pattern = options?.match ?? '*';
    const typeFilter = options?.type;

    // Get all matching keys
    const allKeys = this.db.keys(pattern);

    // Filter by type if specified
    const filtered = typeFilter
      ? allKeys.filter((k) => this.type(k) === typeFilter)
      : allKeys;

    const items = filtered.slice(cursor, cursor + count);
    const nextCursor = cursor + items.length;

    return {
      cursor: nextCursor >= filtered.length ? 0 : nextCursor,
      items,
    };
  }

  /** ECHO - Echo the given string */
  echo(message: string): string {
    return message;
  }

  /** KEYINFO - Get detailed key info (Redlite extension) */
  keyinfo(key: string): KeyInfo | null {
    const row = this.db.queryOne<{
      type: number;
      expire_at: number | null;
      version: number;
      created_at: number;
      updated_at: number;
    }>(
      'SELECT type, expire_at, version, created_at, updated_at FROM keys WHERE db = ? AND key = ?',
      [this.db.currentDb(), key]
    );

    if (!row) return null;

    const types = ['string', 'list', 'set', 'zset', 'hash', 'stream', 'geo', 'vector'];
    const ttl = row.expire_at ? row.expire_at - this.db.nowMs() : -1;

    return {
      type: types[row.type] ?? 'unknown',
      ttl: ttl > 0 ? Math.ceil(ttl / 1000) : ttl,
      encoding: 'raw',
      size: 0, // Would need to calculate based on type
      version: row.version,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    };
  }

  /** FLUSHDB - Delete all keys in current database */
  flushdb(): string {
    this.db.flushDb();
    return 'OK';
  }

  /** DBSIZE - Get number of keys in current database */
  dbsize(): number {
    return this.db.dbSize();
  }

  /** SELECT - Select database (0-15) */
  select(db: number): string {
    this.db.select(db);
    return 'OK';
  }

  /** PING - Ping the database */
  ping(message?: string): string {
    return message ?? 'PONG';
  }

  /** INFO - Get database info */
  info(section?: string): string {
    const lines: string[] = [];

    lines.push('# Server');
    lines.push('redlite_version:0.1.0');
    lines.push('arch_bits:wasm');
    lines.push('');
    lines.push('# Keyspace');
    lines.push(`db${this.db.currentDb()}:keys=${this.dbsize()}`);

    return lines.join('\n');
  }

  /** VACUUM - Run SQLite VACUUM (Redlite extension) */
  vacuum(): string {
    this.db.run('VACUUM');
    return 'OK';
  }

  /** AUTOVACUUM - Get/set autovacuum (Redlite extension) */
  autovacuum(enabled?: boolean): boolean | string {
    if (enabled === undefined) {
      return this.db.isAutovacuumEnabled() ? 'ON' : 'OFF';
    }
    this.db.setAutovacuum(enabled);
    return true;
  }
}
