/**
 * String commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyType, SetOptions, GetExOptions } from '../types';

export class StringCommands {
  constructor(private db: DbCore) {}

  /** GET - Get the value of a key */
  get(key: string): string | null {
    const keyId = this.db.getKeyId(key, KeyType.String);
    if (keyId === null) return null;

    const row = this.db.queryOne<{ value: Uint8Array }>(
      'SELECT value FROM strings WHERE key_id = ?',
      [keyId]
    );

    if (!row) return null;
    return new TextDecoder().decode(row.value);
  }

  /** SET - Set the value of a key */
  set(key: string, value: string | Uint8Array, options?: SetOptions): string | null {
    const now = this.db.nowMs();
    const valueBytes = typeof value === 'string' ? new TextEncoder().encode(value) : value;

    // Check NX/XX conditions
    const existingId = this.db.getKeyId(key);

    if (options?.nx && existingId !== null) return null;
    if (options?.xx && existingId === null) return null;

    // Handle GET option - return old value
    let oldValue: string | null = null;
    if (options?.get && existingId !== null) {
      const row = this.db.queryOne<{ value: Uint8Array }>(
        'SELECT value FROM strings WHERE key_id = ?',
        [existingId]
      );
      if (row) {
        oldValue = new TextDecoder().decode(row.value);
      }
    }

    // Calculate expiration
    let expireAt: number | null = null;
    if (options?.ex) expireAt = now + options.ex * 1000;
    else if (options?.px) expireAt = now + options.px;
    else if (options?.exat) expireAt = options.exat * 1000;
    else if (options?.pxat) expireAt = options.pxat;
    else if (options?.keepttl && existingId !== null) {
      const row = this.db.queryOne<{ expire_at: number | null }>(
        'SELECT expire_at FROM keys WHERE id = ?',
        [existingId]
      );
      expireAt = row?.expire_at ?? null;
    }

    // Delete existing key if different type
    if (existingId !== null) {
      const row = this.db.queryOne<{ type: number }>(
        'SELECT type FROM keys WHERE id = ?',
        [existingId]
      );
      if (row && row.type !== KeyType.String) {
        this.db.deleteKey(key);
      }
    }

    // Create or update key
    const keyId = this.db.getOrCreateKey(key, KeyType.String);

    // Upsert string value
    this.db.run(
      `INSERT INTO strings (key_id, value) VALUES (?, ?)
       ON CONFLICT(key_id) DO UPDATE SET value = excluded.value`,
      [keyId, valueBytes]
    );

    // Update expiration
    this.db.run(
      'UPDATE keys SET expire_at = ?, updated_at = ? WHERE id = ?',
      [expireAt, now, keyId]
    );

    return options?.get ? oldValue : 'OK';
  }

  /** APPEND - Append value to existing string */
  append(key: string, value: string): number {
    const valueBytes = new TextEncoder().encode(value);
    const existingId = this.db.getKeyId(key, KeyType.String);

    if (existingId === null) {
      // Create new key
      const keyId = this.db.getOrCreateKey(key, KeyType.String);
      this.db.run('INSERT INTO strings (key_id, value) VALUES (?, ?)', [keyId, valueBytes]);
      return valueBytes.length;
    }

    // Append to existing
    this.db.run(
      `UPDATE strings SET value = value || ? WHERE key_id = ?`,
      [valueBytes, existingId]
    );
    this.db.touchKey(existingId);

    const row = this.db.queryOne<{ value: Uint8Array }>(
      'SELECT value FROM strings WHERE key_id = ?',
      [existingId]
    );
    return row?.value.length ?? valueBytes.length;
  }

  /** GETRANGE - Get substring of string value */
  getrange(key: string, start: number, end: number): string {
    const value = this.get(key);
    if (value === null) return '';

    // Handle negative indices
    const len = value.length;
    if (start < 0) start = Math.max(0, len + start);
    if (end < 0) end = len + end;
    if (start > end || start >= len) return '';

    end = Math.min(end, len - 1);
    return value.substring(start, end + 1);
  }

  /** SETRANGE - Overwrite part of string at offset */
  setrange(key: string, offset: number, value: string): number {
    let current = this.get(key) ?? '';

    // Pad with null bytes if needed
    if (offset > current.length) {
      current = current.padEnd(offset, '\0');
    }

    // Replace portion
    const newValue = current.substring(0, offset) + value + current.substring(offset + value.length);
    this.set(key, newValue);
    return newValue.length;
  }

  /** STRLEN - Get length of string value */
  strlen(key: string): number {
    const value = this.get(key);
    return value?.length ?? 0;
  }

  /** GETEX - Get value and optionally set expiration */
  getex(key: string, options?: GetExOptions): string | null {
    const value = this.get(key);
    if (value === null) return null;

    const now = this.db.nowMs();
    let expireAt: number | null = null;

    if (options?.ex) expireAt = now + options.ex * 1000;
    else if (options?.px) expireAt = now + options.px;
    else if (options?.exat) expireAt = options.exat * 1000;
    else if (options?.pxat) expireAt = options.pxat;
    else if (options?.persist) expireAt = null;

    if (options) {
      this.db.setExpire(key, expireAt);
    }

    return value;
  }

  /** GETDEL - Get value and delete key */
  getdel(key: string): string | null {
    const value = this.get(key);
    if (value !== null) {
      this.db.deleteKey(key);
    }
    return value;
  }

  /** SETEX - Set value with expiration in seconds */
  setex(key: string, seconds: number, value: string): string {
    return this.set(key, value, { ex: seconds }) ?? 'OK';
  }

  /** PSETEX - Set value with expiration in milliseconds */
  psetex(key: string, milliseconds: number, value: string): string {
    return this.set(key, value, { px: milliseconds }) ?? 'OK';
  }

  /** SETNX - Set value only if key doesn't exist */
  setnx(key: string, value: string): boolean {
    return this.set(key, value, { nx: true }) !== null;
  }

  /** MGET - Get values of multiple keys */
  mget(...keys: string[]): (string | null)[] {
    return keys.map((key) => this.get(key));
  }

  /** MSET - Set multiple key-value pairs */
  mset(mapping: Record<string, string>): string {
    for (const [key, value] of Object.entries(mapping)) {
      this.set(key, value);
    }
    return 'OK';
  }

  /** MSETNX - Set multiple key-value pairs only if none exist */
  msetnx(mapping: Record<string, string>): boolean {
    // Check if any key exists
    for (const key of Object.keys(mapping)) {
      if (this.db.exists(key)) return false;
    }

    // All keys don't exist, set them
    for (const [key, value] of Object.entries(mapping)) {
      this.set(key, value);
    }
    return true;
  }

  /** INCR - Increment integer value by 1 */
  incr(key: string): number {
    return this.incrby(key, 1);
  }

  /** INCRBY - Increment integer value by amount */
  incrby(key: string, amount: number): number {
    const current = this.get(key);
    let value = 0;

    if (current !== null) {
      value = parseInt(current, 10);
      if (isNaN(value)) {
        throw new Error('ERR value is not an integer or out of range');
      }
    }

    value += amount;
    this.set(key, value.toString());
    return value;
  }

  /** INCRBYFLOAT - Increment float value by amount */
  incrbyfloat(key: string, amount: number): number {
    const current = this.get(key);
    let value = 0;

    if (current !== null) {
      value = parseFloat(current);
      if (isNaN(value)) {
        throw new Error('ERR value is not a valid float');
      }
    }

    value += amount;
    this.set(key, value.toString());
    return value;
  }

  /** DECR - Decrement integer value by 1 */
  decr(key: string): number {
    return this.incrby(key, -1);
  }

  /** DECRBY - Decrement integer value by amount */
  decrby(key: string, amount: number): number {
    return this.incrby(key, -amount);
  }

  /** GETBIT - Get bit value at offset */
  getbit(key: string, offset: number): number {
    const value = this.get(key);
    if (value === null) return 0;

    const bytes = new TextEncoder().encode(value);
    const byteIndex = Math.floor(offset / 8);
    const bitIndex = 7 - (offset % 8);

    if (byteIndex >= bytes.length) return 0;
    return (bytes[byteIndex] >> bitIndex) & 1;
  }

  /** SETBIT - Set bit value at offset */
  setbit(key: string, offset: number, bit: number): number {
    let bytes: Uint8Array;
    const current = this.get(key);

    if (current === null) {
      bytes = new Uint8Array(Math.ceil((offset + 1) / 8));
    } else {
      const currentBytes = new TextEncoder().encode(current);
      const requiredLen = Math.ceil((offset + 1) / 8);
      bytes = new Uint8Array(Math.max(currentBytes.length, requiredLen));
      bytes.set(currentBytes);
    }

    const byteIndex = Math.floor(offset / 8);
    const bitIndex = 7 - (offset % 8);
    const oldBit = (bytes[byteIndex] >> bitIndex) & 1;

    if (bit) {
      bytes[byteIndex] |= (1 << bitIndex);
    } else {
      bytes[byteIndex] &= ~(1 << bitIndex);
    }

    this.set(key, bytes);
    return oldBit;
  }

  /** BITCOUNT - Count set bits in string */
  bitcount(key: string, start?: number, end?: number): number {
    const value = this.get(key);
    if (value === null) return 0;

    const bytes = new TextEncoder().encode(value);
    let startByte = start ?? 0;
    let endByte = end ?? bytes.length - 1;

    // Handle negative indices
    if (startByte < 0) startByte = bytes.length + startByte;
    if (endByte < 0) endByte = bytes.length + endByte;

    startByte = Math.max(0, startByte);
    endByte = Math.min(bytes.length - 1, endByte);

    let count = 0;
    for (let i = startByte; i <= endByte; i++) {
      let byte = bytes[i];
      while (byte) {
        count += byte & 1;
        byte >>= 1;
      }
    }
    return count;
  }

  /** BITOP - Perform bitwise operation between strings */
  bitop(operation: string, destKey: string, ...keys: string[]): number {
    if (keys.length === 0) {
      throw new Error('ERR wrong number of arguments for BITOP');
    }

    // Get all values as byte arrays
    const values = keys.map((key) => {
      const val = this.get(key);
      return val ? new TextEncoder().encode(val) : new Uint8Array(0);
    });

    // Find max length
    const maxLen = Math.max(...values.map((v) => v.length));
    if (maxLen === 0) {
      this.db.deleteKey(destKey);
      return 0;
    }

    // Perform operation
    const result = new Uint8Array(maxLen);
    const op = operation.toUpperCase();

    for (let i = 0; i < maxLen; i++) {
      let byte: number;

      switch (op) {
        case 'AND':
          byte = 0xff;
          for (const v of values) {
            byte &= i < v.length ? v[i] : 0;
          }
          break;
        case 'OR':
          byte = 0;
          for (const v of values) {
            byte |= i < v.length ? v[i] : 0;
          }
          break;
        case 'XOR':
          byte = 0;
          for (const v of values) {
            byte ^= i < v.length ? v[i] : 0;
          }
          break;
        case 'NOT':
          if (keys.length !== 1) {
            throw new Error('ERR BITOP NOT requires exactly one source key');
          }
          byte = ~(i < values[0].length ? values[0][i] : 0) & 0xff;
          break;
        default:
          throw new Error(`ERR unknown BITOP operation: ${operation}`);
      }

      result[i] = byte;
    }

    this.set(destKey, result);
    return result.length;
  }
}
