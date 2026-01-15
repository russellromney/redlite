/**
 * Sorted Set commands for Redlite
 */

import type { DbCore } from '../db';
import { KeyType, ZMember, ScanResult } from '../types';

export class ZSetCommands {
  constructor(private db: DbCore) {}

  /** ZADD - Add member(s) to sorted set */
  zadd(
    key: string,
    members: ZMember[] | Record<string, number>,
    options?: { nx?: boolean; xx?: boolean; gt?: boolean; lt?: boolean; ch?: boolean; incr?: boolean }
  ): number | null {
    const keyId = this.db.getOrCreateKey(key, KeyType.ZSet);

    // Normalize input
    const items: ZMember[] = Array.isArray(members)
      ? members
      : Object.entries(members).map(([member, score]) => ({ member, score }));

    let changed = 0;
    let added = 0;
    let lastScore: number | null = null;

    for (const { member, score } of items) {
      const memberBytes = new TextEncoder().encode(member);

      const existing = this.db.queryOne<{ score: number }>(
        'SELECT score FROM zsets WHERE key_id = ? AND member = ?',
        [keyId, memberBytes]
      );

      if (existing) {
        // Member exists
        if (options?.nx) continue;

        let newScore = options?.incr ? existing.score + score : score;

        // GT/LT conditions
        if (options?.gt && newScore <= existing.score) continue;
        if (options?.lt && newScore >= existing.score) continue;

        this.db.run(
          'UPDATE zsets SET score = ? WHERE key_id = ? AND member = ?',
          [newScore, keyId, memberBytes]
        );

        if (newScore !== existing.score) changed++;
        lastScore = newScore;
      } else {
        // New member
        if (options?.xx) continue;

        const newScore = options?.incr && existing ? existing.score + score : score;
        this.db.run(
          'INSERT INTO zsets (key_id, member, score) VALUES (?, ?, ?)',
          [keyId, memberBytes, newScore]
        );
        added++;
        changed++;
        lastScore = newScore;
      }
    }

    this.db.touchKey(keyId);

    if (options?.incr) {
      return lastScore;
    }
    return options?.ch ? changed : added;
  }

  /** ZREM - Remove member(s) from sorted set */
  zrem(key: string, ...members: string[]): number {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return 0;

    let removed = 0;
    for (const member of members) {
      const memberBytes = new TextEncoder().encode(member);
      this.db.run(
        'DELETE FROM zsets WHERE key_id = ? AND member = ?',
        [keyId, memberBytes]
      );
      const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
      removed += changes?.changes ?? 0;
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM zsets WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return removed;
  }

  /** ZSCORE - Get score of member */
  zscore(key: string, member: string): number | null {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return null;

    const memberBytes = new TextEncoder().encode(member);
    const row = this.db.queryOne<{ score: number }>(
      'SELECT score FROM zsets WHERE key_id = ? AND member = ?',
      [keyId, memberBytes]
    );

    return row?.score ?? null;
  }

  /** ZRANGE - Get range of members by index or score */
  zrange(
    key: string,
    start: number | string,
    stop: number | string,
    options?: { withscores?: boolean; rev?: boolean; byscore?: boolean; bylex?: boolean; limit?: { offset: number; count: number } }
  ): string[] | ZMember[] {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return [];

    const order = options?.rev ? 'DESC' : 'ASC';

    if (options?.byscore) {
      return this.zrangebyscore(key, start, stop, options);
    }

    // Index-based range
    const len = this.zcard(key);
    let startIdx = typeof start === 'number' ? start : parseInt(start, 10);
    let stopIdx = typeof stop === 'number' ? stop : parseInt(stop, 10);

    // Handle negative indices
    if (startIdx < 0) startIdx = Math.max(0, len + startIdx);
    if (stopIdx < 0) stopIdx = len + stopIdx;
    if (startIdx > stopIdx || startIdx >= len) return [];
    stopIdx = Math.min(stopIdx, len - 1);

    const rows = this.db.query<{ member: Uint8Array; score: number }>(
      `SELECT member, score FROM zsets WHERE key_id = ?
       ORDER BY score ${order}, member ${order}
       LIMIT ? OFFSET ?`,
      [keyId, stopIdx - startIdx + 1, startIdx]
    );

    if (options?.withscores) {
      return rows.map((r) => ({
        member: new TextDecoder().decode(r.member),
        score: r.score,
      }));
    }
    return rows.map((r) => new TextDecoder().decode(r.member));
  }

  /** ZRANGEBYSCORE - Get members by score range */
  zrangebyscore(
    key: string,
    min: number | string,
    max: number | string,
    options?: { withscores?: boolean; rev?: boolean; limit?: { offset: number; count: number } }
  ): string[] | ZMember[] {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return [];

    // Parse min/max (handle -inf, +inf, exclusive)
    let minVal = this.parseScoreBound(min, true);
    let maxVal = this.parseScoreBound(max, false);
    const minExclusive = typeof min === 'string' && min.startsWith('(');
    const maxExclusive = typeof max === 'string' && max.startsWith('(');

    const order = options?.rev ? 'DESC' : 'ASC';
    const minOp = minExclusive ? '>' : '>=';
    const maxOp = maxExclusive ? '<' : '<=';

    let query = `SELECT member, score FROM zsets
                 WHERE key_id = ? AND score ${minOp} ? AND score ${maxOp} ?
                 ORDER BY score ${order}, member ${order}`;

    const params: unknown[] = [keyId, minVal, maxVal];

    if (options?.limit) {
      query += ` LIMIT ? OFFSET ?`;
      params.push(options.limit.count, options.limit.offset);
    }

    const rows = this.db.query<{ member: Uint8Array; score: number }>(query, params);

    if (options?.withscores) {
      return rows.map((r) => ({
        member: new TextDecoder().decode(r.member),
        score: r.score,
      }));
    }
    return rows.map((r) => new TextDecoder().decode(r.member));
  }

  /** ZCARD - Get cardinality */
  zcard(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM zsets WHERE key_id = ?',
      [keyId]
    );
    return result?.count ?? 0;
  }

  /** ZCOUNT - Count members in score range */
  zcount(key: string, min: number | string, max: number | string): number {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return 0;

    const minVal = this.parseScoreBound(min, true);
    const maxVal = this.parseScoreBound(max, false);
    const minExclusive = typeof min === 'string' && min.startsWith('(');
    const maxExclusive = typeof max === 'string' && max.startsWith('(');

    const minOp = minExclusive ? '>' : '>=';
    const maxOp = maxExclusive ? '<' : '<=';

    const result = this.db.queryOne<{ count: number }>(
      `SELECT COUNT(*) as count FROM zsets
       WHERE key_id = ? AND score ${minOp} ? AND score ${maxOp} ?`,
      [keyId, minVal, maxVal]
    );
    return result?.count ?? 0;
  }

  /** ZINCRBY - Increment score of member */
  zincrby(key: string, increment: number, member: string): number {
    const result = this.zadd(key, [{ member, score: increment }], { incr: true });
    return result ?? increment;
  }

  /** ZRANK - Get rank of member (ascending) */
  zrank(key: string, member: string): number | null {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return null;

    const memberBytes = new TextEncoder().encode(member);
    const row = this.db.queryOne<{ score: number }>(
      'SELECT score FROM zsets WHERE key_id = ? AND member = ?',
      [keyId, memberBytes]
    );

    if (!row) return null;

    const result = this.db.queryOne<{ rank: number }>(
      `SELECT COUNT(*) as rank FROM zsets
       WHERE key_id = ? AND (score < ? OR (score = ? AND member < ?))`,
      [keyId, row.score, row.score, memberBytes]
    );
    return result?.rank ?? null;
  }

  /** ZREVRANK - Get rank of member (descending) */
  zrevrank(key: string, member: string): number | null {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return null;

    const memberBytes = new TextEncoder().encode(member);
    const row = this.db.queryOne<{ score: number }>(
      'SELECT score FROM zsets WHERE key_id = ? AND member = ?',
      [keyId, memberBytes]
    );

    if (!row) return null;

    const result = this.db.queryOne<{ rank: number }>(
      `SELECT COUNT(*) as rank FROM zsets
       WHERE key_id = ? AND (score > ? OR (score = ? AND member > ?))`,
      [keyId, row.score, row.score, memberBytes]
    );
    return result?.rank ?? null;
  }

  /** ZREVRANGE - Get range in reverse order */
  zrevrange(
    key: string,
    start: number,
    stop: number,
    options?: { withscores?: boolean }
  ): string[] | ZMember[] {
    return this.zrange(key, start, stop, { ...options, rev: true });
  }

  /** ZREMRANGEBYRANK - Remove by rank range */
  zremrangebyrank(key: string, start: number, stop: number): number {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return 0;

    // Get members to remove
    const members = this.zrange(key, start, stop) as string[];
    return this.zrem(key, ...members);
  }

  /** ZREMRANGEBYSCORE - Remove by score range */
  zremrangebyscore(key: string, min: number | string, max: number | string): number {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) return 0;

    const minVal = this.parseScoreBound(min, true);
    const maxVal = this.parseScoreBound(max, false);
    const minExclusive = typeof min === 'string' && min.startsWith('(');
    const maxExclusive = typeof max === 'string' && max.startsWith('(');

    const minOp = minExclusive ? '>' : '>=';
    const maxOp = maxExclusive ? '<' : '<=';

    this.db.run(
      `DELETE FROM zsets
       WHERE key_id = ? AND score ${minOp} ? AND score ${maxOp} ?`,
      [keyId, minVal, maxVal]
    );

    const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
    const removed = changes?.changes ?? 0;

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM zsets WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return removed;
  }

  /** ZINTERSTORE - Store intersection of sorted sets */
  zinterstore(
    dest: string,
    keys: string[],
    options?: { weights?: number[]; aggregate?: 'SUM' | 'MIN' | 'MAX' }
  ): number {
    if (keys.length === 0) {
      this.db.deleteKey(dest);
      return 0;
    }

    const weights = options?.weights ?? keys.map(() => 1);
    const aggregate = options?.aggregate ?? 'SUM';

    // Get all members with scores
    const memberScores = new Map<string, number[]>();

    for (let i = 0; i < keys.length; i++) {
      const members = this.zrange(keys[i], 0, -1, { withscores: true }) as ZMember[];
      for (const { member, score } of members) {
        if (!memberScores.has(member)) {
          memberScores.set(member, []);
        }
        memberScores.get(member)![i] = score * weights[i];
      }
    }

    // Filter to intersection and aggregate
    const result: ZMember[] = [];
    for (const [member, scores] of memberScores) {
      // Must have score from all keys
      if (scores.filter((s) => s !== undefined).length !== keys.length) continue;

      let finalScore: number;
      switch (aggregate) {
        case 'MIN':
          finalScore = Math.min(...scores);
          break;
        case 'MAX':
          finalScore = Math.max(...scores);
          break;
        default:
          finalScore = scores.reduce((a, b) => a + b, 0);
      }
      result.push({ member, score: finalScore });
    }

    // Store result
    this.db.deleteKey(dest);
    if (result.length > 0) {
      this.zadd(dest, result);
    }
    return result.length;
  }

  /** ZUNIONSTORE - Store union of sorted sets */
  zunionstore(
    dest: string,
    keys: string[],
    options?: { weights?: number[]; aggregate?: 'SUM' | 'MIN' | 'MAX' }
  ): number {
    if (keys.length === 0) {
      this.db.deleteKey(dest);
      return 0;
    }

    const weights = options?.weights ?? keys.map(() => 1);
    const aggregate = options?.aggregate ?? 'SUM';

    // Get all members with scores
    const memberScores = new Map<string, number[]>();

    for (let i = 0; i < keys.length; i++) {
      const members = this.zrange(keys[i], 0, -1, { withscores: true }) as ZMember[];
      for (const { member, score } of members) {
        if (!memberScores.has(member)) {
          memberScores.set(member, []);
        }
        memberScores.get(member)![i] = score * weights[i];
      }
    }

    // Aggregate all members
    const result: ZMember[] = [];
    for (const [member, scores] of memberScores) {
      const validScores = scores.filter((s) => s !== undefined);
      let finalScore: number;
      switch (aggregate) {
        case 'MIN':
          finalScore = Math.min(...validScores);
          break;
        case 'MAX':
          finalScore = Math.max(...validScores);
          break;
        default:
          finalScore = validScores.reduce((a, b) => a + b, 0);
      }
      result.push({ member, score: finalScore });
    }

    // Store result
    this.db.deleteKey(dest);
    if (result.length > 0) {
      this.zadd(dest, result);
    }
    return result.length;
  }

  /** ZSCAN - Incrementally iterate sorted set */
  zscan(
    key: string,
    cursor: number,
    options?: { match?: string; count?: number }
  ): ScanResult<ZMember> {
    const keyId = this.db.getKeyId(key, KeyType.ZSet);
    if (keyId === null) {
      return { cursor: 0, items: [] };
    }

    const count = options?.count ?? 10;
    const pattern = options?.match ?? '*';
    const regex = new RegExp(
      '^' + pattern.replace(/\*/g, '.*').replace(/\?/g, '.') + '$'
    );

    const rows = this.db.query<{ member: Uint8Array; score: number }>(
      `SELECT member, score FROM zsets WHERE key_id = ?
       ORDER BY score, member
       LIMIT ? OFFSET ?`,
      [keyId, count, cursor]
    );

    const items: ZMember[] = rows
      .map((r) => ({
        member: new TextDecoder().decode(r.member),
        score: r.score,
      }))
      .filter((m) => regex.test(m.member));

    const total = this.zcard(key);
    const nextCursor = cursor + rows.length;

    return {
      cursor: nextCursor >= total ? 0 : nextCursor,
      items,
    };
  }

  private parseScoreBound(value: number | string, isMin: boolean): number {
    if (typeof value === 'number') return value;
    const str = value.replace(/^\(/, '');
    if (str === '-inf') return -Infinity;
    if (str === '+inf' || str === 'inf') return Infinity;
    return parseFloat(str);
  }
}
