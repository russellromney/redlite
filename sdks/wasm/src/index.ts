/**
 * Redlite WASM SDK
 *
 * Redis API + SQLite durability. Embedded Redis-compatible database for browser and Node.js.
 *
 * @example
 * ```typescript
 * import { Redlite } from 'redlite';
 *
 * // Open embedded database
 * const db = await Redlite.open();
 *
 * // Use Redis commands
 * db.set('key', 'value');
 * db.lpush('queue', 'job1', 'job2');
 * db.hset('user:1', { name: 'Alice' });
 *
 * // Full-text search
 * db.fts.enable({ global: true });
 * db.fts.search('hello world');
 *
 * // Vector search
 * db.vector.add('embeddings', 'doc1', [0.1, 0.2, 0.3]);
 * db.vector.search('embeddings', [0.1, 0.2, 0.3]);
 *
 * // History tracking
 * db.history.enable({ global: true });
 * db.history.get('mykey');
 *
 * // Export database
 * const data = db.export();
 * localStorage.setItem('redlite', btoa(String.fromCharCode(...data)));
 * ```
 */

import { DbCore } from './db';
import {
  StringCommands,
  HashCommands,
  ListCommands,
  SetCommands,
  ZSetCommands,
  KeyCommands,
} from './commands';
import {
  FtsNamespace,
  HistoryNamespace,
  VectorNamespace,
  GeoNamespace,
} from './namespaces';

// Re-export types
export * from './types';

/**
 * Redlite - Embedded Redis-compatible database using SQLite WASM
 */
export class Redlite {
  private core: DbCore;

  // Command modules
  private stringCmds: StringCommands;
  private hashCmds: HashCommands;
  private listCmds: ListCommands;
  private setCmds: SetCommands;
  private zsetCmds: ZSetCommands;
  private keyCmds: KeyCommands;

  // Namespaces
  /** Full-text search */
  public fts: FtsNamespace;
  /** History / time-travel */
  public history: HistoryNamespace;
  /** Vector search */
  public vector: VectorNamespace;
  /** Geospatial */
  public geo: GeoNamespace;

  private constructor(core: DbCore) {
    this.core = core;

    // Initialize command modules
    this.stringCmds = new StringCommands(core);
    this.hashCmds = new HashCommands(core);
    this.listCmds = new ListCommands(core);
    this.setCmds = new SetCommands(core);
    this.zsetCmds = new ZSetCommands(core);
    this.keyCmds = new KeyCommands(core);

    // Initialize namespaces
    this.fts = new FtsNamespace(core);
    this.history = new HistoryNamespace(core);
    this.vector = new VectorNamespace(core);
    this.geo = new GeoNamespace(core);
  }

  /**
   * Open an embedded database
   *
   * @param data - Optional existing database binary data
   * @returns Promise<Redlite> instance
   *
   * @example
   * ```typescript
   * // New database
   * const db = await Redlite.open();
   *
   * // Load existing database
   * const saved = localStorage.getItem('redlite');
   * const data = saved ? Uint8Array.from(atob(saved), c => c.charCodeAt(0)) : undefined;
   * const db = await Redlite.open(data);
   * ```
   */
  static async open(data?: ArrayLike<number>): Promise<Redlite> {
    const core = await DbCore.open(data);
    return new Redlite(core);
  }

  /**
   * Export database as binary data
   *
   * Use this to persist the database to localStorage, IndexedDB, or file.
   *
   * @example
   * ```typescript
   * const data = db.export();
   * localStorage.setItem('redlite', btoa(String.fromCharCode(...data)));
   * ```
   */
  export(): Uint8Array {
    return this.core.export();
  }

  /**
   * Close the database
   */
  close(): void {
    this.core.close();
  }

  // -------------------------------------------------------------------------
  // String Commands
  // -------------------------------------------------------------------------

  /** Get the value of a key */
  get(key: string): string | null {
    return this.stringCmds.get(key);
  }

  /** Set the value of a key */
  set(
    key: string,
    value: string | Uint8Array,
    options?: {
      ex?: number;
      px?: number;
      exat?: number;
      pxat?: number;
      nx?: boolean;
      xx?: boolean;
      keepttl?: boolean;
      get?: boolean;
    }
  ): string | null {
    return this.stringCmds.set(key, value, options);
  }

  /** Append value to key */
  append(key: string, value: string): number {
    return this.stringCmds.append(key, value);
  }

  /** Get substring of string value */
  getrange(key: string, start: number, end: number): string {
    return this.stringCmds.getrange(key, start, end);
  }

  /** Overwrite part of string at offset */
  setrange(key: string, offset: number, value: string): number {
    return this.stringCmds.setrange(key, offset, value);
  }

  /** Get length of string value */
  strlen(key: string): number {
    return this.stringCmds.strlen(key);
  }

  /** Get value and set expiration */
  getex(key: string, options?: { ex?: number; px?: number; exat?: number; pxat?: number; persist?: boolean }): string | null {
    return this.stringCmds.getex(key, options);
  }

  /** Get value and delete key */
  getdel(key: string): string | null {
    return this.stringCmds.getdel(key);
  }

  /** Set with expiration in seconds */
  setex(key: string, seconds: number, value: string): string {
    return this.stringCmds.setex(key, seconds, value);
  }

  /** Set with expiration in milliseconds */
  psetex(key: string, milliseconds: number, value: string): string {
    return this.stringCmds.psetex(key, milliseconds, value);
  }

  /** Set if not exists */
  setnx(key: string, value: string): boolean {
    return this.stringCmds.setnx(key, value);
  }

  /** Get multiple values */
  mget(...keys: string[]): (string | null)[] {
    return this.stringCmds.mget(...keys);
  }

  /** Set multiple values */
  mset(mapping: Record<string, string>): string {
    return this.stringCmds.mset(mapping);
  }

  /** Increment integer by 1 */
  incr(key: string): number {
    return this.stringCmds.incr(key);
  }

  /** Increment integer by amount */
  incrby(key: string, amount: number): number {
    return this.stringCmds.incrby(key, amount);
  }

  /** Increment float by amount */
  incrbyfloat(key: string, amount: number): number {
    return this.stringCmds.incrbyfloat(key, amount);
  }

  /** Decrement integer by 1 */
  decr(key: string): number {
    return this.stringCmds.decr(key);
  }

  /** Decrement integer by amount */
  decrby(key: string, amount: number): number {
    return this.stringCmds.decrby(key, amount);
  }

  /** Get bit at offset */
  getbit(key: string, offset: number): number {
    return this.stringCmds.getbit(key, offset);
  }

  /** Set bit at offset */
  setbit(key: string, offset: number, bit: number): number {
    return this.stringCmds.setbit(key, offset, bit);
  }

  /** Count set bits */
  bitcount(key: string, start?: number, end?: number): number {
    return this.stringCmds.bitcount(key, start, end);
  }

  /** Bitwise operation */
  bitop(operation: string, destKey: string, ...keys: string[]): number {
    return this.stringCmds.bitop(operation, destKey, ...keys);
  }

  // -------------------------------------------------------------------------
  // Hash Commands
  // -------------------------------------------------------------------------

  /** Set hash field(s) */
  hset(key: string, fieldOrMapping: string | Record<string, string>, value?: string): number {
    return this.hashCmds.hset(key, fieldOrMapping, value);
  }

  /** Get hash field value */
  hget(key: string, field: string): string | null {
    return this.hashCmds.hget(key, field);
  }

  /** Get all hash fields and values */
  hgetall(key: string): Record<string, string> {
    return this.hashCmds.hgetall(key);
  }

  /** Delete hash field(s) */
  hdel(key: string, ...fields: string[]): number {
    return this.hashCmds.hdel(key, ...fields);
  }

  /** Check if hash field exists */
  hexists(key: string, field: string): boolean {
    return this.hashCmds.hexists(key, field);
  }

  /** Get number of hash fields */
  hlen(key: string): number {
    return this.hashCmds.hlen(key);
  }

  /** Get all hash field names */
  hkeys(key: string): string[] {
    return this.hashCmds.hkeys(key);
  }

  /** Get all hash values */
  hvals(key: string): string[] {
    return this.hashCmds.hvals(key);
  }

  /** Get multiple hash field values */
  hmget(key: string, ...fields: string[]): (string | null)[] {
    return this.hashCmds.hmget(key, ...fields);
  }

  /** Set hash field if not exists */
  hsetnx(key: string, field: string, value: string): boolean {
    return this.hashCmds.hsetnx(key, field, value);
  }

  /** Increment hash field integer */
  hincrby(key: string, field: string, amount: number): number {
    return this.hashCmds.hincrby(key, field, amount);
  }

  /** Increment hash field float */
  hincrbyfloat(key: string, field: string, amount: number): number {
    return this.hashCmds.hincrbyfloat(key, field, amount);
  }

  // -------------------------------------------------------------------------
  // List Commands
  // -------------------------------------------------------------------------

  /** Push to list head */
  lpush(key: string, ...values: string[]): number {
    return this.listCmds.lpush(key, ...values);
  }

  /** Push to list tail */
  rpush(key: string, ...values: string[]): number {
    return this.listCmds.rpush(key, ...values);
  }

  /** Pop from list head */
  lpop(key: string, count?: number): string | string[] | null {
    return this.listCmds.lpop(key, count);
  }

  /** Pop from list tail */
  rpop(key: string, count?: number): string | string[] | null {
    return this.listCmds.rpop(key, count);
  }

  /** Get list range */
  lrange(key: string, start: number, stop: number): string[] {
    return this.listCmds.lrange(key, start, stop);
  }

  /** Get list length */
  llen(key: string): number {
    return this.listCmds.llen(key);
  }

  /** Get list element at index */
  lindex(key: string, index: number): string | null {
    return this.listCmds.lindex(key, index);
  }

  /** Insert before/after pivot */
  linsert(key: string, where: 'BEFORE' | 'AFTER', pivot: string, value: string): number {
    return this.listCmds.linsert(key, where, pivot, value);
  }

  /** Set list element at index */
  lset(key: string, index: number, value: string): string {
    return this.listCmds.lset(key, index, value);
  }

  /** Remove list elements */
  lrem(key: string, count: number, value: string): number {
    return this.listCmds.lrem(key, count, value);
  }

  /** Trim list to range */
  ltrim(key: string, start: number, stop: number): string {
    return this.listCmds.ltrim(key, start, stop);
  }

  // -------------------------------------------------------------------------
  // Set Commands
  // -------------------------------------------------------------------------

  /** Add to set */
  sadd(key: string, ...members: string[]): number {
    return this.setCmds.sadd(key, ...members);
  }

  /** Remove from set */
  srem(key: string, ...members: string[]): number {
    return this.setCmds.srem(key, ...members);
  }

  /** Get all set members */
  smembers(key: string): string[] {
    return this.setCmds.smembers(key);
  }

  /** Check if member exists */
  sismember(key: string, member: string): boolean {
    return this.setCmds.sismember(key, member);
  }

  /** Get set cardinality */
  scard(key: string): number {
    return this.setCmds.scard(key);
  }

  /** Pop random member(s) */
  spop(key: string, count?: number): string | string[] | null {
    return this.setCmds.spop(key, count);
  }

  /** Get random member(s) */
  srandmember(key: string, count?: number): string | string[] | null {
    return this.setCmds.srandmember(key, count);
  }

  /** Set difference */
  sdiff(...keys: string[]): string[] {
    return this.setCmds.sdiff(...keys);
  }

  /** Set intersection */
  sinter(...keys: string[]): string[] {
    return this.setCmds.sinter(...keys);
  }

  /** Set union */
  sunion(...keys: string[]): string[] {
    return this.setCmds.sunion(...keys);
  }

  // -------------------------------------------------------------------------
  // Sorted Set Commands
  // -------------------------------------------------------------------------

  /** Add to sorted set */
  zadd(
    key: string,
    members: Array<{ member: string; score: number }> | Record<string, number>,
    options?: { nx?: boolean; xx?: boolean; gt?: boolean; lt?: boolean; ch?: boolean; incr?: boolean }
  ): number | null {
    return this.zsetCmds.zadd(key, members, options);
  }

  /** Remove from sorted set */
  zrem(key: string, ...members: string[]): number {
    return this.zsetCmds.zrem(key, ...members);
  }

  /** Get member score */
  zscore(key: string, member: string): number | null {
    return this.zsetCmds.zscore(key, member);
  }

  /** Get range of members */
  zrange(
    key: string,
    start: number | string,
    stop: number | string,
    options?: { withscores?: boolean; rev?: boolean; byscore?: boolean }
  ): string[] | Array<{ member: string; score: number }> {
    return this.zsetCmds.zrange(key, start, stop, options);
  }

  /** Get sorted set cardinality */
  zcard(key: string): number {
    return this.zsetCmds.zcard(key);
  }

  /** Count members in score range */
  zcount(key: string, min: number | string, max: number | string): number {
    return this.zsetCmds.zcount(key, min, max);
  }

  /** Increment member score */
  zincrby(key: string, increment: number, member: string): number {
    return this.zsetCmds.zincrby(key, increment, member);
  }

  /** Get member rank (ascending) */
  zrank(key: string, member: string): number | null {
    return this.zsetCmds.zrank(key, member);
  }

  /** Get member rank (descending) */
  zrevrank(key: string, member: string): number | null {
    return this.zsetCmds.zrevrank(key, member);
  }

  // -------------------------------------------------------------------------
  // Key Commands
  // -------------------------------------------------------------------------

  /** Delete key(s) */
  del(...keys: string[]): number {
    return this.keyCmds.del(...keys);
  }

  /** Check if key(s) exist */
  exists(...keys: string[]): number {
    return this.keyCmds.exists(...keys);
  }

  /** Get keys matching pattern */
  keys(pattern: string): string[] {
    return this.keyCmds.keys(pattern);
  }

  /** Get key type */
  type(key: string): string {
    return this.keyCmds.type(key);
  }

  /** Rename key */
  rename(src: string, dst: string): string {
    return this.keyCmds.rename(src, dst);
  }

  /** Set key expiration (seconds) */
  expire(key: string, seconds: number): boolean {
    return this.keyCmds.expire(key, seconds);
  }

  /** Get TTL (seconds) */
  ttl(key: string): number {
    return this.keyCmds.ttl(key);
  }

  /** Get TTL (milliseconds) */
  pttl(key: string): number {
    return this.keyCmds.pttl(key);
  }

  /** Remove expiration */
  persist(key: string): boolean {
    return this.keyCmds.persist(key);
  }

  /** Flush current database */
  flushdb(): string {
    return this.keyCmds.flushdb();
  }

  /** Get database size */
  dbsize(): number {
    return this.keyCmds.dbsize();
  }

  /** Select database */
  select(db: number): string {
    return this.keyCmds.select(db);
  }

  /** Ping */
  ping(message?: string): string {
    return this.keyCmds.ping(message);
  }

  /** Echo */
  echo(message: string): string {
    return this.keyCmds.echo(message);
  }

  /** Get info */
  info(section?: string): string {
    return this.keyCmds.info(section);
  }

  // -------------------------------------------------------------------------
  // Geo Commands (via namespace)
  // -------------------------------------------------------------------------

  /** Add geo members */
  geoadd(
    key: string,
    members: Record<string, [number, number] | { longitude: number; latitude: number }>
  ): number {
    return this.geo.add(key, members);
  }

  /** Get geo positions */
  geopos(key: string, ...members: string[]): Array<{ longitude: number; latitude: number } | null> {
    return this.geo.pos(key, ...members);
  }

  /** Get distance between members */
  geodist(key: string, member1: string, member2: string, unit?: 'm' | 'km' | 'mi' | 'ft'): number | null {
    return this.geo.dist(key, member1, member2, unit);
  }

  /** Get geohash */
  geohash(key: string, ...members: string[]): (string | null)[] {
    return this.geo.hash(key, ...members);
  }

  // -------------------------------------------------------------------------
  // Redlite Extensions
  // -------------------------------------------------------------------------

  /** Run SQLite VACUUM */
  vacuum(): string {
    return this.keyCmds.vacuum();
  }

  /** Get/set autovacuum */
  autovacuum(enabled?: boolean): boolean | string {
    return this.keyCmds.autovacuum(enabled);
  }

  /** Get detailed key info */
  keyinfo(key: string) {
    return this.keyCmds.keyinfo(key);
  }
}

// Default export
export default Redlite;
