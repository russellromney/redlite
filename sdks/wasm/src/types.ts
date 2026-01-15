/**
 * Type definitions for Redlite WASM SDK
 */

/** Key types stored in the database */
export enum KeyType {
  String = 0,
  List = 1,
  Set = 2,
  ZSet = 3,
  Hash = 4,
  Stream = 5,
  Geo = 6,
  Vector = 7,
}

/** Options for SET command */
export interface SetOptions {
  /** Expire after N seconds */
  ex?: number;
  /** Expire after N milliseconds */
  px?: number;
  /** Expire at Unix timestamp (seconds) */
  exat?: number;
  /** Expire at Unix timestamp (milliseconds) */
  pxat?: number;
  /** Only set if key doesn't exist */
  nx?: boolean;
  /** Only set if key exists */
  xx?: boolean;
  /** Keep existing TTL */
  keepttl?: boolean;
  /** Return old value */
  get?: boolean;
}

/** Options for GETEX command */
export interface GetExOptions {
  ex?: number;
  px?: number;
  exat?: number;
  pxat?: number;
  persist?: boolean;
}

/** Sorted set member with score */
export interface ZMember {
  member: string;
  score: number;
}

/** Stream entry */
export interface StreamEntry {
  id: string;
  fields: Record<string, string>;
}

/** Stream ID parts */
export interface StreamId {
  ms: number;
  seq: number;
}

/** History entry */
export interface HistoryEntry {
  version: number;
  operation: string;
  timestamp: number;
  data: unknown;
  expireAt?: number;
}

/** History configuration */
export interface HistoryConfig {
  level: 'global' | 'database' | 'key';
  target: string;
  enabled: boolean;
  retentionType?: 'unlimited' | 'time' | 'count';
  retentionValue?: number;
}

/** Full-text search result */
export interface FtsResult {
  key: string;
  rank: number;
  snippet?: string;
}

/** Full-text search stats */
export interface FtsStats {
  documentsIndexed: number;
  settingsCount: number;
}

/** Vector search result */
export interface VectorSearchResult {
  element: string;
  distance: number;
  attributes?: Record<string, unknown>;
}

/** Vector set info */
export interface VectorSetInfo {
  dimensions: number;
  count: number;
  quantization: string;
}

/** Geo position */
export interface GeoPosition {
  longitude: number;
  latitude: number;
}

/** Geo search result */
export interface GeoSearchResult {
  member: string;
  distance?: number;
  coordinates?: GeoPosition;
  geohash?: string;
}

/** Geo search options */
export interface GeoSearchOptions {
  member?: string;
  longitude?: number;
  latitude?: number;
  radius?: number;
  width?: number;
  height?: number;
  unit?: 'm' | 'km' | 'mi' | 'ft';
  sort?: 'ASC' | 'DESC';
  count?: number;
  withCoord?: boolean;
  withDist?: boolean;
  withHash?: boolean;
}

/** Key info */
export interface KeyInfo {
  type: string;
  ttl: number;
  encoding: string;
  size: number;
  version: number;
  createdAt: number;
  updatedAt: number;
}

/** Scan result */
export interface ScanResult<T> {
  cursor: number;
  items: T[];
}

/** Retention configuration for history */
export interface Retention {
  type: 'unlimited' | 'time' | 'count';
  value?: number;
}

/** Distance metric for vectors */
export type DistanceMetric = 'cosine' | 'euclidean' | 'ip';

/** RediSearch field type */
export type FtFieldType = 'TEXT' | 'NUMERIC' | 'TAG' | 'GEO' | 'VECTOR';

/** RediSearch index field */
export interface FtField {
  name: string;
  type: FtFieldType;
  sortable?: boolean;
  noindex?: boolean;
  nostem?: boolean;
  weight?: number;
  separator?: string;
}

/** RediSearch index info */
export interface FtIndexInfo {
  name: string;
  onType: 'HASH' | 'JSON';
  prefixes: string[];
  schema: FtField[];
  numDocs: number;
}

/** RediSearch search result */
export interface FtSearchResult {
  total: number;
  docs: Array<{
    id: string;
    score?: number;
    fields: Record<string, string>;
  }>;
}

/** Consumer group info */
export interface ConsumerGroupInfo {
  name: string;
  consumers: number;
  pending: number;
  lastDeliveredId: string;
}

/** Consumer info */
export interface ConsumerInfo {
  name: string;
  pending: number;
  idle: number;
}

/** Pending entry */
export interface PendingEntry {
  id: string;
  consumer: string;
  idle: number;
  deliveryCount: number;
}
