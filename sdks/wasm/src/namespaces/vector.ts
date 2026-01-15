/**
 * Vector namespace for Redlite (Redis 8 compatible)
 */

import type { DbCore } from '../db';
import { KeyType, VectorSearchResult, VectorSetInfo, DistanceMetric } from '../types';

export class VectorNamespace {
  constructor(private db: DbCore) {}

  /** VADD - Add vector(s) to a vector set */
  add(
    key: string,
    element: string,
    vector: number[],
    options?: {
      attributes?: Record<string, unknown>;
      quantization?: 'NOQUANT' | 'Q8' | 'BF16';
    }
  ): boolean {
    const keyId = this.db.getOrCreateKey(key, KeyType.Vector);

    // Validate dimensions against existing vectors
    const existing = this.db.queryOne<{ dimensions: number }>(
      'SELECT dimensions FROM vector_sets WHERE key_id = ? LIMIT 1',
      [keyId]
    );

    if (existing && existing.dimensions !== vector.length) {
      throw new Error(`ERR vector dimensions mismatch: expected ${existing.dimensions}, got ${vector.length}`);
    }

    // Convert vector to binary (Float32Array)
    const embedding = new Float32Array(vector);
    const embeddingBytes = new Uint8Array(embedding.buffer);

    const attributes = options?.attributes ? JSON.stringify(options.attributes) : null;
    const quantization = options?.quantization ?? 'NOQUANT';

    // Upsert vector
    this.db.run(
      `INSERT INTO vector_sets (key_id, element, embedding, dimensions, quantization, attributes)
       VALUES (?, ?, ?, ?, ?, ?)
       ON CONFLICT(key_id, element) DO UPDATE SET
         embedding = excluded.embedding,
         quantization = excluded.quantization,
         attributes = excluded.attributes`,
      [keyId, element, embeddingBytes, vector.length, quantization, attributes]
    );

    this.db.touchKey(keyId);
    return true;
  }

  /** VREM - Remove element(s) from vector set */
  rem(key: string, ...elements: string[]): number {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return 0;

    let removed = 0;
    for (const element of elements) {
      this.db.run(
        'DELETE FROM vector_sets WHERE key_id = ? AND element = ?',
        [keyId, element]
      );
      const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
      removed += changes?.changes ?? 0;
    }

    // Delete key if empty
    const remaining = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM vector_sets WHERE key_id = ?',
      [keyId]
    );
    if (remaining?.count === 0) {
      this.db.deleteKey(key);
    }

    return removed;
  }

  /** VSIM - Find similar vectors */
  search(
    key: string,
    queryVector: number[],
    options?: {
      count?: number;
      metric?: DistanceMetric;
      filter?: string;
    }
  ): VectorSearchResult[] {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return [];

    const count = options?.count ?? 10;
    const metric = options?.metric ?? 'cosine';

    // Get all vectors (brute force search - for production, use approximate NN)
    const rows = this.db.query<{
      element: string;
      embedding: Uint8Array;
      attributes: string | null;
    }>(
      'SELECT element, embedding, attributes FROM vector_sets WHERE key_id = ?',
      [keyId]
    );

    // Calculate distances
    const results: VectorSearchResult[] = rows.map((row) => {
      const embedding = new Float32Array(row.embedding.buffer);
      const distance = this.calculateDistance(queryVector, Array.from(embedding), metric);

      return {
        element: row.element,
        distance,
        attributes: row.attributes ? JSON.parse(row.attributes) : undefined,
      };
    });

    // Sort by distance (lower is better for all metrics)
    results.sort((a, b) => a.distance - b.distance);

    return results.slice(0, count);
  }

  /** VCARD - Get number of vectors in set */
  card(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ count: number }>(
      'SELECT COUNT(*) as count FROM vector_sets WHERE key_id = ?',
      [keyId]
    );
    return result?.count ?? 0;
  }

  /** VDIM - Get vector dimensions */
  dim(key: string): number {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return 0;

    const result = this.db.queryOne<{ dimensions: number }>(
      'SELECT dimensions FROM vector_sets WHERE key_id = ? LIMIT 1',
      [keyId]
    );
    return result?.dimensions ?? 0;
  }

  /** VEMB / VGET - Get vector embedding */
  emb(key: string, element: string): number[] | null {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return null;

    const row = this.db.queryOne<{ embedding: Uint8Array }>(
      'SELECT embedding FROM vector_sets WHERE key_id = ? AND element = ?',
      [keyId, element]
    );

    if (!row) return null;

    const embedding = new Float32Array(row.embedding.buffer);
    return Array.from(embedding);
  }

  /** VGETATTR - Get element attributes */
  getattr(key: string, element: string): Record<string, unknown> | null {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return null;

    const row = this.db.queryOne<{ attributes: string | null }>(
      'SELECT attributes FROM vector_sets WHERE key_id = ? AND element = ?',
      [keyId, element]
    );

    if (!row || !row.attributes) return null;
    return JSON.parse(row.attributes);
  }

  /** VSETATTR - Set element attributes */
  setattr(key: string, element: string, attributes: Record<string, unknown>): boolean {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return false;

    this.db.run(
      'UPDATE vector_sets SET attributes = ? WHERE key_id = ? AND element = ?',
      [JSON.stringify(attributes), keyId, element]
    );

    const changes = this.db.queryOne<{ changes: number }>('SELECT changes() as changes');
    return (changes?.changes ?? 0) > 0;
  }

  /** VINFO - Get vector set info */
  info(key: string): VectorSetInfo | null {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return null;

    const row = this.db.queryOne<{
      dimensions: number;
      quantization: string;
      count: number;
    }>(
      `SELECT dimensions, quantization, COUNT(*) as count
       FROM vector_sets WHERE key_id = ? GROUP BY dimensions, quantization`,
      [keyId]
    );

    if (!row) return null;

    return {
      dimensions: row.dimensions,
      count: row.count,
      quantization: row.quantization,
    };
  }

  /** VRANDMEMBER - Get random element(s) */
  randmember(key: string, count?: number): string | string[] | null {
    const keyId = this.db.getKeyId(key, KeyType.Vector);
    if (keyId === null) return count ? [] : null;

    const n = count ?? 1;
    const rows = this.db.query<{ element: string }>(
      'SELECT element FROM vector_sets WHERE key_id = ? ORDER BY RANDOM() LIMIT ?',
      [keyId, n]
    );

    if (rows.length === 0) return count ? [] : null;

    const elements = rows.map((r) => r.element);
    return count !== undefined ? elements : elements[0];
  }

  /** Calculate distance between two vectors */
  private calculateDistance(a: number[], b: number[], metric: DistanceMetric): number {
    if (a.length !== b.length) {
      throw new Error('Vector dimensions must match');
    }

    switch (metric) {
      case 'cosine':
        return this.cosineDistance(a, b);
      case 'euclidean':
        return this.euclideanDistance(a, b);
      case 'ip':
        return this.innerProductDistance(a, b);
      default:
        return this.cosineDistance(a, b);
    }
  }

  private cosineDistance(a: number[], b: number[]): number {
    let dotProduct = 0;
    let normA = 0;
    let normB = 0;

    for (let i = 0; i < a.length; i++) {
      dotProduct += a[i] * b[i];
      normA += a[i] * a[i];
      normB += b[i] * b[i];
    }

    const similarity = dotProduct / (Math.sqrt(normA) * Math.sqrt(normB));
    return 1 - similarity; // Convert similarity to distance
  }

  private euclideanDistance(a: number[], b: number[]): number {
    let sum = 0;
    for (let i = 0; i < a.length; i++) {
      const diff = a[i] - b[i];
      sum += diff * diff;
    }
    return Math.sqrt(sum);
  }

  private innerProductDistance(a: number[], b: number[]): number {
    let dotProduct = 0;
    for (let i = 0; i < a.length; i++) {
      dotProduct += a[i] * b[i];
    }
    // Negate because higher IP = more similar
    return -dotProduct;
  }
}
