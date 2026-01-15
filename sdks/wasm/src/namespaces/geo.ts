/**
 * Geo namespace for Redlite
 */

import type { DbCore } from '../db';
import { KeyType, GeoPosition, GeoSearchResult, GeoSearchOptions } from '../types';

const EARTH_RADIUS_M = 6372797.560856;

export class GeoNamespace {
  constructor(private db: DbCore) {}

  /** GEOADD - Add geo member(s) */
  add(
    key: string,
    members: Record<string, GeoPosition | [number, number]>,
    options?: { nx?: boolean; xx?: boolean; ch?: boolean }
  ): number {
    const keyId = this.db.getOrCreateKey(key, KeyType.Geo);
    let added = 0;
    let changed = 0;

    for (const [member, pos] of Object.entries(members)) {
      const [longitude, latitude] = Array.isArray(pos)
        ? pos
        : [pos.longitude, pos.latitude];

      // Validate coordinates
      if (longitude < -180 || longitude > 180 || latitude < -85.05112878 || latitude > 85.05112878) {
        throw new Error('ERR invalid longitude,latitude pair');
      }

      const geohash = this.encodeGeohash(latitude, longitude);

      const existing = this.db.queryOne<{ member: string }>(
        'SELECT member FROM geo_data WHERE key_id = ? AND member = ?',
        [keyId, member]
      );

      if (existing) {
        if (options?.nx) continue;

        this.db.run(
          `UPDATE geo_data SET longitude = ?, latitude = ?, geohash = ?
           WHERE key_id = ? AND member = ?`,
          [longitude, latitude, geohash, keyId, member]
        );
        changed++;
      } else {
        if (options?.xx) continue;

        this.db.run(
          `INSERT INTO geo_data (key_id, member, longitude, latitude, geohash)
           VALUES (?, ?, ?, ?, ?)`,
          [keyId, member, longitude, latitude, geohash]
        );
        added++;
        changed++;
      }
    }

    this.db.touchKey(keyId);
    return options?.ch ? changed : added;
  }

  /** GEOPOS - Get positions of members */
  pos(key: string, ...members: string[]): (GeoPosition | null)[] {
    const keyId = this.db.getKeyId(key, KeyType.Geo);
    if (keyId === null) return members.map(() => null);

    return members.map((member) => {
      const row = this.db.queryOne<{ longitude: number; latitude: number }>(
        'SELECT longitude, latitude FROM geo_data WHERE key_id = ? AND member = ?',
        [keyId, member]
      );

      if (!row) return null;
      return { longitude: row.longitude, latitude: row.latitude };
    });
  }

  /** GEODIST - Get distance between two members */
  dist(key: string, member1: string, member2: string, unit?: 'm' | 'km' | 'mi' | 'ft'): number | null {
    const positions = this.pos(key, member1, member2);
    if (!positions[0] || !positions[1]) return null;

    const distanceM = this.haversineDistance(
      positions[0].latitude,
      positions[0].longitude,
      positions[1].latitude,
      positions[1].longitude
    );

    return this.convertDistance(distanceM, unit ?? 'm');
  }

  /** GEOHASH - Get geohash of members */
  hash(key: string, ...members: string[]): (string | null)[] {
    const keyId = this.db.getKeyId(key, KeyType.Geo);
    if (keyId === null) return members.map(() => null);

    return members.map((member) => {
      const row = this.db.queryOne<{ geohash: string }>(
        'SELECT geohash FROM geo_data WHERE key_id = ? AND member = ?',
        [keyId, member]
      );
      return row?.geohash ?? null;
    });
  }

  /** GEOSEARCH - Search for members */
  search(key: string, options: GeoSearchOptions): GeoSearchResult[] {
    const keyId = this.db.getKeyId(key, KeyType.Geo);
    if (keyId === null) return [];

    // Get center point
    let centerLon: number;
    let centerLat: number;

    if (options.member) {
      const pos = this.pos(key, options.member)[0];
      if (!pos) return [];
      centerLon = pos.longitude;
      centerLat = pos.latitude;
    } else if (options.longitude !== undefined && options.latitude !== undefined) {
      centerLon = options.longitude;
      centerLat = options.latitude;
    } else {
      throw new Error('ERR must specify FROMMEMBER or FROMLONLAT');
    }

    // Get all members
    const rows = this.db.query<{
      member: string;
      longitude: number;
      latitude: number;
      geohash: string;
    }>(
      'SELECT member, longitude, latitude, geohash FROM geo_data WHERE key_id = ?',
      [keyId]
    );

    // Filter and calculate distances
    const unit = options.unit ?? 'm';
    const results: GeoSearchResult[] = [];

    for (const row of rows) {
      const distanceM = this.haversineDistance(
        centerLat,
        centerLon,
        row.latitude,
        row.longitude
      );

      const distance = this.convertDistance(distanceM, unit);

      // Check if within bounds
      if (options.radius !== undefined) {
        if (distance > options.radius) continue;
      } else if (options.width !== undefined && options.height !== undefined) {
        // Box search - check if within rectangle
        const widthM = this.convertToMeters(options.width, unit);
        const heightM = this.convertToMeters(options.height, unit);

        const deltaLon = Math.abs(row.longitude - centerLon);
        const deltaLat = Math.abs(row.latitude - centerLat);

        const lonDistM = deltaLon * (Math.PI / 180) * EARTH_RADIUS_M * Math.cos(centerLat * Math.PI / 180);
        const latDistM = deltaLat * (Math.PI / 180) * EARTH_RADIUS_M;

        if (lonDistM > widthM / 2 || latDistM > heightM / 2) continue;
      }

      const result: GeoSearchResult = { member: row.member };

      if (options.withDist) {
        result.distance = distance;
      }
      if (options.withCoord) {
        result.coordinates = { longitude: row.longitude, latitude: row.latitude };
      }
      if (options.withHash) {
        result.geohash = row.geohash;
      }

      results.push(result);
    }

    // Sort
    if (options.sort) {
      results.sort((a, b) => {
        const distA = a.distance ?? this.haversineDistance(centerLat, centerLon, a.coordinates!.latitude, a.coordinates!.longitude);
        const distB = b.distance ?? this.haversineDistance(centerLat, centerLon, b.coordinates!.latitude, b.coordinates!.longitude);
        return options.sort === 'ASC' ? distA - distB : distB - distA;
      });
    }

    // Limit
    if (options.count !== undefined) {
      return results.slice(0, options.count);
    }

    return results;
  }

  /** GEOSEARCHSTORE - Store search results */
  searchstore(
    dest: string,
    src: string,
    options: GeoSearchOptions & { storedist?: boolean }
  ): number {
    const results = this.search(src, { ...options, withDist: true, withCoord: true });

    // Store as sorted set (by distance) or geo set
    this.db.deleteKey(dest);

    if (options.storedist) {
      // Store as sorted set with distance as score
      const destKeyId = this.db.getOrCreateKey(dest, KeyType.ZSet);
      for (const result of results) {
        const memberBytes = new TextEncoder().encode(result.member);
        this.db.run(
          'INSERT INTO zsets (key_id, member, score) VALUES (?, ?, ?)',
          [destKeyId, memberBytes, result.distance ?? 0]
        );
      }
    } else {
      // Store as geo set
      const members: Record<string, GeoPosition> = {};
      for (const result of results) {
        if (result.coordinates) {
          members[result.member] = result.coordinates;
        }
      }
      this.add(dest, members);
    }

    return results.length;
  }

  /** Calculate haversine distance in meters */
  private haversineDistance(lat1: number, lon1: number, lat2: number, lon2: number): number {
    const toRad = (deg: number) => deg * Math.PI / 180;

    const dLat = toRad(lat2 - lat1);
    const dLon = toRad(lon2 - lon1);

    const a =
      Math.sin(dLat / 2) * Math.sin(dLat / 2) +
      Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
      Math.sin(dLon / 2) * Math.sin(dLon / 2);

    const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

    return EARTH_RADIUS_M * c;
  }

  /** Convert distance from meters */
  private convertDistance(meters: number, unit: 'm' | 'km' | 'mi' | 'ft'): number {
    switch (unit) {
      case 'km': return meters / 1000;
      case 'mi': return meters / 1609.344;
      case 'ft': return meters / 0.3048;
      default: return meters;
    }
  }

  /** Convert distance to meters */
  private convertToMeters(value: number, unit: 'm' | 'km' | 'mi' | 'ft'): number {
    switch (unit) {
      case 'km': return value * 1000;
      case 'mi': return value * 1609.344;
      case 'ft': return value * 0.3048;
      default: return value;
    }
  }

  /** Encode coordinates to geohash */
  private encodeGeohash(lat: number, lon: number, precision: number = 11): string {
    const base32 = '0123456789bcdefghjkmnpqrstuvwxyz';
    let minLat = -90, maxLat = 90;
    let minLon = -180, maxLon = 180;
    let hash = '';
    let bit = 0;
    let ch = 0;
    let isLon = true;

    while (hash.length < precision) {
      if (isLon) {
        const mid = (minLon + maxLon) / 2;
        if (lon > mid) {
          ch |= (1 << (4 - bit));
          minLon = mid;
        } else {
          maxLon = mid;
        }
      } else {
        const mid = (minLat + maxLat) / 2;
        if (lat > mid) {
          ch |= (1 << (4 - bit));
          minLat = mid;
        } else {
          maxLat = mid;
        }
      }

      isLon = !isLon;
      bit++;

      if (bit === 5) {
        hash += base32[ch];
        bit = 0;
        ch = 0;
      }
    }

    return hash;
  }
}
