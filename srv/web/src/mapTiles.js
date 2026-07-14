import { apiUrl } from "./api.js";

const MAGIC = "SGTILE1\0";
const RECORD_SIZE = 72;
const SPECTRAL_CLASSES = ["UNKNOWN", "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D"];
const sharedTileCache = new Map();

async function maybeDecompress(buffer) {
  const bytes = new Uint8Array(buffer);
  if (bytes[0] !== 0x1f || bytes[1] !== 0x8b) return buffer;
  if (typeof DecompressionStream === "undefined") {
    throw new Error("This browser cannot decompress Spacegate map tiles.");
  }
  const stream = new Blob([buffer]).stream().pipeThrough(new DecompressionStream("gzip"));
  return new Response(stream).arrayBuffer();
}

function readUint64(view, offset) {
  const value = view.getBigUint64(offset, true);
  return value <= BigInt(Number.MAX_SAFE_INTEGER) ? Number(value) : value.toString();
}

export async function decodeMapTile(input) {
  const buffer = await maybeDecompress(input);
  const view = new DataView(buffer);
  const decoder = new TextDecoder();
  const magic = decoder.decode(new Uint8Array(buffer, 0, 8));
  if (magic !== MAGIC) throw new Error(`Unsupported Spacegate tile magic: ${JSON.stringify(magic)}`);
  const headerLength = view.getUint32(8, true);
  const header = JSON.parse(decoder.decode(new Uint8Array(buffer, 12, headerLength)));
  if (header.schema_version !== "spacegate_map_tile_v1" || header.record_size !== RECORD_SIZE) {
    throw new Error(`Unsupported Spacegate tile schema: ${header.schema_version}`);
  }
  const recordStart = 12 + headerLength;
  const stringStart = recordStart + header.emitted_count * header.record_size;
  const strings = new Uint8Array(buffer, stringStart, header.string_bytes);
  const systems = [];
  for (let index = 0; index < header.emitted_count; index += 1) {
    const offset = recordStart + index * header.record_size;
    const nameAt = (refOffset) => {
      const stringOffset = view.getUint32(offset + refOffset, true);
      const stringLength = view.getUint16(offset + refOffset + 4, true);
      return decoder.decode(strings.subarray(stringOffset, stringOffset + stringLength));
    };
    const displayNames = {
      public_full: nameAt(32),
      astronomer_abbrev: nameAt(38),
      catalog_compact: nameAt(44),
      source_technical: nameAt(50),
    };
    const keyOffset = view.getUint32(offset + 56, true);
    const keyLength = view.getUint16(offset + 60, true);
    const flags = view.getUint8(offset + 67);
    systems.push({
      system_id: readUint64(view, offset),
      x_helio_ly: header.origin_ly[0] + view.getFloat32(offset + 8, true),
      y_helio_ly: header.origin_ly[1] + view.getFloat32(offset + 12, true),
      z_helio_ly: header.origin_ly[2] + view.getFloat32(offset + 16, true),
      dist_ly: view.getFloat32(offset + 20, true),
      coolness_score: view.getFloat32(offset + 24, true),
      coolness_rank: view.getUint32(offset + 28, true) || null,
      display_name: displayNames.public_full,
      display_names: displayNames,
      system_name: displayNames.public_full,
      stable_object_key: decoder.decode(strings.subarray(keyOffset, keyOffset + keyLength)),
      star_count: view.getUint16(offset + 62, true),
      planet_count: view.getUint16(offset + 64, true),
      dominant_spectral_class: SPECTRAL_CLASSES[view.getUint8(offset + 66)] || "UNKNOWN",
      has_habitable_candidate: Boolean(flags & 1),
      sampled_lod: Boolean(flags & 4),
      max_star_teff_k: view.getUint32(offset + 68, true) || null,
      tile_id: header.tile_id,
    });
  }
  return { header, systems };
}

async function fetchJson(path, signal) {
  const response = await fetch(apiUrl(path), { signal });
  if (!response.ok) throw new Error(`Map tile metadata failed: ${response.status}`);
  return response.json();
}

export async function fetchMapTileIndex(signal) {
  return fetchJson("/map-tiles/index.json", signal);
}

export async function fetchMapTileManifest(radiusLy, signal) {
  return fetchJson(`/map-tiles/radius-${radiusLy}/manifest.json`, signal);
}

function tileCenter(tile) {
  return tile.origin_ly || [0, 0, 0];
}

function containsPosition(tile, position) {
  if (!position) return false;
  return position.every((value, axis) => (
    value >= Number(tile.bounds_min_ly?.[axis]) && value <= Number(tile.bounds_max_ly?.[axis])
  ));
}

export function mapTileRequestPriority(tile, { focus = [0, 0, 0], direction = [0, 0, 0], urgent = null, queuedAt = 0, now = performance.now() } = {}) {
  const center = tileCenter(tile);
  const distance = Math.hypot(center[0] - focus[0], center[1] - focus[1], center[2] - focus[2]);
  const interest = Number(tile.interest?.top_k_mean || tile.interest?.max || 0);
  const sampleBoost = tile.exact ? 0 : 1000 + Number(tile.depth || 0) * 10;
  const directionLength = Math.hypot(...direction);
  const centerDelta = center.map((value, axis) => value - focus[axis]);
  const centerLength = Math.hypot(...centerDelta);
  const directional = directionLength > 0.001 && centerLength > 0.001
    ? centerDelta.reduce((sum, value, axis) => sum + value * direction[axis], 0) / (centerLength * directionLength)
    : 0;
  const ageSeconds = Math.max(0, (now - queuedAt) / 1000);
  const urgentBoost = containsPosition(tile, urgent) ? 2500 : 0;
  return urgentBoost + sampleBoost - distance
    + Math.max(0, directional) * 24
    + Math.min(30, interest * 30)
    + Math.min(20, ageSeconds * 0.5);
}

export class MapTileManager {
  constructor({ concurrency = 6, cacheLimit = 128, retryLimit = 2, nameStyle = "public_full", onBatch, onStatus, fetchImpl = fetch } = {}) {
    this.concurrency = concurrency;
    this.cacheLimit = cacheLimit;
    this.retryLimit = retryLimit;
    this.onBatch = onBatch || (() => {});
    this.onStatus = onStatus || (() => {});
    this.fetchImpl = fetchImpl;
    this.nameStyle = nameStyle;
    this.cache = sharedTileCache;
    this.controllers = new Set();
    this.generation = 0;
    this.focus = [0, 0, 0];
    this.direction = [0, 0, 0];
    this.urgent = null;
    this.stats = {};
  }

  cancel() {
    this.generation += 1;
    for (const controller of this.controllers) controller.abort();
    this.controllers.clear();
  }

  setFocus(positionLy) {
    if (Array.isArray(positionLy) && positionLy.length === 3) this.focus = positionLy.map(Number);
  }

  setMotion(positionLy, directionLy) {
    this.setFocus(positionLy);
    if (Array.isArray(directionLy) && directionLy.length === 3) {
      const next = directionLy.map(Number);
      this.direction = this.direction.map((value, axis) => value * 0.72 + next[axis] * 0.28);
    }
  }

  prioritizePosition(positionLy) {
    this.urgent = Array.isArray(positionLy) && positionLy.length === 3 ? positionLy.map(Number) : null;
  }

  snapshot() {
    return { ...this.stats, cache_entries: this.cache.size, active_requests: this.controllers.size };
  }

  async loadRadius(radiusLy) {
    this.cancel();
    const generation = this.generation;
    const index = await fetchMapTileIndex();
    if (!index.public_radii_ly?.map(Number).includes(Number(radiusLy))) {
      throw new Error(`Map radius ${radiusLy} ly is not enabled by the promoted tile index.`);
    }
    const manifest = await fetchMapTileManifest(radiusLy);
    if (generation !== this.generation) return null;
    if (String(index.build_id) !== String(manifest.build_id)) {
      throw new Error("Map tile index and radius manifest belong to different builds.");
    }
    const deepestSampleDepth = Math.max(...manifest.tiles.filter((tile) => !tile.exact).map((tile) => Number(tile.depth)), 0);
    const queue = manifest.tiles
      .filter((tile) => tile.exact || (!tile.exact && Number(tile.depth) === deepestSampleDepth))
      .map((tile) => ({ tile, queuedAt: performance.now(), attempt: 0 }));
    this.stats = {
      mode: "tiled",
      radius_ly: radiusLy,
      manifest_tiles: manifest.tiles.length,
      queued_tiles: queue.length,
      loaded_tiles: 0,
      failed_tiles: 0,
      cache_hits: 0,
      aborted_tiles: 0,
      encoded_bytes: 0,
      emitted_systems: 0,
      exact_systems: 0,
      sampled_systems: 0,
      manifest_sha256: manifest.manifest_sha256,
      build_id: manifest.build_id,
      coolness_profile_hash: manifest.coolness_profile?.profile_hash || "",
      eligible_systems: Number(manifest.counts?.eligible_systems || 0),
      planet_systems: Number(manifest.counts?.planet_systems || 0),
      multi_star_systems: Number(manifest.counts?.multi_star_systems || 0),
    };
    this.onStatus(this.snapshot());
    const workers = Array.from({ length: Math.min(this.concurrency, queue.length) }, async () => {
      while (queue.length && generation === this.generation) {
        queue.sort((left, right) => mapTileRequestPriority(right.tile, {
          focus: this.focus,
          direction: this.direction,
          urgent: this.urgent,
          queuedAt: right.queuedAt,
        }) - mapTileRequestPriority(left.tile, {
          focus: this.focus,
          direction: this.direction,
          urgent: this.urgent,
          queuedAt: left.queuedAt,
        }));
        const entry = queue.shift();
        const { tile } = entry;
        try {
          let decoded = this.cache.get(tile.sha256);
          if (decoded) {
            this.cache.delete(tile.sha256);
            this.cache.set(tile.sha256, decoded);
            this.stats.cache_hits += 1;
          } else {
            const controller = new AbortController();
            this.controllers.add(controller);
            const response = await this.fetchImpl(apiUrl(tile.url), { signal: controller.signal });
            this.controllers.delete(controller);
            if (!response.ok) throw new Error(`Tile ${tile.tile_id} failed: ${response.status}`);
            decoded = await decodeMapTile(await response.arrayBuffer());
            this.cache.set(tile.sha256, decoded);
            while (this.cache.size > this.cacheLimit) this.cache.delete(this.cache.keys().next().value);
            this.stats.encoded_bytes += Number(tile.compressed_bytes || 0);
          }
          if (generation !== this.generation) return;
          this.stats.loaded_tiles += 1;
          this.stats.emitted_systems += decoded.systems.length;
          if (tile.exact) this.stats.exact_systems += decoded.systems.length;
          else this.stats.sampled_systems += decoded.systems.length;
          this.onBatch(decoded.systems.map((system) => ({
            ...system,
            display_name: system.display_names?.[this.nameStyle] || system.display_name,
            system_name: system.display_names?.[this.nameStyle] || system.system_name,
          })), { ...tile, header: decoded.header });
          this.onStatus(this.snapshot());
        } catch (error) {
          if (error?.name === "AbortError") this.stats.aborted_tiles += 1;
          else if (entry.attempt < this.retryLimit && generation === this.generation) {
            entry.attempt += 1;
            entry.queuedAt = performance.now();
            queue.push(entry);
            this.stats.retried_tiles = Number(this.stats.retried_tiles || 0) + 1;
          } else this.stats.failed_tiles += 1;
          this.onStatus({ ...this.snapshot(), last_error: error?.message || String(error) });
        }
      }
    });
    await Promise.all(workers);
    if (generation !== this.generation) return null;
    this.onStatus({ ...this.snapshot(), complete: true });
    return { index, manifest, stats: this.snapshot() };
  }
}
