import { apiUrl } from "./api.js";

const MAGIC = "SGTILE1\0";
const RECORD_SIZE = 72;
const SPECTRAL_CLASSES = [
  "UNKNOWN", "O", "B", "A", "F", "G", "K", "M", "L", "T", "Y", "D",
  "WR", "WD", "NS", "PULSAR", "MAGNETAR", "BLACK HOLE",
];
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
  if (!["spacegate_map_tile_v1", "spacegate_map_tile_v2"].includes(header.schema_version) || header.record_size !== RECORD_SIZE) {
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
    const spectralClass = SPECTRAL_CLASSES[view.getUint8(offset + 66)] || "UNKNOWN";
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
      representative_stellar_class: spectralClass,
      // Retained for v1 artifacts and presentation code during the tile-v2 rollout.
      dominant_spectral_class: spectralClass,
      has_habitable_candidate: Boolean(flags & 1),
      sampled_lod: Boolean(flags & 4),
      max_star_teff_k: view.getUint32(offset + 68, true) || null,
      tile_id: header.tile_id,
    });
  }
  return { header, systems };
}

async function fetchJson(path, signal, fetchImpl = fetch) {
  const response = await fetchImpl(apiUrl(path), { signal });
  if (!response.ok) throw new Error(`Map tile metadata failed: ${response.status}`);
  return response.json();
}

export async function fetchMapTileIndex(signal, fetchImpl) {
  return fetchJson("/map-tiles/index.json", signal, fetchImpl);
}

export async function fetchMapTileManifest(radiusLy, signal, fetchImpl) {
  return fetchJson(`/map-tiles/radius-${radiusLy}/manifest.json`, signal, fetchImpl);
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

export function tileIntersectsSphere(tile, position, radiusLy) {
  if (!Array.isArray(position) || position.length !== 3 || !(Number(radiusLy) >= 0)) return false;
  let distanceSq = 0;
  for (let axis = 0; axis < 3; axis += 1) {
    const value = Number(position[axis] || 0);
    const minimum = Number(tile.bounds_min_ly?.[axis] || 0);
    const maximum = Number(tile.bounds_max_ly?.[axis] || 0);
    const nearest = Math.max(minimum, Math.min(maximum, value));
    distanceSq += (value - nearest) ** 2;
  }
  return distanceSq <= Number(radiusLy) ** 2;
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

export function progressiveSampleStages(manifest) {
  const byDepth = new Map();
  for (const tile of manifest?.tiles || []) {
    if (tile.exact) continue;
    const depth = Number(tile.depth || 0);
    if (!byDepth.has(depth)) byDepth.set(depth, []);
    byDepth.get(depth).push(tile);
  }
  return Array.from(byDepth.entries())
    .sort(([left], [right]) => left - right)
    .map(([depth, tiles]) => ({ depth, tiles }));
}

export class MapTileManager {
  constructor({ concurrency = 6, cacheLimit = 128, retryLimit = 2, nameStyle = "public_full", onBatch, onReplace, onStatus, fetchImpl = fetch } = {}) {
    this.concurrency = concurrency;
    this.cacheLimit = cacheLimit;
    this.retryLimit = retryLimit;
    this.onBatch = onBatch || (() => {});
    this.onReplace = onReplace || (() => {});
    this.onStatus = onStatus || (() => {});
    this.fetchImpl = (...fetchArgs) => fetchImpl(...fetchArgs);
    this.nameStyle = nameStyle;
    this.cache = sharedTileCache;
    this.controllers = new Set();
    this.detailControllers = new Set();
    this.generation = 0;
    this.detailGeneration = 0;
    this.focus = [0, 0, 0];
    this.direction = [0, 0, 0];
    this.urgent = null;
    this.stats = {};
    this.manifest = null;
  }

  cancel() {
    this.generation += 1;
    this.detailGeneration += 1;
    for (const controller of this.controllers) controller.abort();
    for (const controller of this.detailControllers) controller.abort();
    this.controllers.clear();
    this.detailControllers.clear();
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
    const index = await fetchMapTileIndex(undefined, this.fetchImpl);
    if (!index.public_radii_ly?.map(Number).includes(Number(radiusLy))) {
      throw new Error(`Map radius ${radiusLy} ly is not enabled by the promoted tile index.`);
    }
    const manifest = await fetchMapTileManifest(radiusLy, undefined, this.fetchImpl);
    if (generation !== this.generation) return null;
    if (String(index.build_id) !== String(manifest.build_id)) {
      throw new Error("Map tile index and radius manifest belong to different builds.");
    }
    this.manifest = manifest;
    const progressive = Number(radiusLy) > 250;
    const sampleStages = progressiveSampleStages(manifest);
    const deepestSampleDepth = Math.max(...sampleStages.map((stage) => stage.depth), 0);
    const shallowTiles = manifest.tiles.filter((tile) => (
      tile.exact || (!tile.exact && Number(tile.depth) === deepestSampleDepth)
    ));
    const stages = progressive
      ? sampleStages
      : [{ depth: null, tiles: shallowTiles }];
    const queuedTiles = stages.reduce((sum, stage) => sum + stage.tiles.length, 0);
    this.stats = {
      mode: "tiled",
      radius_ly: radiusLy,
      manifest_tiles: manifest.tiles.length,
      queued_tiles: queuedTiles,
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
      manifest_ready: true,
      progressive,
      transport_policy: progressive ? "progressive_samples_local_exact_v1" : "complete_exact_v1",
      sample_depths: sampleStages.map((stage) => stage.depth),
    };
    this.onStatus(this.snapshot());
    let previousStage = null;
    for (let stageIndex = 0; stageIndex < stages.length; stageIndex += 1) {
      const stage = stages[stageIndex];
      const queue = stage.tiles.map((tile) => ({ tile, queuedAt: performance.now(), attempt: 0 }));
      const failedBefore = this.stats.failed_tiles;
      this.stats.stage_depth = stage.depth;
      this.stats.stage_index = stageIndex;
      this.stats.stage_tiles = stage.tiles.length;
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
              let response;
              try {
                response = await this.fetchImpl(apiUrl(tile.url), { signal: controller.signal });
              } finally {
                this.controllers.delete(controller);
              }
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
            })), { ...tile, header: decoded.header, stage_depth: stage.depth });
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
      const stageSucceeded = this.stats.failed_tiles === failedBefore;
      if (progressive && stageIndex === 0) this.stats.coarse_complete = stageSucceeded;
      if (
        progressive
        && stageSucceeded
        && previousStage?.depth === 2
        && stage.depth === 3
      ) {
        this.onReplace({
          remove_tile_ids: previousStage.tiles.map((tile) => tile.tile_id),
          replacement_depth: stage.depth,
          reason: "complete_sample_frontier",
        });
        this.stats.replaced_sample_tiles = previousStage.tiles.length;
      }
      this.stats.completed_stage_depth = stage.depth;
      this.onStatus(this.snapshot());
      previousStage = stage;
    }
    if (generation !== this.generation) return null;
    this.stats.complete = true;
    this.onStatus(this.snapshot());
    return { index, manifest, stats: this.snapshot() };
  }

  async loadDetailBubble(positionLy, radiusLy) {
    if (!this.manifest || !Array.isArray(positionLy) || positionLy.length !== 3 || !(Number(radiusLy) > 0)) {
      return [];
    }
    this.detailGeneration += 1;
    const detailGeneration = this.detailGeneration;
    const generation = this.generation;
    for (const controller of this.detailControllers) controller.abort();
    this.detailControllers.clear();
    const tiles = this.manifest.tiles.filter((tile) => (
      tile.exact && tileIntersectsSphere(tile, positionLy, radiusLy)
    ));
    const queue = [...tiles];
    const systems = [];
    let cacheHits = 0;
    let encodedBytes = 0;
    let firstError = null;
    const workers = Array.from({ length: Math.min(this.concurrency, queue.length) }, async () => {
      while (queue.length && generation === this.generation && detailGeneration === this.detailGeneration) {
        const tile = queue.shift();
        try {
          let decoded = this.cache.get(tile.sha256);
          if (decoded) {
            this.cache.delete(tile.sha256);
            this.cache.set(tile.sha256, decoded);
            cacheHits += 1;
          } else {
            const controller = new AbortController();
            this.detailControllers.add(controller);
            let response;
            try {
              response = await this.fetchImpl(apiUrl(tile.url), { signal: controller.signal });
            } finally {
              this.detailControllers.delete(controller);
            }
            if (!response.ok) throw new Error(`Detail tile ${tile.tile_id} failed: ${response.status}`);
            decoded = await decodeMapTile(await response.arrayBuffer());
            this.cache.set(tile.sha256, decoded);
            while (this.cache.size > this.cacheLimit) this.cache.delete(this.cache.keys().next().value);
            encodedBytes += Number(tile.compressed_bytes || 0);
          }
          if (generation !== this.generation || detailGeneration !== this.detailGeneration) return;
          systems.push(...decoded.systems.map((system) => ({
            ...system,
            display_name: system.display_names?.[this.nameStyle] || system.display_name,
            system_name: system.display_names?.[this.nameStyle] || system.system_name,
          })));
        } catch (error) {
          if (error?.name !== "AbortError") firstError ||= error;
        }
      }
    });
    await Promise.all(workers);
    if (generation !== this.generation || detailGeneration !== this.detailGeneration) return null;
    this.stats.detail_center_ly = positionLy.map(Number);
    this.stats.detail_radius_ly = Number(radiusLy);
    this.stats.detail_tiles = tiles.length;
    this.stats.detail_systems_considered = systems.length;
    this.stats.detail_cache_hits = Number(this.stats.detail_cache_hits || 0) + cacheHits;
    this.stats.detail_encoded_bytes = Number(this.stats.detail_encoded_bytes || 0) + encodedBytes;
    if (firstError) {
      this.stats.last_error = firstError.message || String(firstError);
      this.onStatus(this.snapshot());
      throw firstError;
    }
    this.onStatus(this.snapshot());
    return systems;
  }
}
