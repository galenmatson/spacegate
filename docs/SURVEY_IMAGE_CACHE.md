# Survey Image Cache Contract

Status: v1, implemented for IRSA WISE/AllWISE in M8.3e.3.

## Boundary

Survey images are observational presentation products. They are not canonical
objects, selected stellar facts, or simulation geometry. Cached metadata keeps
the provider, release, bands, cutout center and size, source URLs, retrieval
time, attribution, and license note needed to explain and regenerate a preview.
Raw survey mirroring remains outside this runtime cache.

The machine policy is `config/survey_image_cache_v1.json`.

## Budget

- All providers share one 4 GiB hard cap on the public edge.
- WISE currently has a 4 GiB provider quota because it is the only admitted
  provider. Adding another provider requires dividing the shared cap; it does
  not create a second independent allowance.
- The public edge must retain at least 15 GiB free after release and cache
  planning.
- Eviction is oldest accessed, unpinned file first. Survey products are
  re-fetchable and must never displace served or rollback artifacts.

Default layout:

```text
$SPACEGATE_STATE_DIR/cache/survey_images/irsa_wise_allwise/
```

`SPACEGATE_SURVEY_IMAGE_CACHE_DIR` changes the shared root.
`SPACEGATE_WISE_IMAGE_CACHE_DIR` remains an exact WISE provider-root override.
Bulk mode uses:

```text
/mnt/space/spacegate/cache/survey_images/irsa_wise_allwise/
```

## Provider Protection

WISE uses at most two concurrent remote transfers with at least 150 ms between
request starts. A remote failure receives two bounded retries with exponential
backoff. Exhausted metadata failures are negatively cached for 15 minutes;
preview failures for 60 minutes. A repeated request during that interval gets
an explicit `503` and `Retry-After` without contacting IRSA.

Requests for the same metadata or preview key coalesce behind one builder.
Followers recheck the completed cache entry instead of repeating upstream work.
Writes use a temporary sibling and atomic rename; failed previews are removed.

## Browser Scheduling and Validation

The System Page schedules metadata with `requestIdleCallback`, bounded by a
2.4-second timeout. The PNG is not requested until the image panel is within
about 1.5 current viewport heights. Navigating away aborts an unfinished
metadata fetch and emits a bounded client abandonment event. Preview start,
load, failure, and abandonment events are counted without collecting hardware
or user identity.

Preview responses include a content SHA-256 `ETag`, one-day browser freshness,
and seven-day stale-while-revalidate permission. Matching `If-None-Match`
requests return `304`.

## Metrics

Bounded cache status reports expose provider and shared bytes, cap, evictions,
metadata/preview hits and misses, coalesced waits, negative-cache activity,
remote requests, retries, latency, bytes, failures, and client lifecycle
events. Host filesystem paths are never returned.

Metrics are process-local operational counters in v1. Durable fleet telemetry
is deferred until Spacegate has a general metrics sink; request logs and cache
metadata remain available for incident review.

## Admission of New Providers

A provider must declare source identity, attribution, license, bands or energy
range, cache key inputs, quota, priority, concurrency, request interval,
negative TTL, and validator behavior. Provider failure must degrade only its
own panel. No provider may become a required dependency for System Detail.

