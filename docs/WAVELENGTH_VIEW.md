# Wavelength View and Survey Imagery Contract

Status: accepted architecture for M8.3g; provider research and implementation
remain pending.

## Purpose

Wavelength View will replace the System Page's one-off WISE panel with a
reusable way to inspect the same field of sky across curated observing bands.
It is a presentation and observation-product surface. It does not turn image
pixels, background emission, or survey coverage into canonical detections.

The first public release should compare visible and infrared views. Later
releases may add ultraviolet, X-ray, gamma-ray, submillimeter, microwave, and
radio products where the survey coverage and angular resolution make the view
scientifically useful.

## Public Experience

Every view must preserve, where the source data permit:

- a shared sky center, orientation, target marker, and comparable field of view;
- the survey, release, instrument or mission, bands, observation epoch,
  angular scale, and attribution;
- an explicit explanation of natural color, false color, or single-band
  intensity mapping;
- a distinction between image coverage, field context, a catalog association,
  and a reviewed detection of the selected object;
- target-marker propagation to the survey epoch when accepted proper-motion
  evidence makes that defensible.

Pan and zoom should preserve correspondence between adjacent views. An optional
comparison slider may reveal two aligned layers without implying that they have
the same angular resolution, point-spread function, sensitivity, or epoch.

## Wavelength Rail

A radio-to-gamma control is feasible, but it must not behave like a continuous
measurement when the underlying products are discrete surveys.

- Arrange curated stops on a logarithmic wavelength or frequency axis.
- Label each stop with its observational domain and survey or instrument.
- Snap selection to real source products; show unavailable intervals as gaps.
- Crossfade only between available neighboring presentation layers, and label
  the transition as a visual comparison rather than interpolated science.
- Allow wavelength-aware fields of view. Degree-scale gamma-ray context and
  arcminute-scale optical stellar fields should not be forced into one scale.
- Keep keyboard, touch, and screen-reader operation first-class.

The rail is therefore a navigation device over selected observations, not a
synthetic hyperspectral cube.

## Hybrid Delivery Architecture

Wavelength View uses three deliberately separate delivery tiers:

1. **Cached preview:** a bounded, attributed image is the fast and resilient
   default on a System Page. It may be generated from a provider's cutout or
   HiPS service and cached by immutable source/render identity.
2. **Interactive survey view:** Aladin Lite and HiPS tiles are loaded only after
   visitor activation. This supports pan, zoom, layer switching, and local
   comparison without making every page view an upstream tile session.
3. **Scientific product:** FITS and other calibrated products are indexed and
   obtained on demand by later observation labs. Bulk products are not stored
   in the public hot path by default.

Spacegate must remain usable when an imagery provider is slow or unavailable.
No external archive, Photon service, Proton service, or NFS mount may be a
required dependency for the primary public System Page.

## Initial Source Candidates

The following candidates establish the research set; inclusion is not approval.
Every release still requires a recorded provider, license, attribution, cache,
and capacity review.

| Domain | Initial candidate | Intended role | Delivery candidate |
| --- | --- | --- | --- |
| Visible | DSS2 color through CDS HiPS | All-sky baseline and comparison | HiPS plus bounded preview |
| Visible | Deeper regional surveys | Best available view where applicability is explicit | Survey-specific HiPS or cutout |
| Near infrared | 2MASS color | Uniform near-IR baseline | CDS/IRSA HiPS or bounded preview |
| Mid infrared | AllWISE color | Uniform mid-IR field context | CDS/IRSA HiPS or cached composite |
| Ultraviolet | GALEX | UV field context within coverage | HiPS or bounded archive cutout |
| X-ray | ROSAT all-sky; reviewed pointed products later | High-energy context, not automatic association | Coarse HiPS or bounded preview |
| Gamma ray | Fermi all-sky products | Large-scale high-energy context | Coarse HiPS |
| Radio | Provider matrix pending | Curated continuum context | HiPS preferred where available |

Useful official discovery and delivery contracts include:

- CDS Aladin Lite API: <https://aladin.cds.unistra.fr/AladinLite/doc/API/>
- CDS HiPS inventory: <https://aladin.cds.unistra.fr/hips/list>
- IRSA Finder Chart API: <https://irsa.ipac.caltech.edu/onlinehelp/finderchart/finderchart/api.html>
- IRSA image APIs: <https://irsa.ipac.caltech.edu/docs/program_interface/api_images.html>
- NASA SkyView: <https://skyview.gsfc.nasa.gov/current/cgi/titlepage.pl>
- IRSA data-use terms: <https://irsa.ipac.caltech.edu/data_use_terms.html>

The current Spacegate WISE renderer composes selected monochrome WISE bands.
That remains scientifically defensible when the mapping is useful and fully
labeled. It is not automatically the most efficient public delivery path:
provider-generated color products or HiPS color tiles should be evaluated for
the common view, while custom composites remain available when they communicate
a deliberate band mapping unavailable from the standard product.

## Provider-Neighbour Policy

Scientific archives are not assumed to be unlimited public application CDNs.
Before enabling a provider, record:

- supported access protocol and exact request shape;
- product and typical response size;
- documented rate or fair-use guidance;
- attribution, citation, license, and redistribution rules;
- whether transformed previews and raw products may be cached;
- timeout, retry, negative-cache, and stale-cache behavior;
- a responsible concurrency and request-rate ceiling;
- an operational contact and a traffic threshold for consultation.

Spacegate must coalesce identical work, cancel abandoned requests where the
provider permits it, apply exponential backoff and circuit breaking, and never
issue automatic unbounded prefetch. Interactive tiles are fetched only after
explicit visitor activation. If measured demand approaches an undocumented or
reviewed upstream budget, automatic expansion stops until Spacegate has
contacted the provider, arranged accommodation, mirrored redistributable
products, or placed them behind its own object storage/CDN.

## Cache and Storage Policy

- Begin with the current 4-GiB aggregate edge preview-cache ceiling.
- Use one cache budget with provider soft quotas, not additive large caches for
  every wavelength.
- Key cached products by provider, survey/release, band mapping, sky geometry,
  render policy, and output encoding.
- Cache transformed previews and compact metadata at the edge; keep raw FITS
  products on demand unless a reviewed observation-lab policy says otherwise.
- Record hits, misses, same-key coalescing, upstream latency, response bytes,
  negative-cache use, eviction, failure, and stale-if-error service.
- Preserve at least the public-release disk reserve required by
  `docs/PUBLIC_DEPLOYMENT.md` and `docs/RETENTION.md` before warming imagery.

## Research and Acceptance Gates

Before implementation, produce a machine-readable provider matrix covering
coverage, wavelengths, resolution, epochs, access method, response size,
attribution, license, cacheability, capacity guidance, and failure behavior.
Measure representative high-proper-motion, crowded, bright or saturated,
ordinary, ultracool, compact, and extended-object fields.

The first release is acceptable when:

- visible and infrared views remain aligned, attributed, and scientifically
  qualified on desktop and mobile;
- no background image is presented as object-level detection evidence;
- cold, warm, failure, cancellation, cache, and provider-rate behavior is
  measured and bounded;
- an upstream outage does not block the primary System Page;
- the wavelength rail exposes real products and real gaps without simulating
  false spectral continuity;
- projected public traffic fits both the edge disk budget and reviewed provider
  expectations.
