# E7 Scientific A/B Review - 2026-07-23

## Status

Candidate `e7_73349c253a411945c246d459_public` passes the clean compiler,
independent verification, isolated reproduction, public-build, tiled-map,
bounded-scene, API, and browser gates. It is stored on Photon's internal
`/data` volume but is not promoted or served. Operator acceptance, local atomic
promotion, rollback, and re-promotion remain open.

## Immutable Inputs

- clean science: `2d084ee3c5939878259793bb`
- clean CORE: `21971e59527ccf5c729b7cab`
- clean ARM: `45d996e094020fa52d8a3f82`
- clean DISC: `d43e93eeb9c09c9e7445c9d6`
- runtime bundle: `73349c253a411945c246d459`
- public candidate: `e7_73349c253a411945c246d459_public`

## Inventory

| Surface | Served reference | Candidate | Delta |
|---|---:|---:|---:|
| Systems | 5,869,091 | 5,869,091 | 0 |
| Stars | 5,874,636 | 5,874,636 | 0 |
| Planets | 6,311 | 6,311 | 0 |
| Aliases | 1,026,480 | 1,026,480 | 0 |
| Extended objects | 18,277 | 18,277 | 0 |
| System search terms | 6,694,190 | 12,768,410 | +6,074,220 |

The public slice trims zero systems, stars, planets, aliases, or search terms.
Canonical planet inventory is unchanged; TESS candidates and negative evidence
remain outside canonical planet links.

## Classification

The clean hierarchy has 147 candidate-only MSC inferred leaves and no
reference-only leaves. Across shared leaves, 349,153 display classes change,
including 892 `UNKNOWN` to known transitions and 192 known to `UNKNOWN`
transitions.

All 192 known-to-`UNKNOWN` rows have independent case-significant component-
scope collision evidence. They remain adjudication deferrals. The prior
48-object UltracoolSheet migration tail is closed through an exact release-
scoped source-native identifier contract. Gaia-white-dwarf regressions,
unaccounted transitions, duplicate leaf keys, and nonmissing rows without
lineage are zero.

The source-native contract accounts 76 permanent UltracoolSheet identifiers as
51 accepted and 25 missing-current-release. It projects 60 direct facts for 51
stars. Forty-nine become selected display evidence: 41 infrared and 8 optical.
No fuzzy name match, row-position equivalence, identity creation, or containment
creation is allowed.

## Runtime And Map

- CORE/ARM/DISC and canonical Parquet products reproduce independently.
- Exact tiled membership covers 10,209 / 206,913 / 1,820,142 / 5,319,825
  systems at 100 / 250 / 500 / 1,000 ly with no missing, extra, duplicate,
  public-name, representative-class, or badge mismatch.
- All four timestamp-free tile manifests and 3,686 content-addressed payloads
  reproduce exactly.
- Twenty-four priority scenes generate cold with zero failures and all reuse on
  a warm pass.
- API integration and strict search/detail/hierarchy/scene verification pass.
- Desktop and mobile map tests pass 12 applicable cases with four intended
  mobile skips, including nonblank exact/progressive radii and 4K Bright mode.

## Review Decision

The candidate is scientifically preferable to the served reference: it retains
canonical inventory, materially enriches search, closes the owned
UltracoolSheet evidence gap, removes untraceable fallbacks, and makes remaining
classification uncertainty explicit. No unresolved finding currently requires
another scientific rebuild before local promotion.

Promotion should proceed only after explicit operator acceptance. The rollback
drill must retain `20260717T0614Z_f452835_side`, switch atomically to the new
candidate, verify local containers and APIs, roll back, verify again, and then
re-promote. No antiproton deployment is authorized by this review.
