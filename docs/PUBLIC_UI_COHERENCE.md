# Public UI and Simulation Coherence

Status: M8.3e.3 implementation checkpoint, 2026-08-09.

## Purpose

Catalog, Map, Peek, Explorer, and System Page are views of the same public
dataset. They may present different amounts of information, but moving between
them should not feel like leaving one application for another. This contract is
presentation only. It does not select scientific values or change CORE, ARM,
DISC, or RIM.

## Surface Navigation

- Catalog and Map are peer destinations. Wide layouts show a two-part control;
  narrow layouts retain the same accessible names and expose compact actions
  with tooltips.
- The CoolStars brand on a System Page returns to the clean Catalog at
  `/search`.
- A System Page opened from Map retains an explicit `Back to 3D map` action and
  a bounded return token. The normal Map peer remains available separately.
- Selected-system `Explore` remains an inspection action, not site navigation.

## Search and Drill State

Map Search Results remain mounted while Peek or Explorer is open. The results
surface becomes hidden and inert, so it cannot cover the simulation or receive
keyboard input, but its DOM scroll position, exact query, filters, sort,
pagination, and result objects remain intact. Closing the drill restores that
same surface. Explicitly closing Results still clears the result state.

## Simulation Presentation Session

`simulation_presentation_session_v1` stores one active system's presentation
choices in browser session storage:

- scale mode;
- speed multiplier;
- orbit visibility;
- habitable and formation line visibility; and
- label visibility.

Peek, Explorer, fullscreen remounts, and System Detail read the same state when
the stable system ID matches. A different system starts from current global
defaults, including `1x` speed. Running/paused state, camera state, elapsed
simulation days, orbital epoch, hovered objects, and pinned readouts never
carry between surfaces or reloads.

## System Page Width

Only System routes raise the application envelope from 1,200 to 1,600 CSS
pixels. Narrative paragraphs and image explanations remain bounded to 76
characters. Responsive rules continue to collapse the simulation, narrative,
and hierarchy layouts rather than scaling type with viewport width.

## Initial Simulation Rate Review

Automatic rate selection remains disabled. The checked-in evaluator accepts a
period only when the rendered field has a positive value and is not assumed,
missing, unknown, ambiguous, or quarantined. It measures:

- the fastest accepted planet orbit targeting about five real seconds; and
- both the shortest and widest-member accepted stellar orbit targeting about
  sixty real seconds.

The August 9 report covers 7,725 frozen priority scenes, 2,744 accepted planet
periods, and 9,071 accepted stellar periods. The large dispersion, especially
among top-level stellar orbits, confirms that a single automatic rule would
often make either inner planets unreadably fast or outer hierarchy motion still
imperceptible. New systems therefore keep predictable `1x`; the measured
candidates remain visual-review evidence for a later policy.

Machine report:

```text
/data/spacegate/state/reports/public_ui_coherence/20260809/scene_initial_rate_policy.json
```

Its clean rerun SHA-256 is
`4db9d68e9dcf6ec37c0283bbbd814f5c38acbe649561bcdcdee9b957526b77aa`.

Regenerate it with:

```bash
.venv/bin/python scripts/report_simulation_rate_policy.py \
  --scene-dir /data/spacegate/state/cache/simulation_scenes/<build_id> \
  --output /data/spacegate/state/reports/public_ui_coherence/<run>/scene_initial_rate_policy.json
```

## Verification

Unit tests cover same-system state, new-system reset, negative cache behavior,
request coalescing, and rate eligibility. Playwright covers preserved Map
results, cross-surface simulation controls, the 1,600-pixel envelope, peer
navigation, and desktop/mobile WISE scheduling.

Cold/generated and warm/hit WISE validator reports are retained at:

```text
/data/spacegate/state/reports/public_ui_coherence/20260809/survey_image_cache_cold_verification.json
/data/spacegate/state/reports/public_ui_coherence/20260809/survey_image_cache_verification.json
```
