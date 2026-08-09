# Simulation Initial Rate Review, 2026-08-09

The M8.3e.3 evaluator inspected 7,725 frozen priority scenes from Public Read
build `e7_24cb15211f430a37f199f462_full_public`.

| Measure | Count | Minimum | Median | P95 | Maximum |
|---|---:|---:|---:|---:|---:|
| Accepted planet periods | 2,744 | 0.112 d | 11.909 d | 4,218 d | 402,000,000 d |
| Accepted stellar periods | 9,071 | 0.125 d | 219,150 d | 192,852,000 d | 63,188,250,000 d |
| Scenes with a planet anchor | 1,946 | | | | |
| Scenes with a stellar anchor | 4,630 | | | | |

The five-second fastest-planet candidate most often selects `1x` (777 systems)
or `5x` (434), but spans the full manual range. A sixty-second top-level
stellar candidate selects `10000x` for 3,393 of 4,630 eligible systems and can
still leave the widest systems visually slow. The shortest stellar candidate
is distributed across every rate.

Decision: retain `1x` for a newly opened system. Carry a visitor's explicit
rate only while inspecting the same stable system in the current browser
session. Do not activate automatic rates until representative planet-only,
compact multiple, wide hierarchy, and mixed planet/multiple scenes pass visual
review without sacrificing comprehension.

The machine report contains system-level anchors and candidate rates. Assumed,
missing, unknown, ambiguous, and quarantined periods are excluded.
