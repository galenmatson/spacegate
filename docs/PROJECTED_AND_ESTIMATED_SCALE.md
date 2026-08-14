# Projected and Estimated Scale

Status: design reservation for a milestone after M8.3e.3b. No public renderer
mode or control is enabled by this document.

## Purpose

Physical Orbits is reserved for orbital geometry supported by an accepted
semi-major axis or a defensible Kepler derivation from an accepted period and
complete applicable endpoint masses. Many visual multiples do not meet that
contract even though useful angular separation evidence exists. Spacegate may
eventually present that evidence through two separately named and versioned
contracts without weakening Physical Orbits.

## Projected Separation

Projected Separation is a measured two dimensional sky plane snapshot. It may
use an angular separation, position angle, observation epoch, and an applicable
distance estimate to express a projected linear separation.

Required fields:

- exact system, component, and endpoint scope;
- angular separation and position angle with source units and uncertainty;
- observation epoch or an explicit missing epoch state;
- distance evidence, reference frame, and propagation policy;
- projected separation and uncertainty with derivation lineage;
- source, release, record, citation, and quality flags;
- a clear statement that the true three dimensional separation is not known.

This view must not draw a fitted ellipse, imply semi-major axis, infer a period,
or animate a measured snapshot as a known orbit. Multiple epochs may be shown
as measured positions or motion tracks when their identities and frames are
coherent.

Reserved contract vocabulary:

- `scale_mode = projected_separation`
- `scale_state = measured_sky_plane_snapshot`
- `projected_separation_policy_version`
- `observation_epoch`
- `angular_separation_arcsec`
- `position_angle_deg`
- `projected_separation_au`

## Estimated Scale

Estimated Scale is a statistical posterior or bounded interval for physical
separation or semi-major axis. It is not a measurement of the missing line of
sight geometry and is not a known orbit.

Required fields:

- exact endpoint scope and the measured inputs used by the model;
- population definition and applicability gates;
- prior family, calibration sample, model version, and reproducible parameters;
- posterior median or another declared estimator plus credible interval;
- sensitivity to distance, projection geometry, selection effects, and source
  uncertainty;
- confidence and explicit rejection reasons;
- evidence and derivation lineage sufficient to reproduce the result.

Estimated geometry must use visibly uncertain presentation. It may not silently
receive inclination, eccentricity, phase, ascending node, or an animated
Keplerian ellipse. Any motion is explanatory presentation and must be labeled as
such.

Reserved contract vocabulary:

- `scale_mode = estimated_scale`
- `scale_state = statistical_interval`
- `estimated_scale_policy_version`
- `population_prior_id`
- `posterior_quantity`
- `posterior_median_au`
- `credible_interval_au`

## Activation Gate

Before either mode is exposed, a future milestone must:

1. evaluate source coverage, retrieval cost, cadence, and archive usage limits;
2. define exact scope and epoch reconciliation rules;
3. review uncertainty visualization with positive and negative controls;
4. measure false interpretation risk in desktop and mobile usability tests;
5. add deterministic compiler, API, scene, accessibility, and regression gates;
6. receive explicit scientific and product review.

Projected Separation and Estimated Scale remain DISC presentation contracts
backed by ARM evidence and derivations. They do not alter CORE membership,
canonical hierarchy, or accepted orbital solutions.
