from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

from .stellar_classification import spectral_class_from_type

import duckdb


NARRATION_GENERATOR_VERSION = "system_narration_deterministic_v1"
NARRATION_SCHEMA_VERSION = "system_narrative_blocks_v1"


def _text(value: Any, fallback: str = "Unknown") -> str:
    value_text = str(value or "").strip()
    return value_text or fallback


def _number(value: Any, digits: int = 1) -> Optional[str]:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not numeric == numeric:
        return None
    if abs(numeric - round(numeric)) < 10 ** (-(digits + 1)):
        return f"{round(numeric):,}"
    return f"{numeric:,.{digits}f}".rstrip("0").rstrip(".")


def _display_name(system: Dict[str, Any]) -> str:
    return _text(system.get("display_name") or system.get("system_name") or system.get("stable_object_key"), "This system")


def _spectral_tokens(stars: List[Dict[str, Any]]) -> List[str]:
    tokens: List[str] = []
    for star in stars:
        raw = _text(star.get("spectral_class") or star.get("spectral_type_raw"), "")
        if not raw:
            continue
        letter = spectral_class_from_type(raw)
        if letter and letter not in tokens:
            tokens.append(letter)
    return tokens[:6]


def _catalog_summary(system: Dict[str, Any]) -> str:
    catalogs = system.get("evidence_catalogs") or system.get("catalogs") or []
    if isinstance(catalogs, str):
        catalogs = [catalogs]
    clean = [str(item).strip() for item in catalogs if str(item or "").strip()]
    if not clean:
        return "the currently served Spacegate catalog build"
    return ", ".join(clean[:4])


def _infrared_summary(infrared_evidence: Dict[str, Any]) -> Dict[str, Any]:
    summary = infrared_evidence.get("summary") if isinstance(infrared_evidence, dict) else {}
    if not isinstance(summary, dict):
        summary = {}
    return summary


def _block(
    *,
    system: Dict[str, Any],
    block_kind: str,
    title: str,
    body_text: str,
    evidence_inputs: Dict[str, Any],
    concept_slugs: Optional[List[str]] = None,
) -> Dict[str, Any]:
    return {
        "system_id": system.get("system_id"),
        "stable_object_key": system.get("stable_object_key"),
        "block_kind": block_kind,
        "title": title,
        "body_text": body_text,
        "body_markdown": body_text,
        "generation_method": "deterministic_template",
        "generator_version": NARRATION_GENERATOR_VERSION,
        "schema_version": NARRATION_SCHEMA_VERSION,
        "layer": "disc",
        "status": "deterministic_fallback",
        "provenance_status": "derived_presentation",
        "evidence_inputs_json": evidence_inputs,
        "concept_slugs": concept_slugs or [],
    }


def generate_system_narrative_blocks(
    *,
    system: Dict[str, Any],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    hierarchy: Optional[Dict[str, Any]],
    infrared_evidence: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    name = _display_name(system)
    star_count = int(system.get("star_count") or len(stars) or 0)
    planet_count = int(system.get("planet_count") or len(planets) or 0)
    subsystem_count = int(((hierarchy or {}).get("counts") or {}).get("subsystems") or 0)
    orbit_edge_count = int(((hierarchy or {}).get("counts") or {}).get("orbit_edges") or 0)
    distance_text = _number(system.get("dist_ly") or system.get("distance_ly"), 1)
    spectral_tokens = _spectral_tokens(stars)
    spectral_text = ", ".join(spectral_tokens) if spectral_tokens else "incomplete public classification"
    nice_planet_count = int(system.get("coolness_nice_planet_count") or 0)
    infrared = _infrared_summary(infrared_evidence or {})
    infrared_matches = int(infrared.get("match_count") or 0)
    infrared_catalogs = infrared.get("catalog_counts") or {}
    catalog_summary = _catalog_summary(system)
    star_phrase = f"{star_count} stellar member" + ("" if star_count == 1 else "s")
    planet_phrase = f"{planet_count} confirmed planet" + ("" if planet_count == 1 else "s")

    architecture_bits = [star_phrase]
    if subsystem_count > 0:
        architecture_bits.append(f"{subsystem_count} modeled subsystem" + ("" if subsystem_count == 1 else "s"))
    if planet_count > 0:
        architecture_bits.append(planet_phrase)
    architecture = ", ".join(architecture_bits)

    blocks = [
        _block(
            system=system,
            block_kind="what_you_are_looking_at",
            title="What You’re Looking At",
            body_text=(
                f"{name} is {f'about {distance_text} light-years from Sol' if distance_text else 'a system in the current public Spacegate build'}. "
                f"Spacegate currently presents it as {architecture}. Its visible stellar mix is {spectral_text}, based on source fields and derived display support where available."
            ),
            evidence_inputs={
                "star_count": star_count,
                "planet_count": planet_count,
                "subsystem_count": subsystem_count,
                "distance_ly": system.get("dist_ly") or system.get("distance_ly"),
                "spectral_tokens": spectral_tokens,
            },
            concept_slugs=["spectral-class", "multistar-systems"],
        ),
        _block(
            system=system,
            block_kind="why_this_system_matters",
            title="Why This System Matters",
            body_text=(
                f"{name} is useful for exploration because "
                + (
                    "it hosts linked planets, so the simulation can connect real worlds to their stellar environment."
                    if planet_count > 0
                    else "it helps fill in the nearby stellar neighborhood, including quieter systems that shape what local space is actually like."
                )
                + (
                    " Its multi-star structure also matters: companion stars can reshape or disrupt simple planet-building and habitable-zone stories."
                    if star_count > 1 or subsystem_count > 0
                    else ""
                )
                + (
                    " At least one planet triggers a broad habitable-zone signal; that is a screening clue, not a habitability claim."
                    if nice_planet_count > 0 or system.get("has_habitable_candidate")
                    else ""
                )
            ),
            evidence_inputs={
                "planet_count": planet_count,
                "star_count": star_count,
                "subsystem_count": subsystem_count,
                "coolness_score": system.get("coolness_score"),
                "coolness_nice_planet_count": nice_planet_count,
            },
            concept_slugs=["habitable-zone", "planetary-systems", "multistar-systems"],
        ),
        _block(
            system=system,
            block_kind="infrared_view",
            title="Infrared View",
            body_text=(
                "The WISE/AllWISE panel shows observational infrared survey imagery, not an artist impression. "
                "In the false-color preview, W1 is mapped toward blue, W2 toward green, and W3 toward red, making cooler stars, brown dwarfs, dust, and crowded infrared backgrounds easier to investigate. "
                + (
                    f"Spacegate has {infrared_matches} matched infrared evidence row"
                    f"{'' if infrared_matches == 1 else 's'} for this system from {', '.join(sorted(infrared_catalogs.keys()))}."
                    if infrared_matches > 0 and isinstance(infrared_catalogs, dict) and infrared_catalogs
                    else "No targeted WISE evidence match is currently attached to this system in the public ARM support tables."
                )
            ),
            evidence_inputs={
                "infrared_match_count": infrared_matches,
                "infrared_catalog_counts": infrared_catalogs,
                "infrared_policy": infrared.get("policy"),
            },
            concept_slugs=["infrared-astronomy", "wise", "false-color-imagery", "brown-dwarfs"],
        ),
        _block(
            system=system,
            block_kind="what_we_know",
            title="What We Know",
            body_text=(
                f"The current public record is assembled from {catalog_summary}. "
                f"The API links {star_phrase} and {planet_phrase}, and the hierarchy graph exposes {orbit_edge_count} relationship or orbit edge"
                f"{'' if orbit_edge_count == 1 else 's'} where the build can support them. Stronger source values are preferred over derived presentation support."
            ),
            evidence_inputs={
                "catalog_summary": catalog_summary,
                "orbit_edge_count": orbit_edge_count,
                "star_count": star_count,
                "planet_count": planet_count,
            },
            concept_slugs=["evidence", "orbits", "catalogs"],
        ),
        _block(
            system=system,
            block_kind="what_remains_uncertain",
            title="What Remains Uncertain",
            body_text=(
                "A missing value in Spacegate is not evidence that the physical property is absent. "
                "Some orbits, classifications, masses, radii, and temperatures may be incomplete, derived, or represented by clearly labeled simulation assumptions until better source evidence is materialized and reviewed."
            ),
            evidence_inputs={
                "missing_class_count": sum(1 for star in stars if not _text(star.get("spectral_class") or star.get("spectral_type_raw"), "")),
                "simulation_assumption_count": system.get("simulation_assumption_count") or system.get("assumption_count"),
            },
            concept_slugs=["uncertainty", "provenance"],
        ),
        _block(
            system=system,
            block_kind="further_exploration",
            title="Further Exploration",
            body_text=(
                "Use the System Simulation scale modes to move between readable structure and more scale-aware views, then open the hierarchy and evidence sections for the source trail. "
                "Future concept pages will turn tags such as spectral class, habitable zone, infrared astronomy, and multistar systems into deeper guided explanations with representative systems."
            ),
            evidence_inputs={
                "concept_hooks": ["spectral-class", "habitable-zone", "infrared-astronomy", "multistar-systems"],
            },
            concept_slugs=["spectral-class", "habitable-zone", "infrared-astronomy", "multistar-systems"],
        ),
    ]
    return blocks


def fetch_disc_system_narrative_blocks(
    *,
    disc_db_path: Optional[str],
    system_id: Optional[int],
    stable_object_key: Optional[str],
) -> List[Dict[str, Any]]:
    if not disc_db_path:
        return []
    path = Path(disc_db_path)
    if not path.exists():
        return []
    try:
        con = duckdb.connect(str(path), read_only=True)
    except Exception:
        return []
    try:
        exists = con.execute(
            """
            SELECT count(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = 'system_narrative_blocks'
            """
        ).fetchone()[0]
        if not exists:
            return []
        rows = con.execute(
            """
            SELECT
              system_id,
              stable_object_key,
              block_kind,
              title,
              body_markdown,
              body_text,
              generation_method,
              generator_version,
              evidence_inputs_json,
              provenance_status,
              status,
              concept_slugs_json,
              created_at
            FROM system_narrative_blocks
            WHERE (system_id = ? OR stable_object_key = ?)
              AND coalesce(status, '') IN ('published', 'reviewed', 'deterministic')
            ORDER BY block_rank, block_kind
            """,
            [system_id, stable_object_key],
        ).fetchall()
        columns = [desc[0] for desc in con.description]
    except Exception:
        return []
    finally:
        con.close()

    blocks: List[Dict[str, Any]] = []
    for raw in rows:
        row = dict(zip(columns, raw))
        for key in ("evidence_inputs_json", "concept_slugs_json"):
            value = row.get(key)
            if isinstance(value, str) and value:
                try:
                    row[key] = json.loads(value)
                except json.JSONDecodeError:
                    row[key] = value
        row["schema_version"] = NARRATION_SCHEMA_VERSION
        row["layer"] = "disc"
        if "concept_slugs_json" in row:
            row["concept_slugs"] = row.pop("concept_slugs_json") or []
        blocks.append(row)
    return blocks


def system_narrative_blocks(
    *,
    disc_db_path: Optional[str],
    system: Dict[str, Any],
    stars: List[Dict[str, Any]],
    planets: List[Dict[str, Any]],
    hierarchy: Optional[Dict[str, Any]],
    infrared_evidence: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    reviewed_blocks = fetch_disc_system_narrative_blocks(
        disc_db_path=disc_db_path,
        system_id=system.get("system_id"),
        stable_object_key=system.get("stable_object_key"),
    )
    if reviewed_blocks:
        return reviewed_blocks
    return generate_system_narrative_blocks(
        system=system,
        stars=stars,
        planets=planets,
        hierarchy=hierarchy,
        infrared_evidence=infrared_evidence,
    )
