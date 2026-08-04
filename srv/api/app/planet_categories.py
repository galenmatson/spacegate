from __future__ import annotations

from collections.abc import Iterable


PLANET_CATEGORY_BITS = {
    "hot_giant": 1,
    "temperate_giant": 2,
    "cold_giant": 4,
    "hot_terrestrial": 8,
    "temperate_terrestrial": 16,
    "cold_terrestrial": 32,
    "hot_neptune": 64,
    "temperate_neptune": 128,
    "cold_neptune": 256,
    "unclassified_planet": 512,
}

PLANET_CATEGORY_ALIASES = {
    "hot_jupiter": "hot_giant",
    "temperate_jupiter": "temperate_giant",
    "cold_jupiter": "cold_giant",
}

SUPPORTED_PLANET_CATEGORIES = frozenset(
    (*PLANET_CATEGORY_BITS, *PLANET_CATEGORY_ALIASES)
)


def parse_planet_categories(value: str | None) -> list[str]:
    categories: list[str] = []
    seen: set[str] = set()
    for raw in str(value or "").split(","):
        category = raw.strip().lower()
        category = PLANET_CATEGORY_ALIASES.get(category, category)
        if category and category not in seen:
            categories.append(category)
            seen.add(category)
    return categories


def planet_category_mask(categories: Iterable[str]) -> int:
    mask = 0
    for category in categories:
        mask |= PLANET_CATEGORY_BITS[category]
    return mask


def planet_category_eligibility_sql(alias: str = "p") -> str:
    return (
        f"COALESCE({alias}.planet_status, 'confirmed') = 'confirmed' "
        f"AND COALESCE({alias}.is_default_visible, TRUE) "
        f"AND NOT COALESCE({alias}.is_tombstoned, FALSE) "
        f"AND COALESCE({alias}.planet_size_mass_class, '') <> 'subplanet'"
    )


def planet_composition_proxy_sql(alias: str = "p") -> str:
    return f"""
        CASE
          WHEN {alias}.radius_earth IS NOT NULL AND {alias}.radius_earth <= 2.0 THEN 'terrestrial'
          WHEN {alias}.radius_earth IS NOT NULL AND {alias}.radius_earth >= 6.0 THEN 'giant_or_enveloped'
          WHEN {alias}.radius_earth IS NOT NULL THEN 'neptune_size'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NOT NULL
            AND {alias}.radius_jup * 11.209 <= 2.0 THEN 'terrestrial'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NOT NULL
            AND {alias}.radius_jup * 11.209 >= 6.0 THEN 'giant_or_enveloped'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NOT NULL
            THEN 'neptune_size'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NOT NULL AND {alias}.mass_earth <= 10.0 THEN 'terrestrial'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NOT NULL AND {alias}.mass_earth >= 50.0 THEN 'giant_or_enveloped'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NOT NULL THEN 'neptune_size'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NULL AND {alias}.mass_jup IS NOT NULL
            AND {alias}.mass_jup * 317.83 <= 10.0 THEN 'terrestrial'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NULL AND {alias}.mass_jup IS NOT NULL
            AND {alias}.mass_jup * 317.83 >= 50.0 THEN 'giant_or_enveloped'
          WHEN {alias}.radius_earth IS NULL AND {alias}.radius_jup IS NULL
            AND {alias}.mass_earth IS NULL AND {alias}.mass_jup IS NOT NULL
            THEN 'neptune_size'
          ELSE NULL
        END
    """.strip()


def planet_temperature_proxy_sql(alias: str = "p") -> str:
    return (
        f"COALESCE({alias}.eq_temp_k, CASE WHEN {alias}.insol_earth > 0 "
        f"THEN 278.5 * POW({alias}.insol_earth, 0.25) ELSE NULL END)"
    )


def planet_category_bit_sql(alias: str = "p") -> str:
    composition = planet_composition_proxy_sql(alias)
    temperature = planet_temperature_proxy_sql(alias)
    return f"""
        CASE
          WHEN ({composition}) = 'giant_or_enveloped' AND ({temperature}) > 320.0 THEN 1
          WHEN ({composition}) = 'giant_or_enveloped' AND ({temperature}) >= 200.0 THEN 2
          WHEN ({composition}) = 'giant_or_enveloped' AND ({temperature}) < 200.0 THEN 4
          WHEN ({composition}) = 'terrestrial' AND ({temperature}) > 320.0 THEN 8
          WHEN ({composition}) = 'terrestrial' AND ({temperature}) >= 200.0 THEN 16
          WHEN ({composition}) = 'terrestrial' AND ({temperature}) < 200.0 THEN 32
          WHEN ({composition}) = 'neptune_size' AND ({temperature}) > 320.0 THEN 64
          WHEN ({composition}) = 'neptune_size' AND ({temperature}) >= 200.0 THEN 128
          WHEN ({composition}) = 'neptune_size' AND ({temperature}) < 200.0 THEN 256
          ELSE 512
        END
    """.strip()
