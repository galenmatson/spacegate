import React, { useEffect, useMemo, useState } from "react";
import { Link, Route, Routes, useNavigate, useParams, useSearchParams } from "react-router-dom";
import { fetchSystemDetail, fetchSystems } from "./api.js";

const spectralOptions = ["O", "B", "A", "F", "G", "K", "M", "L", "T", "Y"];

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  if (typeof value === "number") {
    return value.toLocaleString(undefined, { maximumFractionDigits: digits });
  }
  return String(value);
}

function formatText(value) {
  if (value === null || value === undefined || value === "") {
    return "Unknown";
  }
  return String(value);
}

function formatCoordinate(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  return value.toFixed(4);
}

function formatConfidence(value) {
  if (value === null || value === undefined || Number.isNaN(value)) {
    return "Unknown";
  }
  return Number(value).toFixed(2);
}

function starCatalogRecordLink(star) {
  const sourceCatalog = String(star?.provenance?.source_catalog || "").toLowerCase();
  if (sourceCatalog === "athyg") {
    if (star?.gaia_id) {
      return {
        label: "Gaia DR3 record",
        url: `https://vizier.cds.unistra.fr/viz-bin/VizieR-5?-source=I/355/gaiadr3&Source=${encodeURIComponent(String(star.gaia_id))}`,
        note: "ATHYG aggregate source resolved via Gaia ID",
      };
    }
    if (star?.hip_id) {
      return {
        label: "SIMBAD HIP record",
        url: `https://simbad.cds.unistra.fr/simbad/sim-id?Ident=${encodeURIComponent(`HIP ${star.hip_id}`)}`,
        note: "ATHYG aggregate source resolved via HIP ID",
      };
    }
    if (star?.hd_id) {
      return {
        label: "SIMBAD HD record",
        url: `https://simbad.cds.unistra.fr/simbad/sim-id?Ident=${encodeURIComponent(`HD ${star.hd_id}`)}`,
        note: "ATHYG aggregate source resolved via HD ID",
      };
    }
  }
  return null;
}

function planetCatalogRecordLink(planet) {
  const sourceCatalog = String(planet?.provenance?.source_catalog || "").toLowerCase();
  if (sourceCatalog === "nasa_exoplanet_archive" && planet?.planet_name) {
    return {
      label: "NASA Exoplanet Archive record",
      url: `https://exoplanetarchive.ipac.caltech.edu/overview/${encodeURIComponent(String(planet.planet_name))}`,
    };
  }
  return null;
}

function SnapshotVisual({ snapshot, systemName, compact = false }) {
  const hasImage = Boolean(snapshot?.url);
  if (!hasImage) {
    return (
      <div className={`snapshot-fallback ${compact ? "compact" : ""}`}>
        <span>Snapshot pending</span>
        <small>Run the snapshot generator for this build to populate deterministic visuals.</small>
      </div>
    );
  }

  const labelBits = [];
  if (snapshot?.view_type) {
    labelBits.push(String(snapshot.view_type));
  }
  if (snapshot?.params_hash) {
    labelBits.push(String(snapshot.params_hash).slice(0, 8));
  }

  return (
    <figure className={`snapshot-frame ${compact ? "compact" : ""}`}>
      <img src={snapshot.url} alt={`${formatText(systemName)} deterministic system snapshot`} loading="lazy" />
      {labelBits.length > 0 && (
        <figcaption className="snapshot-caption">{labelBits.join(" · ")}</figcaption>
      )}
    </figure>
  );
}

function SnapshotMetadata({ system, snapshot }) {
  const rows = [
    ["System", formatText(system?.system_name)],
    ["Stable key", formatText(system?.stable_object_key)],
    ["Distance", `${formatNumber(system?.dist_ly, 2)} ly`],
    ["Stars", formatNumber(system?.star_count, 0)],
    ["Planets", formatNumber(system?.planet_count, 0)],
    ["View", formatText(snapshot?.view_type)],
    ["Params hash", formatText(snapshot?.params_hash)],
    ["Image size", (snapshot?.width_px && snapshot?.height_px) ? `${snapshot.width_px} x ${snapshot.height_px}` : "Unknown"],
  ];
  return (
    <div className="snapshot-meta" role="note" aria-label="Snapshot metadata">
      <h4>Snapshot Metadata</h4>
      {rows.map(([label, value]) => (
        <div key={label} className="snapshot-meta-row">
          <span className="snapshot-meta-label">{label}</span>
          <code className="snapshot-meta-value">{value}</code>
        </div>
      ))}
    </div>
  );
}

function Layout({ children, headerExtra = null, showSearchLink = true }) {
  return (
    <div className="app">
      <header className="site-header">
        <div>
          <div className="eyebrow">Stellar Data Explorer</div>
          <h1><Link to="/" className="title-link">Spacegate Browser</Link></h1>
          <p>
            Discover and explore nearby systems, stars, and exoplanets.
          </p>
        </div>
        <div className="header-actions">
          {headerExtra}
          {showSearchLink && <Link to="/" className="button ghost">Search</Link>}
        </div>
      </header>
      <main>{children}</main>
    </div>
  );
}

function SearchPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const [query, setQuery] = useState(() => searchParams.get("q") || "");
  const [maxDist, setMaxDist] = useState("");
  const [sort, setSort] = useState("coolness");
  const [spectral, setSpectral] = useState([]);
  const [hasPlanets, setHasPlanets] = useState(false);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [cursor, setCursor] = useState(null);
  const [hasMore, setHasMore] = useState(false);
  const [searchStarted, setSearchStarted] = useState(false);

  const spectralSet = useMemo(() => new Set(spectral), [spectral]);

  const buildParams = (cursorValue) => {
    const params = {};
    if (query.trim()) {
      params.q = query.trim();
    }
    if (maxDist) {
      params.max_dist_ly = maxDist;
    }
    if (spectral.length) {
      params.spectral_class = spectral.join(",");
    }
    if (hasPlanets) {
      params.has_planets = "true";
    }
    params.sort = sort;
    params.limit = "50";
    if (cursorValue) {
      params.cursor = cursorValue;
    }
    return params;
  };

  const runSearch = async (cursorValue, reset = false) => {
    setLoading(true);
    setSearchStarted(true);
    setError("");
    try {
      const data = await fetchSystems(buildParams(cursorValue));
      setHasMore(Boolean(data.has_more));
      setCursor(data.next_cursor || null);
      setResults((prev) => (reset ? data.items : [...prev, ...data.items]));
    } catch (err) {
      setError(err?.message || "Data temporarily unavailable.");
    } finally {
      setLoading(false);
    }
  };

  const onSubmit = (event) => {
    event.preventDefault();
    const q = query.trim();
    if (q) {
      setSearchParams({ q });
    } else {
      setSearchParams({});
    }
    runSearch(null, true);
  };

  useEffect(() => {
    runSearch(null, true);
  }, []);

  const toggleSpectral = (value) => {
    setSpectral((prev) => {
      if (prev.includes(value)) {
        return prev.filter((item) => item !== value);
      }
      return [...prev, value];
    });
  };

  return (
    <Layout
      showSearchLink={false}
      headerExtra={(
        <label className="header-search">
          <span className="sr-only">Search</span>
          <input
            type="text"
            value={query}
            onChange={(event) => setQuery(event.target.value)}
            onKeyDown={(event) => {
              if (event.key === "Enter") {
                event.preventDefault();
                runSearch(null, true);
              }
            }}
            placeholder="Search systems by name, ID, or catalog key..."
            autoFocus
          />
        </label>
      )}
    >
      <section className="search-layout">
        <form className="panel" onSubmit={onSubmit}>
          <label className="field">
            <span>Max distance (ly)</span>
            <input
              type="number"
              min="0"
              step="0.1"
              value={maxDist}
              onChange={(event) => setMaxDist(event.target.value)}
              placeholder="e.g., 50"
            />
          </label>

          <div className="field">
            <span>Spectral classes</span>
            <div className="chip-group">
              {spectralOptions.map((option) => (
                <button
                  type="button"
                  key={option}
                  className={`chip ${spectralSet.has(option) ? "active" : ""}`}
                  onClick={() => toggleSpectral(option)}
                >
                  {option}
                </button>
              ))}
            </div>
          </div>

          <label className="field toggle">
            <input
              type="checkbox"
              checked={hasPlanets}
              onChange={(event) => setHasPlanets(event.target.checked)}
            />
            <span>Has confirmed planets</span>
          </label>

          <label className="field">
            <span>Sort</span>
            <select value={sort} onChange={(event) => setSort(event.target.value)}>
              <option value="coolness">Coolness (top-ranked)</option>
              <option value="name">Name (A-Z)</option>
              <option value="distance">Distance (nearest)</option>
            </select>
          </label>

          <button className="button" type="submit" disabled={loading}>
            {loading ? "Searching..." : "Search"}
          </button>

          {error && (
            <div className="error-box">
              <div>{error}</div>
              <button
                type="button"
                className="button ghost retry"
                onClick={() => runSearch(null, true)}
                disabled={loading}
              >
                Retry
              </button>
            </div>
          )}
        </form>

        <section className="results">
          {loading && results.length === 0 && (
            <div className="empty-state">
              <h2>Loading cool systems...</h2>
              <p>Fetching top-ranked systems by the active coolness profile.</p>
            </div>
          )}

          {!searchStarted && !loading && (
            <div className="empty-state">
              <h2>Start typing to search</h2>
              <p>Enter a system name or set filters to begin browsing.</p>
            </div>
          )}

          {searchStarted && !loading && results.length === 0 && (
            <div className="empty-state">
              <h2>No matches found</h2>
              <p>Try relaxing filters or changing the search terms.</p>
              <p className="muted">Hint: try catalog IDs like HIP ####, HD ####, or Gaia source id.</p>
            </div>
          )}

          {results.length > 0 && (
            <div className="results-list">
              {results.map((item) => (
                <Link
                  key={item.system_id}
                  to={`/systems/${item.system_id}`}
                  className="result-card"
                >
                  <div className="result-shell">
                    <SnapshotVisual snapshot={item.snapshot} systemName={item.system_name} compact />
                    <div className="result-content">
                      <div className="result-header">
                        <div>
                          <h3>{formatText(item.system_name)}</h3>
                          <p className="muted">{item.stable_object_key}</p>
                        </div>
                        <div className="distance">{formatNumber(item.dist_ly, 2)} ly</div>
                      </div>
                      <div className="result-meta">
                        <span>Stars: {formatNumber(item.star_count, 0)}</span>
                        <span>Planets: {formatNumber(item.planet_count, 0)}</span>
                        <span>Spectral: {item.spectral_classes?.length ? item.spectral_classes.join(", ") : "Unknown"}</span>
                      </div>
                      {(item.coolness_rank !== null && item.coolness_rank !== undefined) && (
                        <div className="result-meta">
                          <span>Coolness rank: #{formatNumber(item.coolness_rank, 0)}</span>
                          <span>Coolness score: {formatNumber(item.coolness_score, 2)}</span>
                        </div>
                      )}
                      <div className="result-meta">
                        <span>RA: {formatCoordinate(item.ra_deg)} deg</span>
                        <span>Dec: {formatCoordinate(item.dec_deg)} deg</span>
                      </div>
                      <div className="result-meta">
                        <span>Gaia: {formatText(item.gaia_id)}</span>
                        <span>HIP: {formatText(item.hip_id)}</span>
                        <span>HD: {formatText(item.hd_id)}</span>
                      </div>
                      <div className="result-source">
                        Source: {formatText(item.provenance?.source_catalog)} · {formatText(item.provenance?.source_version)}
                      </div>
                    </div>
                  </div>
                </Link>
              ))}
            </div>
          )}

          {hasMore && (
            <button
              className="button ghost load-more"
              onClick={() => runSearch(cursor, false)}
              disabled={loading}
            >
              {loading ? "Loading..." : "Load more"}
            </button>
          )}
        </section>
      </section>
    </Layout>
  );
}

function ProvenanceBlock({ provenance }) {
  if (!provenance) {
    return null;
  }
  const redistribution =
    provenance.redistribution_ok === true
      ? "Allowed"
      : provenance.redistribution_ok === false
        ? "Restricted"
        : "Unknown";
  return (
    <div className="provenance">
      <div>
        <strong>Source</strong>
        <span>{formatText(provenance.source_catalog)} {formatText(provenance.source_version)}</span>
      </div>
      <div>
        <strong>License</strong>
        <span>{formatText(provenance.license)}</span>
      </div>
      <div>
        <strong>Redistribution</strong>
        <span>
          {redistribution}
          {provenance.redistribution_ok === false && (
            <span className="warning-chip">Restricted</span>
          )}
        </span>
      </div>
      <div>
        <strong>Retrieved</strong>
        <span>{formatText(provenance.retrieved_at)}</span>
      </div>
      <div>
        <strong>Transform</strong>
        <span>{formatText(provenance.transform_version)}</span>
      </div>
      <div>
        <strong>Source URL</strong>
        <span>
          {provenance.source_url ? (
            <a href={String(provenance.source_url)} target="_blank" rel="noreferrer">
              Open source page
            </a>
          ) : (
            "Unknown"
          )}
        </span>
      </div>
      <div>
        <strong>Download URL</strong>
        <span>
          {provenance.source_download_url ? (
            <a href={String(provenance.source_download_url)} target="_blank" rel="noreferrer">
              Open download
            </a>
          ) : (
            "Unknown"
          )}
        </span>
      </div>
    </div>
  );
}

function SystemDetailPage() {
  const { systemId } = useParams();
  const navigate = useNavigate();
  const [quickSearchQuery, setQuickSearchQuery] = React.useState("");
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

  const onQuickSearchSubmit = (event) => {
    event.preventDefault();
    const q = quickSearchQuery.trim();
    if (!q) {
      navigate("/");
      return;
    }
    navigate(`/?q=${encodeURIComponent(q)}`);
  };

  React.useEffect(() => {
    let isActive = true;
    setLoading(true);
    setError("");
    fetchSystemDetail(systemId)
      .then((payload) => {
        if (isActive) {
          setData(payload);
        }
      })
      .catch(() => {
        if (isActive) {
          setError("System not found.");
        }
      })
      .finally(() => {
        if (isActive) {
          setLoading(false);
        }
      });
    return () => {
      isActive = false;
    };
  }, [systemId]);

  if (loading) {
    return (
      <Layout>
        <div className="panel">Loading system details...</div>
      </Layout>
    );
  }

  if (error || !data) {
    return (
      <Layout>
        <div className="panel">
          <h2>System not found</h2>
          <p>{error || "No data returned."}</p>
          <button className="button ghost" onClick={() => navigate("/")}>Back to search</button>
        </div>
      </Layout>
    );
  }

  const { system, stars, planets } = data;

  return (
    <Layout
      headerExtra={(
        <form className="header-search" onSubmit={onQuickSearchSubmit}>
          <span className="sr-only">Search systems</span>
          <input
            type="text"
            value={quickSearchQuery}
            onChange={(event) => setQuickSearchQuery(event.target.value)}
            placeholder="Search systems..."
          />
        </form>
      )}
    >
      <section className="detail">
        <div className="detail-header">
          <div>
            <h2>{formatText(system.system_name)}</h2>
            <p className="muted">{formatText(system.stable_object_key)}</p>
          </div>
          <button className="button ghost" onClick={() => navigate("/")}>Back</button>
        </div>

        <div className="quick-facts">
          <div>
            <strong>Distance</strong>
            <span>{formatNumber(system.dist_ly, 2)} ly</span>
          </div>
          <div>
            <strong>RA / Dec</strong>
            <span>{formatCoordinate(system.ra_deg)} / {formatCoordinate(system.dec_deg)} deg</span>
          </div>
          <div>
            <strong>XYZ (helio)</strong>
            <span>
              {formatCoordinate(system.x_helio_ly)}, {formatCoordinate(system.y_helio_ly)}, {formatCoordinate(system.z_helio_ly)}
            </span>
          </div>
          <div>
            <strong>Stars</strong>
            <span>{formatNumber(system.star_count, 0)}</span>
          </div>
          <div>
            <strong>Planets</strong>
            <span>{formatNumber(system.planet_count, 0)}</span>
          </div>
          <div>
            <strong>Identifiers</strong>
            <span>Gaia {formatText(system.gaia_id)} | HIP {formatText(system.hip_id)} | HD {formatText(system.hd_id)}</span>
          </div>
        </div>

        <section className="panel snapshot-panel">
          <h3>System Snapshot</h3>
          <div className="snapshot-panel-layout">
            <SnapshotMetadata system={system} snapshot={system.snapshot} />
            <SnapshotVisual snapshot={system.snapshot} systemName={system.system_name} />
          </div>
          <p className="muted">Metadata is plain selectable text; the image contains only system visualization.</p>
        </section>

        <section className="panel">
          <h3>Stars</h3>
          {stars.length === 0 && <p className="muted">No star members recorded.</p>}
          {stars.length > 0 && (
            <div className="table">
              {stars.map((star) => (
                <div className="row" key={star.star_id}>
                  {(() => {
                    const record = starCatalogRecordLink(star);
                    return (
                      <>
                        <div>
                          <strong>{formatText(star.star_name)}</strong>
                          <span className="muted">Component {formatText(star.component)}</span>
                        </div>
                        <div>
                          <span>Spectral: {formatText(star.spectral_type_raw)}</span>
                          <span className="muted">
                            Class {formatText(star.spectral_class)} {formatText(star.luminosity_class)} ·
                            Subtype {formatText(star.spectral_subtype)} ·
                            Peculiar {formatText(star.spectral_peculiar)}
                          </span>
                        </div>
                        <div>
                          <span>Distance {formatNumber(star.dist_ly, 2)} ly</span>
                          <span className="muted">Vmag {formatNumber(star.vmag, 2)}</span>
                        </div>
                        <div className="muted">
                          IDs Gaia {formatText(star.gaia_id)} · HIP {formatText(star.hip_id)} · HD {formatText(star.hd_id)}
                        </div>
                        <div className="muted">
                          Source {formatText(star.provenance?.source_catalog)} · {formatText(star.provenance?.source_version)}
                        </div>
                        <div className="muted">
                          Catalog record{" "}
                          {record ? (
                            <a href={record.url} target="_blank" rel="noreferrer">{record.label}</a>
                          ) : (
                            "Unavailable for this source"
                          )}
                          {record?.note ? ` · ${record.note}` : ""}
                        </div>
                      </>
                    );
                  })()}
                </div>
              ))}
            </div>
          )}
        </section>

        <section className="panel">
          <h3>Planets</h3>
          {planets.length === 0 && <p className="muted">No confirmed exoplanets recorded.</p>}
          {planets.length > 0 && (
            <div className="table">
              {planets.map((planet) => (
                <div className="row" key={planet.planet_id}>
                  {(() => {
                    const record = planetCatalogRecordLink(planet);
                    return (
                      <>
                        <div>
                          <strong>{formatText(planet.planet_name)}</strong>
                          <span className="muted">
                            Discovery {formatText(planet.disc_year)} · {formatText(planet.discovery_method)} · {formatText(planet.discovery_facility)}
                          </span>
                          <span className="muted">
                            Telescope {formatText(planet.discovery_telescope)} · Instrument {formatText(planet.discovery_instrument)}
                          </span>
                        </div>
                        <div>
                          <span>Period {formatNumber(planet.orbital_period_days, 2)} d</span>
                          <span className="muted">
                            SMA {formatNumber(planet.semi_major_axis_au, 3)} AU · Eccentricity {formatNumber(planet.eccentricity, 3)}
                          </span>
                        </div>
                        <div>
                          <span>
                            Radius {formatNumber(planet.radius_earth, 2)} Earth / {formatNumber(planet.radius_jup, 2)} Jupiter
                          </span>
                          <span className="muted">
                            Mass {formatNumber(planet.mass_earth, 2)} Earth / {formatNumber(planet.mass_jup, 2)} Jupiter
                          </span>
                        </div>
                        <div className="muted">
                          Match {formatText(planet.match_method)} · {formatConfidence(planet.match_confidence)}
                          {(planet.match_confidence ?? 1) < 0.7 && (
                            <span className="warning-chip">Low confidence</span>
                          )}
                        </div>
                        <div className="muted">
                          Eq Temp {formatNumber(planet.eq_temp_k, 1)} K · Insolation {formatNumber(planet.insol_earth, 2)} Earth
                        </div>
                        <div className="muted">
                          Notes {formatText(planet.match_notes)}
                        </div>
                        <div className="muted">
                          Source {formatText(planet.provenance?.source_catalog)} · {formatText(planet.provenance?.source_version)}
                        </div>
                        <div className="muted">
                          Catalog record{" "}
                          {record ? (
                            <a href={record.url} target="_blank" rel="noreferrer">{record.label}</a>
                          ) : (
                            "Unavailable for this source"
                          )}
                        </div>
                      </>
                    );
                  })()}
                </div>
              ))}
            </div>
          )}
        </section>

        <section className="panel">
          <h3>Provenance</h3>
          <ProvenanceBlock provenance={system.provenance} />
        </section>
      </section>
    </Layout>
  );
}

export default function App() {
  return (
    <Routes>
      <Route path="/" element={<SearchPage />} />
      <Route path="/systems/:systemId" element={<SystemDetailPage />} />
    </Routes>
  );
}
