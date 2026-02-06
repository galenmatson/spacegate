import React, { useMemo, useState } from "react";
import { Link, Route, Routes, useNavigate, useParams } from "react-router-dom";
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

function Layout({ children }) {
  return (
    <div className="app">
      <header className="site-header">
        <div>
          <div className="eyebrow">Public database browser</div>
          <h1>Spacegate Browser</h1>
          <p>
            Search nearby systems, stars, and confirmed exoplanets with full provenance.
          </p>
        </div>
        <div className="header-actions">
          <Link to="/" className="button ghost">Search</Link>
        </div>
      </header>
      <main>{children}</main>
    </div>
  );
}

function SearchPage() {
  const [query, setQuery] = useState("");
  const [maxDist, setMaxDist] = useState("");
  const [sort, setSort] = useState("name");
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
      setSearchStarted(true);
    }
  };

  const onSubmit = (event) => {
    event.preventDefault();
    const hasFilters =
      Boolean(query.trim()) ||
      Boolean(maxDist) ||
      spectral.length > 0 ||
      hasPlanets;
    if (!hasFilters) {
      setResults([]);
      setHasMore(false);
      setCursor(null);
      setSearchStarted(false);
      return;
    }
    runSearch(null, true);
  };

  const toggleSpectral = (value) => {
    setSpectral((prev) => {
      if (prev.includes(value)) {
        return prev.filter((item) => item !== value);
      }
      return [...prev, value];
    });
  };

  return (
    <Layout>
      <section className="search-layout">
        <form className="panel" onSubmit={onSubmit}>
          <label className="field">
            <span>Search</span>
            <input
              type="text"
              value={query}
              onChange={(event) => setQuery(event.target.value)}
              placeholder="Search systems by name, ID, or catalog key"
            />
          </label>

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
              <option value="name">Name (A-Z)</option>
              <option value="distance">Distance (nearest)</option>
            </select>
          </label>

          <button className="button" type="submit" disabled={loading}>
            {loading ? "Searching..." : "Search"}
          </button>

          {error && <div className="error-box">{error}</div>}
        </form>

        <section className="results">
          {!searchStarted && (
            <div className="empty-state">
              <h2>Start exploring</h2>
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
                    Source: {formatText(item.provenance?.source_catalog)} {formatText(item.provenance?.source_version)}
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
        <span>{redistribution}</span>
      </div>
      <div>
        <strong>Retrieved</strong>
        <span>{formatText(provenance.retrieved_at)}</span>
      </div>
      <div>
        <strong>Transform</strong>
        <span>{formatText(provenance.transform_version)}</span>
      </div>
    </div>
  );
}

function SystemDetailPage() {
  const { systemId } = useParams();
  const navigate = useNavigate();
  const [data, setData] = React.useState(null);
  const [loading, setLoading] = React.useState(true);
  const [error, setError] = React.useState("");

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
    <Layout>
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

        <section className="panel">
          <h3>Stars</h3>
          {stars.length === 0 && <p className="muted">No star members recorded.</p>}
          {stars.length > 0 && (
            <div className="table">
              {stars.map((star) => (
                <div className="row" key={star.star_id}>
                  <div>
                    <strong>{formatText(star.star_name)}</strong>
                    <span className="muted">Component {formatText(star.component)}</span>
                  </div>
                  <div>
                    <span>Spectral: {formatText(star.spectral_type_raw)}</span>
                    <span className="muted">Class {formatText(star.spectral_class)} {formatText(star.luminosity_class)}</span>
                  </div>
                  <div>
                    <span>Distance {formatNumber(star.dist_ly, 2)} ly</span>
                    <span className="muted">Vmag {formatNumber(star.vmag, 2)}</span>
                  </div>
                  <div className="muted">Source {formatText(star.provenance?.source_catalog)}</div>
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
                  <div>
                    <strong>{formatText(planet.planet_name)}</strong>
                    <span className="muted">Discovery {formatText(planet.disc_year)} · {formatText(planet.discovery_method)}</span>
                  </div>
                  <div>
                    <span>Period {formatNumber(planet.orbital_period_days, 2)} d</span>
                    <span className="muted">SMA {formatNumber(planet.semi_major_axis_au, 3)} AU</span>
                  </div>
                  <div>
                    <span>Radius {formatNumber(planet.radius_earth, 2)} Earth</span>
                    <span className="muted">Mass {formatNumber(planet.mass_earth, 2)} Earth</span>
                  </div>
                  <div className="muted">
                    Match {formatText(planet.match_method)} · {formatNumber(planet.match_confidence, 2)}
                  </div>
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
