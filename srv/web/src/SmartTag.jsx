import React, { createContext, useContext, useEffect, useMemo, useRef, useState } from "react";
import { Link } from "react-router-dom";

import { fetchSmartTagRegistry } from "./api.js";


const SmartTagRegistryContext = createContext({
  definitions: new Map(),
  status: "idle",
});

export function SmartTagRegistryProvider({ children }) {
  const [state, setState] = useState({
    definitions: new Map(),
    status: "loading",
  });

  useEffect(() => {
    let active = true;
    fetchSmartTagRegistry()
      .then((payload) => {
        if (!active) {
          return;
        }
        const definitions = new Map(
          (payload?.definitions || []).map((definition) => [definition.key, definition]),
        );
        setState({ definitions, status: "ready" });
      })
      .catch(() => {
        if (active) {
          setState({ definitions: new Map(), status: "unavailable" });
        }
      });
    return () => {
      active = false;
    };
  }, []);

  return (
    <SmartTagRegistryContext.Provider value={state}>
      {children}
    </SmartTagRegistryContext.Provider>
  );
}

export function useSmartTagDefinition(tagKey) {
  const registry = useContext(SmartTagRegistryContext);
  return registry.definitions.get(String(tagKey || "")) || null;
}

function stopCardNavigation(event) {
  event.stopPropagation();
}

const EVIDENCE_STATUS_MARKERS = {
  derived: "D",
  assumed: "A",
  screen: "S",
  candidate: "C",
  ambiguous: "?",
  quarantined: "Q",
  missing: "-",
  source_model: "E",
};
const EVIDENCE_STATUS_LABELS = {
  derived: "Derived evidence",
  assumed: "Assumed presentation value",
  screen: "Screening result",
  candidate: "Candidate evidence",
  ambiguous: "Ambiguous evidence",
  quarantined: "Quarantined evidence",
  missing: "Missing evidence",
  source_model: "Source model estimate",
};

function normalizedEvidenceStatuses(value) {
  const values = Array.isArray(value) ? value : (value ? [value] : []);
  return Array.from(new Set(
    values
      .map((status) => String(status || "").trim().toLowerCase())
      .filter(Boolean),
  )).sort();
}

function evidenceStateSummary(statuses) {
  const visible = statuses.filter((status) => !["accepted", "source"].includes(status));
  if (!visible.length) {
    return null;
  }
  if (visible.length > 1) {
    return { key: "mixed", marker: "M", label: "Mixed evidence states" };
  }
  const key = visible[0];
  return {
    key,
    marker: EVIDENCE_STATUS_MARKERS[key] || "!",
    label: EVIDENCE_STATUS_LABELS[key]
      || `${key.charAt(0).toUpperCase()}${key.slice(1)} evidence`,
  };
}

export function SmartTag({
  tagKey = "",
  definition = null,
  sources = [],
  variant = "default",
  className = "",
  label = "",
  tooltip = "",
  details = [],
  copyValue = null,
  evidenceStatuses = null,
}) {
  const registryDefinition = useSmartTagDefinition(tagKey);
  const resolved = definition || registryDefinition || {
    key: tagKey,
    label: label || tagKey,
    name: label || tagKey,
    category: "uncategorized",
    kind: "concept",
    layer: "presentation",
    visual_token: "generic",
    tooltip,
    short_tooltip: tooltip,
    concept_slug: null,
    source_policy: "",
  };
  const [hovered, setHovered] = useState(false);
  const [focused, setFocused] = useState(false);
  const [pinned, setPinned] = useState(false);
  const [dismissed, setDismissed] = useState(false);
  const [copied, setCopied] = useState(false);
  const rootRef = useRef(null);
  const suppressFocusOpenRef = useRef(false);
  const open = !dismissed && (pinned || hovered || focused);
  const resolvedEvidenceStatuses = normalizedEvidenceStatuses(
    evidenceStatuses ?? resolved.assignment?.evidence_statuses,
  );
  const evidenceState = evidenceStateSummary(resolvedEvidenceStatuses);
  const assignmentDetails = [
    {
      label: "Evidence state",
      value: resolvedEvidenceStatuses.length
        ? resolvedEvidenceStatuses.join(", ")
        : "",
    },
    {
      label: "Scope",
      value: resolved.assignment?.scope,
    },
    {
      label: "Members",
      value: resolved.assignment?.member_count
        ? String(resolved.assignment.member_count)
        : "",
    },
    {
      label: "Confidence",
      value: resolved.assignment?.min_confidence === null
        || resolved.assignment?.min_confidence === undefined
        ? ""
        : (
          resolved.assignment.min_confidence === resolved.assignment.max_confidence
            ? String(resolved.assignment.min_confidence)
            : `${resolved.assignment.min_confidence} to ${resolved.assignment.max_confidence}`
        ),
    },
  ];
  const resolvedDetails = [...details, ...assignmentDetails];
  const conceptPath = resolved.concept_slug ? `/concepts/${resolved.concept_slug}` : "";
  const filterPath = resolved.filterable
    ? `/search?tags_all=${encodeURIComponent(resolved.key)}`
    : "";

  useEffect(() => {
    if (!pinned) {
      return undefined;
    }
    const closeOutside = (event) => {
      if (!rootRef.current?.contains(event.target)) {
        setPinned(false);
        setDismissed(false);
      }
    };
    const closeEscape = (event) => {
      if (event.key === "Escape") {
        setPinned(false);
        setDismissed(true);
        suppressFocusOpenRef.current = true;
        rootRef.current?.querySelector("button")?.focus();
      }
    };
    document.addEventListener("pointerdown", closeOutside);
    document.addEventListener("keydown", closeEscape);
    return () => {
      document.removeEventListener("pointerdown", closeOutside);
      document.removeEventListener("keydown", closeEscape);
    };
  }, [pinned]);

  const copyLink = async () => {
    const relative = conceptPath || filterPath;
    if (!relative || !navigator?.clipboard) {
      return;
    }
    await navigator.clipboard.writeText(new URL(relative, window.location.origin).href);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };
  const copyDetails = async () => {
    if (copyValue === null || copyValue === undefined || !navigator?.clipboard) {
      return;
    }
    const rendered = typeof copyValue === "string"
      ? copyValue
      : JSON.stringify(copyValue, null, 2);
    await navigator.clipboard.writeText(rendered);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };

  return (
    <span
      ref={rootRef}
      className={`smart-tag-root smart-tag-${variant} ${className}`.trim()}
      data-tag-category={resolved.category}
      data-tag-visual={resolved.visual_token}
      data-tag-kind={resolved.kind}
      data-tag-layer={resolved.layer}
      data-evidence-state={evidenceState?.key || "accepted"}
      onMouseEnter={() => {
        setDismissed(false);
        setHovered(true);
      }}
      onMouseLeave={() => setHovered(false)}
      onFocusCapture={() => {
        if (suppressFocusOpenRef.current) {
          suppressFocusOpenRef.current = false;
        } else {
          setDismissed(false);
        }
        setFocused(true);
      }}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) {
          setFocused(false);
          setDismissed(false);
        }
      }}
      onKeyDown={(event) => {
        if (event.key === "Escape" && open) {
          event.stopPropagation();
          setPinned(false);
          setDismissed(true);
        }
      }}
      onClick={stopCardNavigation}
    >
      <button
        type="button"
        className={variant === "stellar" ? "stellar-class-chip smart-tag-trigger" : "smart-tag-trigger"}
        data-stellar-token={variant === "stellar"
          ? String(resolved.label || label).toLowerCase().replace(/[^a-z0-9]+/g, "-")
          : undefined}
        aria-expanded={open}
        aria-haspopup="dialog"
        aria-label={evidenceState
          ? `${resolved.label || label || tagKey}, ${evidenceState.label}`
          : (resolved.label || label || tagKey)}
        onClick={() => {
          setDismissed(false);
          setPinned((value) => !value);
        }}
      >
        {resolved.label || label || tagKey}
        {evidenceState ? (
          <span
            className="smart-tag-state-marker"
            aria-hidden="true"
            title={evidenceState.label}
          >
            {evidenceState.marker}
          </span>
        ) : null}
      </button>
      {open ? (
        <span className="smart-tag-popover" role="dialog" aria-label={`${resolved.name} details`}>
          <span className="smart-tag-popover-heading">
            <strong>{resolved.name}</strong>
            <span>{resolved.layer}</span>
          </span>
          <span className="smart-tag-popover-copy">{resolved.tooltip || resolved.short_tooltip}</span>
          {resolvedDetails.some((row) => row?.value) ? (
            <span className="smart-tag-details">
              {resolvedDetails.filter((row) => row?.value).map((row) => (
                <span key={`${row.label}-${row.value}`}>
                  <strong>{row.label}</strong>
                  <span>{row.value}</span>
                </span>
              ))}
            </span>
          ) : null}
          {resolved.source_policy ? (
            <span className="smart-tag-policy">
              <strong>Basis</strong>
              <span>{String(resolved.source_policy).replaceAll("_", " ")}</span>
            </span>
          ) : null}
          {sources.length ? (
            <span className="smart-tag-sources">
              <strong>Sources in this system</strong>
              {sources.slice(0, 4).map((source) => (
                source.citation_url ? (
                  <a
                    key={`${source.key}-${source.contribution_kind}`}
                    href={source.citation_url}
                    target="_blank"
                    rel="noreferrer"
                  >
                    {source.public_name || source.publisher || source.source_id}
                  </a>
                ) : (
                  <span key={`${source.key}-${source.contribution_kind}`}>
                    {source.public_name || source.publisher || source.source_id}
                  </span>
                )
              ))}
            </span>
          ) : null}
          <span className="smart-tag-actions">
            {conceptPath ? <Link to={conceptPath}>Learn</Link> : null}
            {filterPath ? <Link to={filterPath}>Find more</Link> : null}
            {conceptPath || filterPath ? (
              <button type="button" onClick={copyLink}>{copied ? "Copied" : "Copy link"}</button>
            ) : null}
            {copyValue !== null && copyValue !== undefined ? (
              <button type="button" onClick={copyDetails}>{copied ? "Copied" : "Copy details"}</button>
            ) : null}
          </span>
        </span>
      ) : null}
    </span>
  );
}


export function SourceTagList({
  sources = [],
  limit = 3,
  className = "",
}) {
  const visible = (Array.isArray(sources) ? sources : []).slice(0, limit);
  if (!visible.length) {
    return null;
  }
  return (
    <span className={`smart-tag-list smart-source-list ${className}`.trim()} aria-label="Contributing sources">
      {visible.map((source) => (
        <SmartTag
          key={`${source.key}-${source.contribution_kind || ""}`}
          tagKey={source.key}
          variant="source"
          definition={{
            key: source.key,
            label: source.public_name || source.publisher || source.source_id,
            name: source.public_name || source.publisher || source.source_id,
            category: "source_reference",
            kind: "source",
            layer: "evidence",
            visual_token: "source_reference",
            tooltip: source.description || "A source contributing accepted evidence shown for this system.",
            short_tooltip: source.description || "A contributing scientific source.",
            concept_slug: null,
            source_policy: source.contribution_kind || "displayed evidence",
            filterable: false,
          }}
          details={[
            { label: "Publisher", value: source.publisher },
            { label: "Release", value: source.release_id },
            { label: "Contribution", value: source.contribution_kind },
            { label: "Context records", value: source.member_count ? String(source.member_count) : "" },
            { label: "Mission / instrument", value: source.mission_instrument },
          ]}
          copyValue={source}
        />
      ))}
    </span>
  );
}

export function SmartTagList({
  tags = [],
  sources = [],
  limit = null,
  mode = "normal",
  className = "",
  label = "Smart tags",
}) {
  const budget = Number.isFinite(Number(limit))
    ? Number(limit)
    : ({ compact: 4, normal: 8, expanded: 16 }[mode] || 8);
  const ordered = useMemo(
    () => (Array.isArray(tags) ? tags : [])
      .slice()
      .sort((left, right) => (
        Number(right?.priority?.[mode] || right?.priority?.normal || 0)
          - Number(left?.priority?.[mode] || left?.priority?.normal || 0)
        || String(left?.name || left?.key).localeCompare(String(right?.name || right?.key))
      )),
    [mode, tags],
  );
  const visible = ordered.slice(0, budget);
  const overflow = Math.max(0, ordered.length - visible.length);
  if (!visible.length) {
    return null;
  }
  return (
    <span className={`smart-tag-list ${className}`.trim()} aria-label={label}>
      {visible.map((tag) => (
        <SmartTag
          key={tag.key}
          tagKey={tag.key}
          definition={tag}
          sources={sources}
        />
      ))}
      {overflow ? (
        <span
          className="smart-tag-overflow"
          aria-label={`${overflow} additional tags not shown in this ${mode} view`}
          title={ordered.slice(budget).map((tag) => tag.name || tag.label || tag.key).join(", ")}
        >
          +{overflow}
        </span>
      ) : null}
    </span>
  );
}
