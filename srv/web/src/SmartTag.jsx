import React, {
  createContext,
  useContext,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
} from "react";
import { createPortal } from "react-dom";
import { Link, useNavigate } from "react-router-dom";

import { fetchSmartTagRegistry } from "./api.js";


const SmartTagRegistryContext = createContext({
  definitions: new Map(),
  status: "idle",
  openInspector: () => {},
});

export const OBJECT_BADGE_TAG_CATEGORIES = Object.freeze([
  "stellar_class",
  "compact_object",
]);

export function SmartTagRegistryProvider({ children }) {
  const [state, setState] = useState({
    definitions: new Map(),
    status: "loading",
  });
  const [inspectorTrail, setInspectorTrail] = useState([]);

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

  const value = useMemo(() => ({
    ...state,
    openInspector: (entry) => {
      setInspectorTrail((current) => {
        const previous = current.at(-1);
        if (
          previous?.definition?.key === entry?.definition?.key
          && previous?.contextName === entry?.contextName
        ) {
          return [...current.slice(0, -1), entry];
        }
        return [...current, entry].slice(-8);
      });
    },
  }), [state]);

  return (
    <SmartTagRegistryContext.Provider value={value}>
      {children}
      <SmartTagInspector
        trail={inspectorTrail}
        onBack={() => setInspectorTrail((current) => current.slice(0, -1))}
        onClose={() => {
          inspectorTrail.at(-1)?.trigger?.focus?.();
          setInspectorTrail([]);
        }}
      />
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

function popoverFocusableElements(popover) {
  return Array.from(popover?.querySelectorAll(
    "a[href], button:not([disabled]), [tabindex]:not([tabindex='-1'])",
  ) || []);
}

const EVIDENCE_STATUS_MARKERS = {
  derived: "D",
  assumed: "A",
  screen: "M",
  candidate: "?",
  ambiguous: "!",
  quarantined: "Q",
  missing: "-",
  source_model: "M",
};
const EVIDENCE_STATUS_LABELS = {
  derived: "Derived evidence",
  assumed: "Assumed presentation value",
  screen: "Model screening result",
  candidate: "Candidate claim",
  ambiguous: "Disputed or ambiguous evidence",
  quarantined: "Quarantined evidence",
  missing: "Missing evidence",
  source_model: "Model estimate",
};

const CLAIM_MODE_PRESENTATION = {
  observed: { marker: "O", label: "Observed claim" },
  accepted: null,
  derived: { marker: "D", label: "Derived claim" },
  modeled: { marker: "M", label: "Model based claim" },
  likely: { marker: "L", label: "Likely claim" },
  candidate: { marker: "?", label: "Candidate claim" },
  disputed: { marker: "!", label: "Disputed claim" },
  contextual: { marker: "@", label: "Context dependent claim" },
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

function claimModeSummary(resolved, statuses) {
  const explicit = String(resolved?.hero_assignment?.claim_mode || "").trim();
  if (explicit && explicit !== "evidence_bound") {
    return CLAIM_MODE_PRESENTATION[explicit] || null;
  }
  const configured = String(resolved?.application?.claim_mode || "").trim();
  if (configured && configured !== "evidence_bound") {
    return CLAIM_MODE_PRESENTATION[configured] || null;
  }
  const status = evidenceStateSummary(statuses);
  return status ? { marker: status.marker, label: status.label } : null;
}

function SmartTagInspector({ trail = [], onBack, onClose }) {
  const entry = trail.at(-1);
  const [copied, setCopied] = useState(false);
  const inspectorRef = useRef(null);
  const navigate = useNavigate();
  useEffect(() => {
    if (!entry) return undefined;
    inspectorRef.current?.focus();
    const closeEscape = (event) => {
      if (event.key === "Escape") onClose();
    };
    document.addEventListener("keydown", closeEscape);
    return () => document.removeEventListener("keydown", closeEscape);
  }, [entry, onClose]);
  if (!entry || typeof document === "undefined") return null;
  const resolved = entry.definition;
  const conceptPath = resolved.concept_slug ? `/concepts/${resolved.concept_slug}` : "";
  const filterPath = resolved.filterable
    ? `/search?tags_all=${encodeURIComponent(resolved.key)}`
    : "";
  const focusTarget = entry.targetKey || resolved.hero_assignment?.origin_target_key;
  const focusTargetType = entry.targetType || resolved.hero_assignment?.origin_target_type;
  const copy = async () => {
    if (!navigator?.clipboard) return;
    const path = conceptPath || filterPath || window.location.pathname;
    await navigator.clipboard.writeText(new URL(path, window.location.origin).href);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1200);
  };
  const returnState = () => {
    const mapReturn = {};
    window.dispatchEvent(new CustomEvent("spacegate:capture-map-return-state", {
      detail: mapReturn,
    }));
    return {
      spacegateReturn: {
      path: `${window.location.pathname}${window.location.search}${window.location.hash}`,
      scrollY: window.scrollY,
      tagKey: resolved.key,
        mapReturnToken: mapReturn.token || "",
      },
    };
  };
  return createPortal(
    <aside
      ref={inspectorRef}
      className="smart-tag-inspector"
      role="dialog"
      aria-modal="false"
      aria-label={`${resolved.name} tag inspector`}
      tabIndex={-1}
      onKeyDown={(event) => {
        if (event.key !== "Tab") return;
        const focusable = popoverFocusableElements(inspectorRef.current);
        if (!focusable.length) return;
        if (event.shiftKey && document.activeElement === focusable[0]) {
          event.preventDefault();
          focusable.at(-1)?.focus();
        } else if (!event.shiftKey && document.activeElement === focusable.at(-1)) {
          event.preventDefault();
          focusable[0]?.focus();
        }
      }}
    >
      <div className="smart-tag-inspector-nav">
        <button type="button" onClick={onBack} disabled={trail.length < 2}>Back</button>
        <span>{trail.length > 1 ? `${trail.length} inspected tags` : "Tag inspector"}</span>
        <button type="button" onClick={onClose} aria-label="Close tag inspector">Close</button>
      </div>
      <div className="smart-tag-popover-heading">
        <strong>{entry.contextName ? `${entry.contextName}: ${resolved.name}` : resolved.name}</strong>
        <span>{resolved.layer}</span>
      </div>
      <p>{resolved.tooltip || resolved.short_tooltip}</p>
      {(entry.details || []).some((row) => row?.value) ? (
        <div className="smart-tag-details">
          {entry.details.filter((row) => row?.value).map((row) => (
            <span key={`${row.label}-${row.value}`}><strong>{row.label}</strong><span>{row.value}</span></span>
          ))}
        </div>
      ) : null}
      {resolved.application ? (
        <div className="smart-tag-inspector-basis">
          <strong>How this applies</strong>
          <span>{resolved.application.evidence_requirements?.join("; ")}</span>
          <span>{resolved.application.uncertainty_policy}</span>
        </div>
      ) : null}
      {entry.sources?.length ? (
        <div className="smart-tag-sources">
          <strong>Sources in this system</strong>
          {entry.sources.slice(0, 4).map((source) => (
            source.citation_url
              ? <a key={`${source.key}-${source.contribution_kind}`} href={source.citation_url} target="_blank" rel="noreferrer">{source.public_name || source.source_id}</a>
              : <span key={`${source.key}-${source.contribution_kind}`}>{source.public_name || source.source_id}</span>
          ))}
        </div>
      ) : null}
      <div className="smart-tag-actions">
        {conceptPath ? (
          <Link
            to={conceptPath}
            onClick={(event) => {
              event.preventDefault();
              navigate(conceptPath, { state: returnState() });
            }}
          >
            Learn
          </Link>
        ) : null}
        {filterPath ? <Link to={filterPath}>Find more</Link> : null}
        {focusTarget && entry.systemId && focusTargetType !== "system" ? (
          <button
            type="button"
            onClick={() => window.dispatchEvent(new CustomEvent("spacegate:focus-object", {
              detail: { systemId: entry.systemId, targetKey: focusTarget, targetType: focusTargetType },
            }))}
          >
            Focus object
          </button>
        ) : null}
        <button type="button" onClick={copy}>{copied ? "Copied" : "Copy link"}</button>
      </div>
    </aside>,
    document.body,
  );
}

export function SmartTag({
  tagKey = "",
  definition = null,
  sources = [],
  variant = "default",
  className = "",
  label = "",
  tooltip = "",
  contextName = "",
  details = [],
  copyValue = null,
  evidenceStatuses = null,
  systemId = null,
  targetKey = "",
  targetType = "",
}) {
  const registry = useContext(SmartTagRegistryContext);
  const registryDefinition = registry.definitions.get(String(tagKey || "")) || null;
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
  const [dismissed, setDismissed] = useState(false);
  const [copied, setCopied] = useState(false);
  const [popoverPosition, setPopoverPosition] = useState(null);
  const rootRef = useRef(null);
  const triggerRef = useRef(null);
  const popoverRef = useRef(null);
  const hoverCloseTimerRef = useRef(null);
  const suppressFocusOpenRef = useRef(false);
  const open = !dismissed && (hovered || focused);
  const popoverId = React.useId();
  const resolvedEvidenceStatuses = normalizedEvidenceStatuses(
    evidenceStatuses ?? resolved.assignment?.evidence_statuses,
  );
  const evidenceState = evidenceStateSummary(resolvedEvidenceStatuses);
  const claimMode = claimModeSummary(resolved, resolvedEvidenceStatuses);
  const resolvedDisplayName = contextName
    ? `${contextName}: ${resolved.name || resolved.label || label || tagKey}`
    : (resolved.name || resolved.label || label || tagKey);
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

  const cancelHoverClose = () => {
    if (hoverCloseTimerRef.current !== null) {
      window.clearTimeout(hoverCloseTimerRef.current);
      hoverCloseTimerRef.current = null;
    }
  };
  const scheduleHoverClose = () => {
    cancelHoverClose();
    hoverCloseTimerRef.current = window.setTimeout(() => {
      setHovered(false);
      hoverCloseTimerRef.current = null;
    }, 120);
  };

  useLayoutEffect(() => {
    if (!open || !triggerRef.current || !popoverRef.current) {
      setPopoverPosition(null);
      return undefined;
    }

    const positionPopover = () => {
      const triggerBounds = triggerRef.current?.getBoundingClientRect();
      const popoverBounds = popoverRef.current?.getBoundingClientRect();
      if (!triggerBounds || !popoverBounds) {
        return;
      }
      const edge = 12;
      const gap = 7;
      const maxLeft = Math.max(edge, window.innerWidth - popoverBounds.width - edge);
      const left = Math.min(Math.max(triggerBounds.left, edge), maxLeft);
      const roomBelow = window.innerHeight - triggerBounds.bottom - edge;
      const roomAbove = triggerBounds.top - edge;
      const placeAbove = roomBelow < popoverBounds.height + gap && roomAbove > roomBelow;
      const desiredTop = placeAbove
        ? triggerBounds.top - popoverBounds.height - gap
        : triggerBounds.bottom + gap;
      const maxTop = Math.max(edge, window.innerHeight - popoverBounds.height - edge);
      const top = Math.min(Math.max(desiredTop, edge), maxTop);
      const next = {
        left: Math.round(left),
        top: Math.round(top),
        placement: placeAbove ? "above" : "below",
      };
      setPopoverPosition((current) => (
        current
          && current.left === next.left
          && current.top === next.top
          && current.placement === next.placement
          ? current
          : next
      ));
    };

    positionPopover();
    window.addEventListener("resize", positionPopover);
    window.addEventListener("scroll", positionPopover, true);
    const observer = typeof ResizeObserver === "undefined"
      ? null
      : new ResizeObserver(positionPopover);
    observer?.observe(triggerRef.current);
    observer?.observe(popoverRef.current);
    return () => {
      window.removeEventListener("resize", positionPopover);
      window.removeEventListener("scroll", positionPopover, true);
      observer?.disconnect();
    };
  }, [open]);

  useEffect(() => () => cancelHoverClose(), []);

  useEffect(() => {
    const closeEscape = (event) => {
      if (event.key === "Escape" && open) {
        setDismissed(true);
        suppressFocusOpenRef.current = true;
        rootRef.current?.querySelector("button")?.focus();
      }
    };
    document.addEventListener("keydown", closeEscape);
    return () => {
      document.removeEventListener("keydown", closeEscape);
    };
  }, [open]);

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
      data-tag-key={resolved.key}
      data-tag-category={resolved.category}
      data-tag-visual={resolved.visual_token}
      data-tag-kind={resolved.kind}
      data-tag-layer={resolved.layer}
      data-evidence-state={evidenceState?.key || "accepted"}
      onMouseEnter={() => {
        cancelHoverClose();
        setDismissed(false);
        setHovered(true);
      }}
      onMouseLeave={scheduleHoverClose}
      onFocusCapture={() => {
        if (suppressFocusOpenRef.current) {
          suppressFocusOpenRef.current = false;
        } else {
          setDismissed(false);
        }
        setFocused(true);
      }}
      onBlurCapture={(event) => {
        if (
          !event.currentTarget.contains(event.relatedTarget)
          && !popoverRef.current?.contains(event.relatedTarget)
        ) {
          setFocused(false);
          setDismissed(false);
        }
      }}
      onKeyDown={(event) => {
        if (event.key === "Escape" && open) {
          event.stopPropagation();
          setDismissed(true);
        } else if (
          event.key === "Tab"
          && !event.shiftKey
          && open
          && !event.defaultPrevented
          && document.activeElement === triggerRef.current
        ) {
          const firstPopoverControl = popoverFocusableElements(popoverRef.current)[0];
          if (firstPopoverControl) {
            event.preventDefault();
            firstPopoverControl.focus();
          }
        }
      }}
      onClick={stopCardNavigation}
    >
      <button
        ref={triggerRef}
        type="button"
        className={variant === "stellar" ? "stellar-class-chip smart-tag-trigger" : "smart-tag-trigger"}
        data-stellar-token={variant === "stellar"
          ? String(resolved.label || label).toLowerCase().replace(/[^a-z0-9]+/g, "-")
          : undefined}
        aria-expanded={open}
        aria-haspopup="dialog"
        aria-controls={open ? popoverId : undefined}
        aria-label={claimMode
          ? `${resolvedDisplayName}, ${claimMode.label}`
          : resolvedDisplayName}
        onClick={() => {
          const inspectorDetails = resolvedDetails.filter((row) => row?.value);
          registry.openInspector({
            definition: resolved,
            sources,
            contextName,
            details: inspectorDetails,
            copyValue,
            systemId,
            targetKey,
            targetType,
            trigger: triggerRef.current,
          });
          setHovered(false);
          setFocused(false);
          setDismissed(true);
        }}
      >
        {resolved.label || label || tagKey}
        {claimMode ? (
          <span
            className="smart-tag-state-marker"
            aria-hidden="true"
            title={claimMode.label}
          >
            {claimMode.marker}
          </span>
        ) : null}
      </button>
      {open && typeof document !== "undefined" ? createPortal(
        <span
          className={`smart-tag-portal smart-tag-root smart-tag-${variant}`}
          data-tag-key={resolved.key}
          data-tag-category={resolved.category}
          data-tag-visual={resolved.visual_token}
          data-tag-kind={resolved.kind}
          data-tag-layer={resolved.layer}
          data-evidence-state={evidenceState?.key || "accepted"}
          onMouseEnter={() => {
            cancelHoverClose();
            setHovered(true);
          }}
          onMouseLeave={scheduleHoverClose}
          onFocusCapture={() => setFocused(true)}
          onBlurCapture={(event) => {
            if (
              !event.currentTarget.contains(event.relatedTarget)
              && !rootRef.current?.contains(event.relatedTarget)
            ) {
              setFocused(false);
              setDismissed(false);
            }
          }}
          onKeyDown={(event) => {
            if (event.key !== "Tab") {
              return;
            }
            const focusable = popoverFocusableElements(popoverRef.current);
            if (
              (event.shiftKey && event.target === focusable[0])
              || (!event.shiftKey && event.target === focusable.at(-1))
            ) {
              event.preventDefault();
              triggerRef.current?.focus();
            }
          }}
          onClick={stopCardNavigation}
        >
          <span
            ref={popoverRef}
            id={popoverId}
            className="smart-tag-popover"
            role="dialog"
            aria-label={`${resolvedDisplayName} details`}
            data-placement={popoverPosition?.placement || "below"}
            style={{
              "--smart-tag-popover-left": `${popoverPosition?.left ?? 12}px`,
              "--smart-tag-popover-top": `${popoverPosition?.top ?? 12}px`,
            }}
          >
            <span className="smart-tag-popover-heading">
              <strong>{resolvedDisplayName}</strong>
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
        </span>,
        document.body,
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
            label: source.short_name || source.public_name || source.publisher || source.source_id,
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
  excludeCategories = [],
  systemId = null,
  contextName = "",
  targetKey = "",
  targetType = "",
}) {
  const budget = Number.isFinite(Number(limit))
    ? Number(limit)
    : ({ compact: 4, normal: 8, expanded: 16 }[mode] || 8);
  const ordered = useMemo(
    () => (Array.isArray(tags) ? tags : [])
      .filter((tag) => !excludeCategories.includes(String(tag?.category || "")))
      .slice()
      .sort((left, right) => (
        Number(right?.priority?.[mode] || right?.priority?.normal || 0)
          - Number(left?.priority?.[mode] || left?.priority?.normal || 0)
        || String(left?.name || left?.key).localeCompare(String(right?.name || right?.key))
      )),
    [excludeCategories, mode, tags],
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
          systemId={systemId}
          contextName={contextName}
          targetKey={targetKey}
          targetType={targetType}
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

export function HeroSmartTagList({
  tags = [],
  sources = [],
  systemId = null,
  excludeCategories = [],
  className = "",
  label = "Featured tags",
  allTagsTargetId = "",
  allTagCount = null,
}) {
  const [expanded, setExpanded] = useState(false);
  const allTags = Array.isArray(tags) ? tags : [];
  const eligible = allTags.filter(
    (tag) => !excludeCategories.includes(String(tag?.category || "")),
  );
  const featured = eligible
    .filter((tag) => tag?.hero_assignment)
    .slice()
    .sort((left, right) => (
      Number(left.hero_assignment.rank) - Number(right.hero_assignment.rank)
      || String(left.key).localeCompare(String(right.key))
    ))
    .slice(0, 4);
  if (!featured.length && !allTags.length) return null;
  return (
    <span className={`smart-tag-hero-group ${className}`.trim()}>
      <span className="smart-tag-list" aria-label={label}>
        {featured.map((tag) => (
          <SmartTag
            key={tag.key}
            tagKey={tag.key}
            definition={tag}
            sources={sources}
            systemId={systemId}
          />
        ))}
        {allTagsTargetId ? (
          <a className="smart-tag-all-toggle" href={`#${allTagsTargetId}`}>
            {`All tags (${allTagCount ?? allTags.length})`}
          </a>
        ) : (
          <button
            type="button"
            className="smart-tag-all-toggle"
            aria-expanded={expanded}
            onClick={() => setExpanded((value) => !value)}
          >
            {expanded ? "Hide tags" : `All tags (${allTags.length})`}
          </button>
        )}
      </span>
      {expanded && !allTagsTargetId ? (
        <span className="smart-tag-all-panel" aria-label="All system tags">
          <SmartTagList
            tags={allTags}
            sources={sources}
            mode="expanded"
            limit={allTags.length}
            systemId={systemId}
          />
          <SourceTagList sources={sources} limit={sources.length} />
        </span>
      ) : null}
    </span>
  );
}
