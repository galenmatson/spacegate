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

export function SmartTag({
  tagKey = "",
  definition = null,
  sources = [],
  variant = "default",
  className = "",
  label = "",
  tooltip = "",
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
  const [copied, setCopied] = useState(false);
  const rootRef = useRef(null);
  const open = pinned || hovered || focused;
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
      }
    };
    const closeEscape = (event) => {
      if (event.key === "Escape") {
        setPinned(false);
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

  return (
    <span
      ref={rootRef}
      className={`smart-tag-root smart-tag-${variant} ${className}`.trim()}
      data-tag-category={resolved.category}
      data-tag-visual={resolved.visual_token}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => setHovered(false)}
      onFocusCapture={() => setFocused(true)}
      onBlurCapture={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) {
          setFocused(false);
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
        onClick={() => setPinned((value) => !value)}
      >
        {resolved.label || label || tagKey}
      </button>
      {open ? (
        <span className="smart-tag-popover" role="dialog" aria-label={`${resolved.name} details`}>
          <span className="smart-tag-popover-heading">
            <strong>{resolved.name}</strong>
            <span>{resolved.layer}</span>
          </span>
          <span className="smart-tag-popover-copy">{resolved.tooltip || resolved.short_tooltip}</span>
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
                    {source.publisher || source.source_id}
                  </a>
                ) : (
                  <span key={`${source.key}-${source.contribution_kind}`}>
                    {source.publisher || source.source_id}
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
          </span>
        </span>
      ) : null}
    </span>
  );
}

export function SmartTagList({
  tags = [],
  sources = [],
  limit = 8,
  className = "",
  label = "Smart tags",
}) {
  const visible = useMemo(
    () => (Array.isArray(tags) ? tags : [])
      .slice()
      .sort((left, right) => (
        Number(right?.priority?.normal || 0) - Number(left?.priority?.normal || 0)
        || String(left?.name || left?.key).localeCompare(String(right?.name || right?.key))
      ))
      .slice(0, limit),
    [limit, tags],
  );
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
    </span>
  );
}
