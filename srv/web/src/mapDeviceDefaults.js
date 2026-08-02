const SOFTWARE_RENDERER_PATTERN = /(swiftshader|llvmpipe|software|basic render|softpipe|mesa offscreen)/i;

let cachedGraphicsProfile = null;

function finiteCapability(value) {
  const numeric = Number(value);
  return Number.isFinite(numeric) && numeric > 0 ? numeric : null;
}

function readGraphicsProfile(documentObject) {
  if (cachedGraphicsProfile) return cachedGraphicsProfile;
  const fallback = {
    webgl2: false,
    renderer: "unavailable",
    hardwareAccelerated: false,
    maxTextureSize: 0,
  };
  if (!documentObject?.createElement) {
    cachedGraphicsProfile = fallback;
    return cachedGraphicsProfile;
  }

  let gl = null;
  try {
    const canvas = documentObject.createElement("canvas");
    gl = canvas.getContext("webgl2", {
      alpha: false,
      antialias: false,
      depth: false,
      failIfMajorPerformanceCaveat: true,
      powerPreference: "high-performance",
      preserveDrawingBuffer: false,
      stencil: false,
    });
    if (!gl) {
      cachedGraphicsProfile = fallback;
      return cachedGraphicsProfile;
    }
    const debugInfo = gl.getExtension("WEBGL_debug_renderer_info");
    const renderer = String(
      debugInfo
        ? gl.getParameter(debugInfo.UNMASKED_RENDERER_WEBGL)
        : gl.getParameter(gl.RENDERER),
    );
    cachedGraphicsProfile = {
      webgl2: true,
      renderer,
      hardwareAccelerated: !SOFTWARE_RENDERER_PATTERN.test(renderer),
      maxTextureSize: Number(gl.getParameter(gl.MAX_TEXTURE_SIZE) || 0),
    };
    gl.getExtension("WEBGL_lose_context")?.loseContext();
  } catch {
    cachedGraphicsProfile = fallback;
  }
  return cachedGraphicsProfile;
}

export function mapDeviceDefaultsFor(profile = {}) {
  const width = finiteCapability(profile.width) || 1440;
  const touch = Boolean(profile.touch);
  const cores = finiteCapability(profile.cores);
  const memoryGiB = finiteCapability(profile.memoryGiB);
  const accelerated = Boolean(profile.webgl2 && profile.hardwareAccelerated);
  const clearlyConstrained = !accelerated
    || (cores !== null && cores <= 4)
    || (memoryGiB !== null && memoryGiB <= 4);
  const clearlyHighCapacity = accelerated
    && width >= 1200
    && cores !== null
    && cores >= 8
    && memoryGiB !== null
    && memoryGiB >= 8
    && Number(profile.maxTextureSize || 0) >= 8192;
  const highCapacityTouch = accelerated
    && touch
    && cores !== null
    && cores >= 8
    && memoryGiB !== null
    && memoryGiB >= 8;

  if (touch) {
    if (clearlyConstrained) {
      return { tier: "constrained_touch", radiusLy: 100, densityMode: "performance", starRenderMode: "discovery" };
    }
    if (highCapacityTouch) {
      return { tier: "enhanced_touch", radiusLy: 500, densityMode: "balanced", starRenderMode: "bright" };
    }
    return { tier: "standard_touch", radiusLy: 250, densityMode: "performance", starRenderMode: "discovery" };
  }

  if (width < 900 || clearlyConstrained) {
    return { tier: "constrained_desktop", radiusLy: 250, densityMode: "performance", starRenderMode: "discovery" };
  }
  if (clearlyHighCapacity) {
    return { tier: "enhanced_desktop", radiusLy: 1000, densityMode: "exact", starRenderMode: "bright" };
  }
  return { tier: "standard_desktop", radiusLy: 500, densityMode: "balanced", starRenderMode: "bright" };
}

export function enhancedDesktopQualification(profile = {}) {
  const width = finiteCapability(profile.width) || 0;
  const cores = finiteCapability(profile.cores);
  const memoryGiB = finiteCapability(profile.memoryGiB);
  const maxTextureSize = Number(profile.maxTextureSize || 0);
  const checks = [
    {
      id: "fine_pointer",
      passed: !Boolean(profile.touch),
      detail: profile.touch ? "coarse pointer" : "fine pointer",
    },
    {
      id: "viewport_width",
      passed: width >= 1200,
      detail: width ? `viewport ${width}px` : "viewport width unavailable",
    },
    {
      id: "logical_processors",
      passed: cores !== null && cores >= 8,
      detail: cores === null ? "CPU threads hidden" : `${cores} CPU threads`,
    },
    {
      id: "device_memory",
      passed: memoryGiB !== null && memoryGiB >= 8,
      detail: memoryGiB === null ? "device memory hidden" : `${memoryGiB} GiB device memory`,
    },
    {
      id: "webgl2",
      passed: Boolean(profile.webgl2),
      detail: profile.webgl2 ? "WebGL2 available" : "WebGL2 unavailable",
    },
    {
      id: "hardware_acceleration",
      passed: Boolean(profile.hardwareAccelerated),
      detail: profile.hardwareAccelerated ? "hardware renderer" : "software or masked renderer",
    },
    {
      id: "texture_size",
      passed: maxTextureSize >= 8192,
      detail: maxTextureSize ? `max texture ${maxTextureSize}px` : "texture limit unavailable",
    },
  ];
  return {
    eligible: checks.every((check) => check.passed),
    checks,
    blockers: checks.filter((check) => !check.passed).map((check) => check.detail),
  };
}

export function readMapDeviceProfile(windowObject = globalThis.window, navigatorObject = globalThis.navigator) {
  if (!windowObject) {
    return {
      width: 1440,
      height: 900,
      dpr: 1,
      touch: false,
      cores: null,
      memoryGiB: null,
      webgl2: false,
      renderer: "unavailable",
      hardwareAccelerated: false,
      maxTextureSize: 0,
    };
  }
  const graphics = readGraphicsProfile(windowObject.document);
  return {
    width: windowObject.innerWidth || 1440,
    height: windowObject.innerHeight || 900,
    dpr: windowObject.devicePixelRatio || 1,
    touch: Boolean(windowObject.matchMedia?.("(pointer: coarse)")?.matches),
    cores: finiteCapability(navigatorObject?.hardwareConcurrency),
    memoryGiB: finiteCapability(navigatorObject?.deviceMemory),
    ...graphics,
  };
}
