# Proton ComfyUI Asset Generator

Status date: 2026-08-03 UTC

This document records the supported Spacegate integration contract for the
on-demand ComfyUI installation on Proton. The endpoint is an offline asset
worker. It is not a public service, a scientific authority, or a dependency of
the served Spacegate application.

## Endpoint

- host: `proton` (`10.0.0.10`)
- LAN-only ComfyUI endpoint: `http://10.0.0.10:8188`
- application: `/data/comfyui/app`
- environment: `/data/comfyui/venv`
- models: `/data/comfyui/models`
- output: `/data/comfyui/output`
- Spacegate wrapper: `/data/comfyui/tools/spacegate_image.py`
- pipeline notes: `/data/comfyui/PIPELINES.md`
- host notes: `/data/comfyui/LOCAL_NOTES.md`
- model manifest: `/data/comfyui/models/MANIFEST.json`
- checksums: `/data/comfyui/models.sha256`

ComfyUI has no application-level authentication. Port 8188 must remain bound
only to the trusted LAN and must never be exposed to the internet. Public
Spacegate requests may not proxy directly to it.

## Validated Pipeline

Both supported pipelines use one persistent RealVisXL V5 FP16 checkpoint:

```text
icon:    RealVisXL -> CPU VAE decode -> CPU BiRefNet/Lucida -> RGBA PNG
surface: RealVisXL -> CPU VAE decode -> RGB PNG
```

Active checkpoint:

```text
SDXL/RealVisXL_V5.0_fp16.safetensors
sha256 6a35a7855770ae9820a3c931d4964c3817b6d9e3c6f9c4dabb5b3a94e5643b80
```

SDXL Base 1.0 is retained outside the active directory for reproducibility:

```text
/data/comfyui/models/archive/SDXL/sd_xl_base_1.0.safetensors
sha256 31e35c80fc4829d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b
```

## ROCm Safety Invariants

The following constraints are empirical clean-boot results and must not be
relaxed casually:

1. Do not switch between SDXL Base and RealVisXL in one boot. A hot swap caused
   `hipErrorLaunchFailure`, amdgpu MES failure, and recoverable MODE2 resets.
2. Keep VAE decode on CPU. Ordinary and tiled GPU VAE convolution both caused
   MODE2 resets on the Radeon 780M APU.
3. Keep BiRefNet/Lucida matting on CPU.
4. Do not use pinned memory or asynchronous offload on the 32-GiB UMA host.
5. If the current boot has an amdgpu reset, submit no more GPU jobs. Reboot
   Proton; restarting ComfyUI alone is insufficient.

Validated service flags:

```text
--cpu-vae
--lowvram
--disable-dynamic-vram
--disable-async-offload
--disable-pinned-memory
```

The wrapper checks the current boot's kernel log and refuses local GPU work
after a reset. Do not bypass this guard.

## Service Operation

The user service is linked but deliberately not enabled at boot. It uses
`Restart=no` so a ROCm fault cannot create a reset loop.

```bash
systemctl --user start comfyui.service
systemctl --user stop comfyui.service
systemctl --user status comfyui.service
journalctl --user -u comfyui.service -f
```

The wrapper starts an inactive service and reuses the persistent single-model
process for subsequent jobs.

## Supported Wrapper Calls

Transparent icon:

```bash
/data/comfyui/tools/spacegate_image.py generate icon \
  --prompt "a compact blue-white stellar photosphere with a restrained radial corona, polished scientific digital illustration" \
  --context /data/comfyui/contexts/icon_star_test.json \
  --profile draft \
  --seed 20260810 \
  --output-prefix Spacegate/icons/stellar-test
```

Describe astrophysical stars as round photospheres or disks. A bare request for
a "star" frequently produces a five-point symbol. Icon contexts should reject
polygons, badges, emblems, and symbolic stars where those forms are unwanted.

Planetary surface:

```bash
/data/comfyui/tools/spacegate_image.py generate surface \
  --prompt "ground-level wide-angle view across an ancient volcanic plain" \
  --context /data/comfyui/contexts/surface_m_dwarf_test.json \
  --profile draft \
  --seed 20260807 \
  --output-prefix Spacegate/surfaces/m-dwarf-test
```

Dimensions must be multiples of 64 and at least 256 pixels. Production may
override width, height, steps, and CFG explicitly. BiRefNet is the default
matting model for compact silhouettes; Lucida may be selected when broad soft
glow is more important.

## Scientific Context

Every job uses a JSON context that separates sourced facts, visual constraints,
negative constraints, sources, and disclosed assumptions. Only visual
constraints enter the positive prompt. Negative constraints enter the negative
prompt. The complete context enters output metadata.

Generated pixels are always non-canonical synthetic illustrations. They may be
classified only as DISC or RIM and must never be represented as CORE or ARM
science. Context must contain no credentials, secrets, private notes, or
unpublished sensitive information because image metadata may become public.

## Photon Invocation

The current safe integration is an SSH-restricted operator workflow:

```bash
scp /tmp/spacegate-image-context.json \
  galen@10.0.0.10:/data/comfyui/contexts/requests/example.json

ssh galen@10.0.0.10 \
  /data/comfyui/tools/spacegate_image.py generate icon \
  --prompt "a compact orange-red stellar photosphere with a restrained corona" \
  --context /data/comfyui/contexts/requests/example.json \
  --profile draft \
  --seed 42 \
  --output-prefix Spacegate/requests/example

scp galen@10.0.0.10:/data/comfyui/output/Spacegate/requests/example_00001_.png \
  /tmp/
```

Do not expose ComfyUI directly. A future automated integration should use a
small authenticated Spacegate job API or an SSH-restricted command, but only if
manual review volume demonstrates that automation is worthwhile.

## Metadata and Promotion

Every output PNG embeds the executable ComfyUI graph and Spacegate metadata,
including context hash, layer/canonicality warning, prompts, seed, sampler,
scheduler, dimensions, model sources/licenses/hashes, software versions,
device, and service flags.

Inspect an output on Proton with:

```bash
/data/comfyui/tools/spacegate_image.py inspect /data/comfyui/output/path/image.png
```

PNG optimization and CDN processing may remove text chunks. An accepted asset
therefore requires a durable Spacegate metadata record containing at least:

- stable asset identity, purpose, and object/concept/tag scope;
- DISC or RIM classification and user-visible synthetic-art warning;
- context JSON and hash, prompt/negative prompt, seed, and generation settings;
- model/pipeline identifiers, versions, licenses, and cryptographic hashes;
- source/citation and assumption records;
- original and promoted file hashes, dimensions, MIME type, and alpha policy;
- reviewer, review state, supersession, build identity, and timestamps.

The current `disc.generated_images` v1.5 contract is not sufficient for this
promotion record and must be extended before generated imagery becomes a
durable public asset class.

## UI Asset Policy

Generated art is appropriate for concept illustrations, object-category art,
large decorative icons, and speculative DISC/RIM scenes. It should not replace
Lucide controls or deterministic CSS/vector scientific badges merely because a
model can produce an attractive image.

Any icon intended for a compact interface must be evaluated at its actual
16/24/32/48-pixel sizes, in every public theme, on light and dark backgrounds,
and with transparency, silhouette, color-vision, and high-DPI checks. Accepted
source art should be normalized into a bounded sprite/asset contract rather
than loading independent production-size PNGs throughout the interface.

## Failure Runbook

```bash
systemctl --user status comfyui.service --no-pager
journalctl --user -u comfyui.service -n 150 --no-pager
journalctl -k -b --no-pager | \
  grep -Ei 'amdgpu|kfd|gpu reset|MODE2|device wedged|MES failed|gpu fault'

cd /data/comfyui/models
sha256sum --check ../models.sha256
```

If a GPU reset is present, stop and reboot Proton before another job.

## Validation Evidence

The final clean-boot validation used one persistent RealVisXL process and
completed a 768x512 surface plus a 512x512 RGBA icon without kernel faults.
BiRefNet produced substantially cleaner empty transparency than Lucida on the
symbolic-star comparison. The corrected astrophysical-star result was a round
photosphere/corona, and all retained model checksums and final PNG metadata
checks passed.

Host-local representative outputs:

```text
/data/comfyui/output/Spacegate/tests/final-single-model-surface_00001_.png
/data/comfyui/output/Spacegate/tests/final-astrophysical-star-icon_00001_.png
```
