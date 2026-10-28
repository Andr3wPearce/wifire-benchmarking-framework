"""Preview images of a Prediction, shown on the asset in the Dagster UI.

Rendered by the framework (not by each model block) so every model's output is
drawn the same way: same colour maps, same fixed scales, same layout. That is
what makes previews of different partitions comparable at a glance.

Also written next to the rasters as preview.png.
"""
from __future__ import annotations

import base64
import io
import json
from pathlib import Path

import numpy as np

# fixed scales so colours mean the same thing in every run; fireline intensity spans
# orders of magnitude (grass ~100s, crown fire 10,000+ kW/m), so it's log-scaled
FIRELINE_INTENSITY_RANGE_KW_M = (10.0, 10_000.0)


def render_prediction(pred_dir: Path, duration_min: float | None = None) -> tuple[bytes, Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import LogNorm
    import rasterio

    pred_dir = Path(pred_dir)
    with rasterio.open(pred_dir / "time_of_arrival.tif") as src:
        toa = src.read(1).astype("float32")
        b = src.bounds
    extent_km = [0, (b.right - b.left) / 1000, 0, (b.top - b.bottom) / 1000]   # north-up
    tmax = duration_min or (float(np.nanmax(toa)) if np.isfinite(toa).any() else 1.0)
    fli_path = pred_dir / "fireline_intensity.tif"
    fli = rasterio.open(fli_path).read(1).astype("float32") if fli_path.exists() else None
    burned = np.isfinite(toa)

    panels = 2 if fli is not None else 1
    fig, axes = plt.subplots(1, panels, figsize=(4.2 * panels, 3.8), dpi=90, squeeze=False)
    ax = axes[0][0]
    im = ax.imshow(np.where(burned, toa / 60.0, np.nan), cmap="inferno_r", vmin=0, vmax=tmax / 60.0,
                   extent=extent_km)
    fig.colorbar(im, ax=ax, label="hours after ignition", shrink=0.85)
    ax.set_title("time of arrival")
    if fli is not None:
        ax = axes[0][1]
        lo, hi = FIRELINE_INTENSITY_RANGE_KW_M
        im = ax.imshow(np.where(burned, np.clip(fli, lo, hi), np.nan), cmap="YlOrRd",
                       norm=LogNorm(vmin=lo, vmax=hi), extent=extent_km)
        fig.colorbar(im, ax=ax, label="kW/m (log scale)", shrink=0.85)
        ax.set_title("fireline intensity")
    for a in axes[0]:
        if burned.any():
            a.contour(burned.astype(float), levels=[0.5], colors="k", linewidths=0.6,
                      extent=extent_km, origin="upper")
        a.set_xlabel("km east"); a.set_ylabel("km north"); a.set_aspect("equal")
        a.tick_params(labelsize=7)
    summary = pred_dir / "summary.json"
    if summary.exists():
        s = json.loads(summary.read_text())
        fig.suptitle(f"{s.get('burned_area_ha', '?')} ha burned in {tmax / 60:g} h", fontsize=10)
    fig.tight_layout()
    buf = io.BytesIO()
    fig.savefig(buf, format="png")
    plt.close(fig)
    png = buf.getvalue()
    out = pred_dir / "preview.png"
    out.write_bytes(png)
    return png, out


def preview_markdown(png: bytes) -> str:
    """Markdown with the image inline: Dagster shows it on the materialization."""
    return f"![prediction preview](data:image/png;base64,{base64.b64encode(png).decode()})"
