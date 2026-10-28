"""Per-run param overrides, shared by the API stand-in and the sensor."""
from __future__ import annotations


def apply_overrides(spec: dict, overrides: dict) -> dict:
    """{"farsite": {"spread_adjustment": 1.0}} -> set on every use of that block in the spec."""
    entries = list(spec.get("models", []))
    for cfg in spec.get("inputs", {}).values():
        entries += cfg.get("sources") or [cfg["source"]]
        entries += cfg.get("transforms", [])
    for e in entries:
        if e["block"] in overrides:
            e.setdefault("params", {}).update(overrides[e["block"]])
    return spec
