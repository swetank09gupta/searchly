"""
Products config — single source of truth for valid product IDs.

Loaded from products.yml at startup (mounted from connectors/products.yml).
Falls back to a hardcoded list if the file isn't available.

Usage:
    from products_config import PRODUCTS, product_menu, validate_products
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

# Populated at startup by load()
PRODUCTS: dict[str, str] = {}   # id → description


def load(path: str | None = None) -> None:
    global PRODUCTS
    path = path or os.getenv("PRODUCTS_YML", "/app/products.yml")
    try:
        import yaml
        data = yaml.safe_load(Path(path).read_text())
        PRODUCTS = {
            pid: cfg.get("description", pid)
            for pid, cfg in data.get("products", {}).items()
        }
        log.info("Loaded %d products from %s", len(PRODUCTS), path)
    except Exception as e:
        log.warning("Could not load products.yml (%s) — using defaults", e)
        PRODUCTS = {
            "pick-assist":     "Pick Assist / OGA — operator guidance, task allocation",
            "greymatter":      "GreyMatter WES/WCS platform",
            "intralogistics":  "Intralogistics — CRN robot fleet",
            "gsb":             "GreyMatter Solution Builder",
            "rdc":             "RDC — rack-based delivery cart / sortation",
            "wms":             "WMS / ERP integrations",
            "sre":             "SRE Platform — infra, monitoring, deployment",
            "ai-ml":           "AI/ML models and analytics",
        }


def ids() -> list[str]:
    return list(PRODUCTS.keys())


def product_menu() -> str:
    """Numbered menu string for chat clarification messages."""
    lines = []
    for i, (pid, desc) in enumerate(PRODUCTS.items(), 1):
        lines.append(f"  {i}. **{pid}** — {desc}")
    return "\n".join(lines)


def validate_products(raw: list[str]) -> tuple[list[str], list[str]]:
    """
    Split a product list into (valid, unknown).
    valid   — IDs that exist in PRODUCTS
    unknown — anything that didn't match
    """
    valid, unknown = [], []
    known_ids = ids()
    for p in raw:
        if p in known_ids:
            valid.append(p)
        else:
            unknown.append(p)
    return valid, unknown


def parse_selection(text: str) -> list[str]:
    """
    Parse a user's reply to the numbered product menu.

    Accepts:
      "1, 3"              → [ids[0], ids[2]]
      "1 and 2"           → [ids[0], ids[1]]
      "pick-assist, gsb"  → ["pick-assist", "gsb"]
      "all"               → all product IDs
    """
    import re
    text = text.strip().lower()
    known = ids()

    if "all" in text:
        return known

    result = []
    # Numbered picks: "1", "2", "3"
    for m in re.finditer(r"\b(\d+)\b", text):
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(known):
            result.append(known[idx])

    # Named picks: "pick-assist", "greymatter"
    for pid in known:
        if pid in text or pid.replace("-", " ") in text:
            if pid not in result:
                result.append(pid)

    return result
