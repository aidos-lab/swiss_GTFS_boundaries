"""Boundary shapefile management.

Downloads canton/district/commune and agglomeration boundaries from the
Swiss STAC API and ensures the correct files are present locally.
"""

from __future__ import annotations

import os
import zipfile
from pathlib import Path

import requests

from swiss_gtfs.data.gtfs_source import download_file
from swiss_gtfs.mappings.regions import SHAPEFILE_CONFIG

STAC_ADMIN_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.bfs.historisierte-administrative_grenzen_g0/items"
)
STAC_AGGLO_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.bfs.generalisierte-grenzen_agglomerationen_g1/items"
)

_SIDECAR_EXTS = [".shp", ".dbf", ".prj", ".shx", ".cpg", ".sbn", ".sbx", ".shp.xml"]


def _get_stac_asset_url(stac_url: str, id_fragment: str) -> str:
    """Query a STAC items endpoint and return the _2056.shp.zip asset href."""
    resp = requests.get(stac_url, params={"limit": 500}, timeout=30)
    resp.raise_for_status()
    for item in resp.json().get("features", []):
        if id_fragment in item.get("id", ""):
            for _, asset in item.get("assets", {}).items():
                href = asset.get("href", "")
                if href.endswith("_2056.shp.zip"):
                    return href
    raise RuntimeError(
        f"Could not find STAC item containing '{id_fragment}' at {stac_url}"
    )


def _download_admin_boundaries(geodata_dir: str) -> None:
    """Download historisierte admin boundaries (canton/district/commune)."""
    url = _get_stac_asset_url(STAC_ADMIN_URL, "1860-01-01")
    dest_dir = os.path.join(geodata_dir, "boundaries")
    os.makedirs(dest_dir, exist_ok=True)
    print("  Downloading admin boundaries ...")
    tmp = os.path.join(dest_dir, "_tmp_admin.shp.zip")
    download_file(url, tmp)
    with zipfile.ZipFile(tmp) as zf:
        zf.extractall(dest_dir)
    os.remove(tmp)


def _download_agglomeration_boundaries(geodata_dir: str) -> None:
    """Download agglomeration boundaries, renaming files to k4a24_12.*"""
    url = _get_stac_asset_url(STAC_AGGLO_URL, "2024-01-01")
    dest_dir = os.path.join(geodata_dir, "agglomerations")
    os.makedirs(dest_dir, exist_ok=True)
    print("  Downloading agglomeration boundaries ...")
    tmp = os.path.join(dest_dir, "_tmp_agglo.shp.zip")
    download_file(url, tmp)
    with zipfile.ZipFile(tmp) as zf:
        zf.extractall(dest_dir)
    os.remove(tmp)

    extracted_shps = [p for p in Path(dest_dir).glob("*.shp") if p.stem != "k4a24_12"]
    if not extracted_shps:
        return
    src_base = extracted_shps[0].stem
    for ext in _SIDECAR_EXTS:
        src = Path(dest_dir) / (src_base + ext)
        dst = Path(dest_dir) / ("k4a24_12" + ext)
        if src.exists():
            src.rename(dst)


def shapefile_path(scale: str, geodata_dir: str) -> str:
    """Return the absolute path to the expected shapefile for a scale."""
    subdir, shp_name, _ = SHAPEFILE_CONFIG[scale]
    return os.path.join(geodata_dir, subdir, shp_name)


def ensure_boundaries(scale: str, geodata_dir: str, refresh: bool = False) -> str:
    """Ensure the required boundary shapefile exists, downloading if needed.

    Returns the path to the shapefile.
    """
    shp = shapefile_path(scale, geodata_dir)

    if os.path.exists(shp) and not refresh:
        print(f"[+] Boundary shapefile present: {shp}")
        return shp

    print(f"[*] Fetching boundary data for scale='{scale}' ...")
    if scale == "agglomeration":
        _download_agglomeration_boundaries(geodata_dir)
    else:
        _download_admin_boundaries(geodata_dir)

    if not os.path.exists(shp):
        raise RuntimeError(
            f"Boundary download completed but expected file not found: {shp}"
        )
    print(f"[+] Boundary ready: {shp}")
    return shp
