"""GTFS feed discovery, download, and extraction.

Handles version scraping from opentransportdata.swiss, streaming download,
and zip extraction with idempotent caching.
"""

from __future__ import annotations

import functools
import os
import re
import time
import zipfile
from pathlib import Path

import requests

GTFS_DOWNLOAD_PAGE = "https://data.opentransportdata.swiss/dataset/timetable-2026-gtfs2020"

_VERSION_RE = re.compile(r'href="(https?://[^"]*gtfs_fp\d{4}_(\d{8})\.zip)"')
_FILENAME_RE = re.compile(r'(gtfs_fp\d{4}_\d{8}\.zip)')


@functools.lru_cache(maxsize=1)
def scrape_gtfs_versions() -> list[tuple[str, str]]:
    """Scrape the dataset page and return sorted (version, url) pairs.

    version is a YYYYMMDD string derived from the filename.
    """
    resp = requests.get(GTFS_DOWNLOAD_PAGE, timeout=30)
    resp.raise_for_status()
    seen: dict[str, str] = {}
    for url, version in _VERSION_RE.findall(resp.text):
        if version not in seen:
            seen[version] = url
    return sorted(seen.items())


def list_available_versions() -> list[str]:
    """Return all GTFS version strings (YYYYMMDD) available on the dataset page."""
    return [v for v, _ in scrape_gtfs_versions()]


def download_file(url: str, dest: str, chunk_size: int = 8 * 1024 * 1024) -> None:
    """Stream-download url to dest, printing progress."""
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = 100 * downloaded / total
                    print(f"  {downloaded/1e6:.0f}/{total/1e6:.0f} MB ({pct:.0f}%)", end="\r")
    print()


def extract_gtfs(zip_path: str, dest_dir: str, force: bool = False) -> None:
    """Extract zip_path into dest_dir unless it already exists (or force=True)."""
    if os.path.isdir(dest_dir) and not force:
        print(f"[+] Already extracted: {dest_dir}")
        return
    print(f"[*] Extracting {os.path.basename(zip_path)} → {dest_dir}")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)
    print("[+] Extraction complete.")


def resolve_gtfs(
    data_dir: str = "Data",
    gtfs_version: str | None = None,
    gtfs_path: str | None = None,
    refresh_gtfs: bool = False,
) -> tuple[str, str, str]:
    """Locate or download the GTFS feed.

    Returns (zip_path, extracted_dir, version).
    zip_path is an empty string when gtfs_path points directly to a directory.
    version is YYYYMMDD when determinable, otherwise 'custom'.
    """
    if gtfs_path:
        p = os.path.abspath(gtfs_path)
        m = re.search(r'gtfs_fp\d{4}_(\d{8})', os.path.basename(p))
        version = m.group(1) if m else "custom"
        if os.path.isdir(p):
            return ("", p, version)
        if zipfile.is_zipfile(p):
            stem = os.path.splitext(os.path.basename(p))[0]
            extracted_dir = os.path.join(data_dir, stem)
            extract_gtfs(p, extracted_dir, force=refresh_gtfs)
            return (p, extracted_dir, version)
        raise ValueError(f"gtfs_path '{p}' is neither a directory nor a valid zip.")

    versions = scrape_gtfs_versions()
    if not versions:
        raise RuntimeError("No GTFS versions found on the dataset page.")

    if gtfs_version:
        matching = [(v, u) for v, u in versions if v == gtfs_version]
        if not matching:
            available = ", ".join(v for v, _ in versions)
            raise ValueError(
                f"GTFS version '{gtfs_version}' not found.\n"
                f"Available: {available}\n"
                f"Use list_available_versions() to inspect."
            )
        version, url = matching[0]
    else:
        version, url = versions[-1]

    m = _FILENAME_RE.search(url)
    if not m:
        raise RuntimeError(f"Could not parse filename from URL: {url}")
    filename = m.group(1)
    zip_path = os.path.join(data_dir, filename)

    if os.path.exists(zip_path) and not refresh_gtfs:
        print(f"[+] GTFS already present: {zip_path}")
    else:
        print(f"[*] Downloading GTFS version {version} ...")
        os.makedirs(data_dir, exist_ok=True)
        download_file(url, zip_path)
        print(f"[+] Saved to {zip_path}")

    stem = os.path.splitext(os.path.basename(zip_path))[0]
    extracted_dir = os.path.join(data_dir, stem)
    extract_gtfs(zip_path, extracted_dir, force=refresh_gtfs)

    return (zip_path, extracted_dir, version)
