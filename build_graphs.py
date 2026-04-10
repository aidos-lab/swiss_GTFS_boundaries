"""
build_graphs.py — Swiss GTFS transit graph pipeline using city2graph.

Downloads the latest (or a pinned) GTFS feed from opentransportdata.swiss,
ensures boundary shapefiles are present, filters the GTFS by geographic scale,
builds a transit summary graph using city2graph, and saves the output.

Usage:
    python build_graphs.py --scale agglomeration
    python build_graphs.py --scale canton --gtfs-version 20260408
    python build_graphs.py --list-versions
    python build_graphs.py --scale agglomeration --cities zurich bern --skip-filter
"""

import argparse
import json
import os
import re
import warnings
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import networkx as nx
import requests
import city2graph as c2g

from filter_by_geometry_scale import filter_fun

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
GTFS_DOWNLOAD_PAGE = "https://data.opentransportdata.swiss/dataset/timetable-2026-gtfs2020"
STAC_ADMIN_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.bfs.historisierte-administrative_grenzen_g0/items"
)
STAC_AGGLO_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.bfs.generalisierte-grenzen_agglomerationen_g1/items"
)

# Expected shapefile path (subdir, filename) relative to geodata_dir
_REQUIRED_SHP = {
    "canton":        ("boundaries",    "Cantons_G0_18600101.shp"),
    "district":      ("boundaries",    "Districts_G0_18600101.shp"),
    "commune":       ("boundaries",    "Communes_G0_18600101.shp"),
    "agglomeration": ("agglomerations", "k4a24_12.shp"),
}

_SIDECAR_EXTS = [".shp", ".dbf", ".prj", ".shx", ".cpg", ".sbn", ".sbx", ".shp.xml"]

# GraphML only supports these Python types as attribute values
_SCALAR_TYPES = (str, int, float, bool)


# ===========================================================================
# GTFS version discovery
# ===========================================================================

def _scrape_gtfs_versions() -> list[tuple[str, str]]:
    """Scrape the dataset page and return sorted (version, url) pairs.

    version is a YYYYMMDD string derived from the filename.
    """
    resp = requests.get(GTFS_DOWNLOAD_PAGE, timeout=30)
    resp.raise_for_status()
    # Match any href pointing to a versioned GTFS zip
    matches = re.findall(
        r'href="(https?://[^"]*gtfs_fp\d{4}_(\d{8})\.zip)"',
        resp.text,
    )
    seen: dict[str, str] = {}
    for url, version in matches:
        if version not in seen:
            seen[version] = url
    return sorted(seen.items())  # ascending by date string


def list_available_versions() -> list[str]:
    """Return all GTFS version strings (YYYYMMDD) available on the dataset page."""
    return [v for v, _ in _scrape_gtfs_versions()]


# ===========================================================================
# GTFS download / path resolution
# ===========================================================================

def _download_file(url: str, dest: str) -> None:
    """Stream-download url to dest in 8 MB chunks, printing progress."""
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        total = int(r.headers.get("content-length", 0))
        downloaded = 0
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = 100 * downloaded / total
                    print(f"  {downloaded/1e6:.0f}/{total/1e6:.0f} MB ({pct:.0f}%)", end="\r")
    print()


def _extract_if_needed(zip_path: str, dest_dir: str, force: bool = False) -> None:
    """Extract zip_path into dest_dir unless it already exists (or force=True)."""
    if os.path.isdir(dest_dir) and not force:
        print(f"[+] Already extracted: {dest_dir}")
        return
    print(f"[*] Extracting {os.path.basename(zip_path)} → {dest_dir}")
    with zipfile.ZipFile(zip_path) as zf:
        zf.extractall(dest_dir)
    print("[+] Extraction complete.")


def resolve_gtfs_path(args: argparse.Namespace) -> tuple[str, str, str]:
    """Locate or download the GTFS feed.

    Returns (zip_path, extracted_dir, version).
    zip_path is empty string when --gtfs-path points to a directory.
    version is YYYYMMDD when determinable, otherwise 'custom'.
    """
    data_dir = args.data_dir

    # --- User supplied a local path ---
    if args.gtfs_path:
        p = os.path.abspath(args.gtfs_path)
        m = re.search(r'gtfs_fp\d{4}_(\d{8})', os.path.basename(p))
        version = m.group(1) if m else "custom"
        if os.path.isdir(p):
            return ("", p, version)
        if zipfile.is_zipfile(p):
            stem = os.path.splitext(os.path.basename(p))[0]
            extracted_dir = os.path.join(data_dir, stem)
            _extract_if_needed(p, extracted_dir, args.refresh_gtfs)
            return (p, extracted_dir, version)
        raise ValueError(f"--gtfs-path '{p}' is neither a directory nor a valid zip.")

    # --- Discover versions from the dataset page ---
    versions = _scrape_gtfs_versions()
    if not versions:
        raise RuntimeError("No GTFS versions found on the dataset page.")

    if args.gtfs_version:
        matching = [(v, u) for v, u in versions if v == args.gtfs_version]
        if not matching:
            available = ", ".join(v for v, _ in versions)
            raise ValueError(
                f"GTFS version '{args.gtfs_version}' not found on the dataset page.\n"
                f"Available: {available}\n"
                f"Use --list-versions to inspect."
            )
        version, url = matching[0]
    else:
        version, url = versions[-1]  # newest

    filename = re.search(r'(gtfs_fp\d{4}_\d{8}\.zip)', url).group(1)
    zip_path = os.path.join(data_dir, filename)

    if os.path.exists(zip_path) and not args.refresh_gtfs:
        print(f"[+] GTFS already present: {zip_path}")
    else:
        print(f"[*] Downloading GTFS version {version} ...")
        os.makedirs(data_dir, exist_ok=True)
        _download_file(url, zip_path)
        print(f"[+] Saved to {zip_path}")

    stem = os.path.splitext(os.path.basename(zip_path))[0]
    extracted_dir = os.path.join(data_dir, stem)
    _extract_if_needed(zip_path, extracted_dir, args.refresh_gtfs)

    return (zip_path, extracted_dir, version)


# ===========================================================================
# Boundary shapefiles
# ===========================================================================

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
    """Download historisierte admin boundaries (canton/district/commune) from STAC."""
    url = _get_stac_asset_url(STAC_ADMIN_URL, "1860-01-01")
    dest_dir = os.path.join(geodata_dir, "boundaries")
    os.makedirs(dest_dir, exist_ok=True)
    print(f"  Downloading admin boundaries ...")
    tmp = os.path.join(dest_dir, "_tmp_admin.shp.zip")
    _download_file(url, tmp)
    with zipfile.ZipFile(tmp) as zf:
        zf.extractall(dest_dir)
    os.remove(tmp)


def _download_agglomeration_boundaries(geodata_dir: str) -> None:
    """Download agglomeration boundaries from STAC, renamed to k4a24_12.*"""
    url = _get_stac_asset_url(STAC_AGGLO_URL, "2024-01-01")
    dest_dir = os.path.join(geodata_dir, "agglomerations")
    os.makedirs(dest_dir, exist_ok=True)
    print(f"  Downloading agglomeration boundaries ...")
    tmp = os.path.join(dest_dir, "_tmp_agglo.shp.zip")
    _download_file(url, tmp)
    with zipfile.ZipFile(tmp) as zf:
        zf.extractall(dest_dir)
    os.remove(tmp)

    # Rename extracted files to the name filter_fun expects: k4a24_12.*
    extracted_shps = [p for p in Path(dest_dir).glob("*.shp") if p.stem != "k4a24_12"]
    if not extracted_shps:
        return  # already correctly named
    src_base = extracted_shps[0].stem
    for ext in _SIDECAR_EXTS:
        src = Path(dest_dir) / (src_base + ext)
        dst = Path(dest_dir) / ("k4a24_12" + ext)
        if src.exists():
            src.rename(dst)


def ensure_boundaries(args: argparse.Namespace) -> None:
    """Ensure the required boundary shapefile exists, downloading if needed."""
    subdir, shp_name = _REQUIRED_SHP[args.scale]
    shp_path = os.path.join(args.geodata_dir, subdir, shp_name)

    if os.path.exists(shp_path) and not args.refresh_boundaries:
        print(f"[+] Boundary shapefile present: {shp_path}")
        return

    print(f"[*] Fetching boundary data for scale='{args.scale}' ...")
    if args.scale == "agglomeration":
        _download_agglomeration_boundaries(args.geodata_dir)
    else:
        _download_admin_boundaries(args.geodata_dir)

    if not os.path.exists(shp_path):
        raise RuntimeError(
            f"Boundary download completed but expected file not found: {shp_path}"
        )
    print(f"[+] Boundary ready: {shp_path}")


# ===========================================================================
# GTFS filtering
# ===========================================================================

def run_filtering(
    city_key: str,
    scale: str,
    extracted_dir: str,
    data_dir: str,
    geodata_dir: str,
    skip_filter: bool,
) -> str | None:
    """Filter raw GTFS to a single city boundary and return the output zip path.

    geodata_dir must end with os.sep (filter_fun uses string concatenation).
    Returns None on error.
    """
    scale_dir = os.path.join(data_dir, scale)
    os.makedirs(scale_dir, exist_ok=True)
    out_path = os.path.join(scale_dir, f"{city_key}.zip")

    if skip_filter and os.path.exists(out_path):
        print(f"  [>] Using existing filtered zip: {out_path}")
        return out_path

    try:
        filter_fun(city_key, scale, extracted_dir, out_path, geodata_dir_path=geodata_dir)
        return out_path
    except Exception as e:
        print(f"  [!] Filter error for '{city_key}': {e}")
        return None


# ===========================================================================
# GraphML sanitization
# ===========================================================================

def _sanitize_dict(d: dict) -> None:
    """Convert non-scalar values in d to GraphML-compatible types, in-place."""
    # Split pos tuple (lon, lat) into scalar x/y attributes
    if "pos" in d and isinstance(d["pos"], (tuple, list)) and len(d["pos"]) == 2:
        d["x"] = float(d["pos"][0])
        d["y"] = float(d["pos"][1])
        del d["pos"]
    for k in list(d):
        v = d[k]
        if v is None:
            d[k] = ""
        elif not isinstance(v, _SCALAR_TYPES):
            d[k] = str(v)


def sanitize_for_graphml(G: nx.Graph) -> None:
    """Mutate G in-place so all node/edge/graph attributes are GraphML-safe."""
    for _, node_data in G.nodes(data=True):
        _sanitize_dict(node_data)
    for _, _, edge_data in G.edges(data=True):
        _sanitize_dict(edge_data)
    _sanitize_dict(G.graph)


# ===========================================================================
# Graph construction and saving
# ===========================================================================

def build_and_save_graph(
    city_key: str,
    scale: str,
    filtered_zip_path: str,
    args: argparse.Namespace,
    gtfs_version: str,
) -> None:
    """Build a transit summary graph and save to disk.

    Outputs per city:
      Graphs/{scale}/{city_key}.gpkg     — nodes + edges GeoPackage layers
      Graphs/{scale}/{city_key}.graphml  — NetworkX GraphML
      Graphs/{scale}/{city_key}.json     — reproducibility metadata sidecar
    """
    out_dir = os.path.join(args.output_dir, scale)
    os.makedirs(out_dir, exist_ok=True)

    gpkg_path    = os.path.join(out_dir, f"{city_key}.gpkg")
    graphml_path = os.path.join(out_dir, f"{city_key}.graphml")
    meta_path    = os.path.join(out_dir, f"{city_key}.json")

    directed     = not args.undirected
    need_gpkg    = args.save_format in ("gpkg", "both")
    need_graphml = args.save_format in ("graphml", "both")

    # Skip if all requested outputs already exist
    gpkg_done    = not need_gpkg    or os.path.exists(gpkg_path)
    graphml_done = not need_graphml or os.path.exists(graphml_path)
    if gpkg_done and graphml_done:
        print(f"  [>] Outputs already exist. Skipping. (delete to regenerate)")
        return

    # Suppress cosmetic CRS warnings from city2graph internals
    warnings.filterwarnings("ignore", category=UserWarning, module="city2graph")

    con = c2g.load_gtfs(filtered_zip_path)

    nodes_gdf, edges_gdf = c2g.travel_summary_graph(
        con,
        start_time=args.start_time,
        end_time=args.end_time,
        calendar_start=args.calendar_start or None,
        calendar_end=args.calendar_end or None,
        as_nx=False,
        directed=directed,
        use_frequencies=True,
    )

    if len(nodes_gdf) < 2 or len(edges_gdf) == 0:
        print(
            f"  [!] Graph too small "
            f"({len(nodes_gdf)} nodes, {len(edges_gdf)} edges). Skipping."
        )
        return

    print(f"  [+] Graph: {len(nodes_gdf)} nodes, {len(edges_gdf)} edges")

    if need_gpkg and not gpkg_done:
        nodes_gdf.reset_index().to_file(gpkg_path, layer="nodes", driver="GPKG")
        edges_gdf.reset_index().to_file(gpkg_path, layer="edges", driver="GPKG")
        print(f"  [+] GeoPackage: {gpkg_path}")

    if need_graphml and not graphml_done:
        G = c2g.gdf_to_nx(nodes=nodes_gdf, edges=edges_gdf, keep_geom=False, directed=directed)
        sanitize_for_graphml(G)
        nx.write_graphml(G, graphml_path)
        print(f"  [+] GraphML:    {graphml_path}")

    # Metadata sidecar — always written / overwritten
    meta = {
        "city_key":       city_key,
        "scale":          scale,
        "gtfs_version":   gtfs_version,
        "gtfs_source":    GTFS_DOWNLOAD_PAGE,
        "start_time":     args.start_time,
        "end_time":       args.end_time,
        "calendar_start": args.calendar_start,
        "calendar_end":   args.calendar_end,
        "directed":       directed,
        "n_nodes":        len(nodes_gdf),
        "n_edges":        len(edges_gdf),
        "built_at":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)


# ===========================================================================
# CLI
# ===========================================================================

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build city2graph transit graphs from Swiss GTFS data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--scale", required=True,
        choices=["canton", "agglomeration", "district", "commune"],
        help="Geographic scale.",
    )
    p.add_argument("--cities", nargs="+", metavar="CITY",
                   help="Specific city keys to process (default: all).")
    p.add_argument("--start-time", default="07:00:00",
                   help="Time-window start (HH:MM:SS).")
    p.add_argument("--end-time", default="10:00:00",
                   help="Time-window end (HH:MM:SS).")
    p.add_argument("--calendar-start", default=None, metavar="YYYYMMDD",
                   help="Calendar window start date.")
    p.add_argument("--calendar-end", default=None, metavar="YYYYMMDD",
                   help="Calendar window end date.")
    p.add_argument("--gtfs-version", default=None, metavar="YYYYMMDD",
                   help="Pin to a specific GTFS release (e.g. 20260408).")
    p.add_argument("--gtfs-path", default=None, metavar="PATH",
                   help="Use a local GTFS zip or extracted directory (skips download).")
    p.add_argument("--list-versions", action="store_true",
                   help="Print all available GTFS versions and exit.")
    p.add_argument("--data-dir", default="Data",
                   help="Directory for GTFS data.")
    p.add_argument("--geodata-dir", default="geodata",
                   help="Directory containing boundary shapefiles.")
    p.add_argument("--output-dir", default="Graphs",
                   help="Output directory for graph files.")
    p.add_argument("--refresh-gtfs", action="store_true",
                   help="Re-download GTFS even if already present.")
    p.add_argument("--refresh-boundaries", action="store_true",
                   help="Re-download boundary shapefiles.")
    p.add_argument("--skip-filter", action="store_true",
                   help="Skip GTFS filtering if filtered zip already exists.")
    p.add_argument("--save-format", default="both",
                   choices=["gpkg", "graphml", "both"],
                   help="Output format.")
    p.add_argument("--undirected", action="store_true",
                   help="Build undirected graph (default: directed).")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    # --list-versions: print and exit
    if args.list_versions:
        versions = list_available_versions()
        if not versions:
            print("No versions found on the dataset page.")
            return
        print("Available GTFS versions:")
        for v in versions[:-1]:
            print(f"  {v}")
        print(f"  {versions[-1]}   <-- latest")
        return

    # Fix cwd so filter_fun's hardcoded relative 'gtfs_temp/' path resolves correctly
    os.chdir(os.path.dirname(os.path.abspath(__file__)))

    # filter_fun builds paths via string concat: geodata_dir + "boundaries/" + ...
    if not args.geodata_dir.endswith(os.sep):
        args.geodata_dir += os.sep

    # ---- Step 1: Resolve GTFS --------------------------------------------------
    _, extracted_dir, gtfs_version = resolve_gtfs_path(args)
    print(f"[+] GTFS version: {gtfs_version}  ({extracted_dir})")

    # ---- Step 2: Boundary shapefiles -------------------------------------------
    ensure_boundaries(args)

    # ---- Step 3: City name mappings --------------------------------------------
    from name_mappings import (
        canton_names, agglomerations_names, district_names, comune_names,
    )
    mapping = {
        "canton":        canton_names,
        "agglomeration": agglomerations_names,
        "district":      district_names,
        "commune":       comune_names,
    }[args.scale]

    if args.cities:
        city_keys = []
        for c in args.cities:
            if c in mapping:
                city_keys.append(c)
            else:
                print(f"[!] Unknown city key '{c}' for scale '{args.scale}'. Skipping.")
    else:
        city_keys = list(mapping.keys())

    # Deduplicate — same logic as filter_by_geometry_scale.py
    processed_raw: set[str] = set()
    cities_to_process: list[tuple[str, str]] = []
    for city_key in city_keys:
        raw_name = mapping[city_key]
        if raw_name in processed_raw:
            continue
        processed_raw.add(raw_name)
        cities_to_process.append((city_key, raw_name))

    total = len(cities_to_process)
    print(f"\n[*] {total} cities to process at scale='{args.scale}' (version {gtfs_version})\n")

    for i, (city_key, raw_name) in enumerate(cities_to_process, 1):
        print(f"[{i}/{total}] {raw_name} ({city_key})")

        filtered_path = run_filtering(
            city_key, args.scale, extracted_dir,
            args.data_dir, args.geodata_dir, args.skip_filter,
        )
        if filtered_path is None:
            continue

        try:
            build_and_save_graph(city_key, args.scale, filtered_path, args, gtfs_version)
        except Exception as e:
            print(f"  [!] Unhandled error for '{city_key}': {e}")

    print(f"\n[DONE] {total} cities processed.")
    print(f"[DONE] Output directory: {os.path.join(args.output_dir, args.scale)}/")


if __name__ == "__main__":
    main()
