"""CLI: download GTFS, ensure boundaries, build city2graph transit graphs.

Usage:
    gtfs-build --scale agglomeration
    gtfs-build --scale canton --gtfs-version 20260408
    gtfs-build --scale agglomeration --cities zurich bern --skip-filter
    gtfs-build --list-versions
"""

from __future__ import annotations

import argparse
import os

from swiss_gtfs.config import VALID_SCALES, GraphConfig
from swiss_gtfs.data.boundaries import ensure_boundaries
from swiss_gtfs.data.filtering import filter_gtfs_city
from swiss_gtfs.data.gtfs_source import list_available_versions, resolve_gtfs
from swiss_gtfs.graphs.build import build_summary_graph
from swiss_gtfs.graphs.io import outputs_exist, save_graph_artifacts
from swiss_gtfs.mappings.regions import resolve_cities


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build city2graph transit graphs from Swiss GTFS data.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scale", choices=VALID_SCALES,
                   help="Geographic scale.")
    p.add_argument("--cities", nargs="+", metavar="CITY",
                   help="Specific city keys to process (default: all).")
    p.add_argument("--start-time", default="07:00:00",
                   help="Time-window start (HH:MM:SS).")
    p.add_argument("--end-time", default="10:00:00",
                   help="Time-window end (HH:MM:SS).")
    p.add_argument("--calendar-start", default=None, metavar="YYYYMMDD")
    p.add_argument("--calendar-end", default=None, metavar="YYYYMMDD")
    p.add_argument("--gtfs-version", default=None, metavar="YYYYMMDD",
                   help="Pin to a specific GTFS release.")
    p.add_argument("--gtfs-path", default=None, metavar="PATH",
                   help="Use a local GTFS zip or extracted directory.")
    p.add_argument("--list-versions", action="store_true",
                   help="Print available GTFS versions and exit.")
    p.add_argument("--data-dir", default="Data")
    p.add_argument("--geodata-dir", default="geodata/")
    p.add_argument("--output-dir", default="outputs/graphs")
    p.add_argument("--refresh-gtfs", action="store_true")
    p.add_argument("--refresh-boundaries", action="store_true")
    p.add_argument("--skip-filter", action="store_true",
                   help="Reuse existing filtered zip when present.")
    p.add_argument("--save-format", default="both",
                   choices=["gpkg", "graphml", "both"])
    p.add_argument("--undirected", action="store_true")
    p.add_argument("--force", action="store_true",
                   help="Overwrite existing graph outputs.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    if args.list_versions:
        versions = list_available_versions()
        if not versions:
            print("No versions found on the dataset page.")
            return
        for v in versions[:-1]:
            print(f"  {v}")
        print(f"  {versions[-1]}   <-- latest")
        return

    if not args.scale:
        print("Error: --scale is required (unless --list-versions is used).")
        return

    geodata_dir = args.geodata_dir
    cfg = GraphConfig.from_args(args)

    _, extracted_dir, gtfs_version = resolve_gtfs(
        data_dir=args.data_dir,
        gtfs_version=args.gtfs_version,
        gtfs_path=args.gtfs_path,
        refresh_gtfs=args.refresh_gtfs,
    )
    print(f"[+] GTFS version: {gtfs_version}  ({extracted_dir})")

    ensure_boundaries(
        scale=args.scale,
        geodata_dir=geodata_dir,
        refresh=args.refresh_boundaries,
    )

    city_pairs = resolve_cities(args.scale, args.cities)
    total = len(city_pairs)
    print(f"\n[*] {total} cities to process at scale='{args.scale}' (version {gtfs_version})\n")

    filtered_dir = os.path.join(args.data_dir, args.scale, gtfs_version)
    os.makedirs(filtered_dir, exist_ok=True)

    for i, (city_key, raw_name) in enumerate(city_pairs, 1):
        print(f"[{i}/{total}] {raw_name} ({city_key})")

        filtered_path = os.path.join(filtered_dir, f"{city_key}.zip")

        if args.skip_filter and os.path.exists(filtered_path):
            print(f"  [>] Using existing filtered zip: {filtered_path}")
        else:
            try:
                filter_gtfs_city(
                    city_key, args.scale, extracted_dir,
                    filtered_path, geodata_dir=geodata_dir,
                )
            except Exception as e:
                print(f"  [!] Filter error for '{city_key}': {e}")
                continue

        if cfg.skip_existing and outputs_exist(city_key, args.scale, cfg.output_dir, cfg.save_format, gtfs_version):
            print(f"  [>] Outputs already exist. Skipping.")
            continue

        try:
            nodes_gdf, edges_gdf = build_summary_graph(
                filtered_path,
                start_time=cfg.start_time,
                end_time=cfg.end_time,
                calendar_start=cfg.calendar_start,
                calendar_end=cfg.calendar_end,
                directed=cfg.directed,
            )
            print(f"  [+] Graph: {len(nodes_gdf)} nodes, {len(edges_gdf)} edges")
            save_graph_artifacts(city_key, args.scale, nodes_gdf, edges_gdf, cfg, gtfs_version)
        except Exception as e:
            print(f"  [!] Error for '{city_key}': {e}")

    print(f"\n[DONE] {total} cities processed.")
    print(f"[DONE] Output: {os.path.join(cfg.output_dir, args.scale, gtfs_version)}/")


if __name__ == "__main__":
    main()
