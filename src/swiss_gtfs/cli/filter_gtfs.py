"""CLI: filter GTFS feeds by Swiss geographic boundaries.

Usage:
    gtfs-filter --scale agglomeration
    gtfs-filter --scale canton --in-path Data/gtfs_fp2026_20260408 --cities zurich bern
"""

from __future__ import annotations

import argparse

from swiss_gtfs.config import VALID_SCALES
from swiss_gtfs.data.filtering import filter_scale


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Filter Swiss GTFS by geographic boundary.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scale", required=True, choices=VALID_SCALES,
                   help="Geographic scale.")
    p.add_argument("--gtfs-version", default=None, metavar="YYYYMMDD",
                   help="Pin to a specific GTFS release.")
    p.add_argument("--gtfs-path", default=None, metavar="PATH",
                   help="Use a local GTFS zip or extracted directory.")
    p.add_argument("--data-dir", default="Data",
                   help="Base output directory (files go to data-dir/{scale}/{version}/{city}.zip).")
    p.add_argument("--geodata-dir", default="geodata/",
                   help="Root directory for boundary shapefiles.")
    p.add_argument("--cities", nargs="+", metavar="CITY",
                   help="Specific city keys to process (default: all).")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    
    from swiss_gtfs.data.gtfs_source import resolve_gtfs
    _, extracted_dir, gtfs_version = resolve_gtfs(
        data_dir=args.data_dir,
        gtfs_version=args.gtfs_version,
        gtfs_path=args.gtfs_path,
    )
    print(f"[+] GTFS version: {gtfs_version}  ({extracted_dir})")

    filter_scale(
        scale=args.scale,
        in_dir=extracted_dir,
        out_dir=args.data_dir,
        geodata_dir=args.geodata_dir,
        cities=args.cities,
        gtfs_version=gtfs_version,
    )


if __name__ == "__main__":
    main()
