"""CLI: build persistence diagrams and feature vectors from transit graphs.

Loads filtered GTFS zips (or existing GraphML), computes shortest-path
distance matrices, runs Ripser, then vectorizes the resulting diagrams.

Usage:
    gtfs-features --scale agglomeration
    gtfs-features --scale canton --method landscape --n-points 500
    gtfs-features --scale agglomeration --cities zurich bern
"""

from __future__ import annotations

import argparse
import os
import time

import gtfs2nx as gx
import numpy as np

from swiss_gtfs.config import VALID_SCALES, FeatureConfig
from swiss_gtfs.features.persistence import compute_and_save
from swiss_gtfs.features.vectorize import build_feature_matrix
from swiss_gtfs.graphs.distances import compute_distance_matrix
from swiss_gtfs.mappings.regions import resolve_cities


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Compute persistence diagrams and feature vectors from GTFS graphs.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scale", required=True, choices=VALID_SCALES)
    p.add_argument("--gtfs-version", required=True, metavar="YYYYMMDD",
                   help="GTFS version string (required to locate the correct filtered zips and save features).")
    p.add_argument("--cities", nargs="+", metavar="CITY",
                   help="Specific city keys (default: all).")
    p.add_argument("--gtfs-dir", default="Data",
                   help="Directory containing filtered GTFS zips ({scale}/{version}/{city}.zip).")
    p.add_argument("--diagrams-dir", default="outputs/diagrams")
    p.add_argument("--features-dir", default="outputs/features")
    p.add_argument("--start-time", default="07:00:00", help="Time-window start (HH:MM:SS).")
    p.add_argument("--end-time", default="10:00:00",   help="Time-window end (HH:MM:SS).")
    p.add_argument("--method", default="stats", choices=["stats", "landscape"],
                   help="Vectorization method.")
    p.add_argument("--n-points", type=int, default=500,
                   help="Sample points for landscape vectorization.")
    p.add_argument("--force", action="store_true",
                   help="Recompute even if .npz already exists.")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    skip_existing = not args.force

    city_pairs = resolve_cities(args.scale, args.cities)
    total = len(city_pairs)
    print(f"\n[*] {total} cities at scale='{args.scale}' (version {args.gtfs_version})")

    for i, (city_key, raw_name) in enumerate(city_pairs, 1):
        gtfs_path = os.path.join(args.gtfs_dir, args.scale, args.gtfs_version, f"{city_key}.zip")
        print(f"\n[{i}/{total}] {raw_name} ({city_key})")

        if not os.path.exists(gtfs_path):
            print(f"  [!] Missing filtered GTFS: {gtfs_path}. Skipping.")
            continue

        try:
            t0 = time.time()
            G = gx.transit_graph(gtfs_path, time_window=(args.start_time, args.end_time))
            if len(G.nodes()) < 2:
                print(f"  [!] Graph too small ({len(G.nodes())} nodes). Skipping.")
                continue
            print(f"  [+] Graph: {len(G.nodes())} nodes, {len(G.edges())} edges ({time.time()-t0:.1f}s)")

            t1 = time.time()
            dist_matrix = compute_distance_matrix(G)
            print(f"  [+] Distance matrix ({time.time()-t1:.1f}s)")

            compute_and_save(
                city_key, args.scale, dist_matrix,
                args.diagrams_dir, skip_existing=skip_existing, gtfs_version=args.gtfs_version
            )
        except Exception as e:
            print(f"  [!] Error for '{city_key}': {e}")

    print(f"\n[*] Vectorizing diagrams (method={args.method}) ...")
    try:
        kwargs = {}
        if args.method == "landscape":
            kwargs["n_points"] = args.n_points

        keys, matrix = build_feature_matrix(
            scale=args.scale,
            diagrams_dir=args.diagrams_dir,
            method=args.method,
            city_keys=[k for k, _ in city_pairs] if args.cities else None,
            gtfs_version=args.gtfs_version,
            **kwargs,
        )
        print(f"[+] Feature matrix shape: {matrix.shape}")

        out_dir = os.path.join(args.features_dir, args.scale, args.gtfs_version)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"feature_matrix_{args.method}.npz")
        np.savez(out_path, keys=keys, matrix=matrix)
        print(f"[+] Saved → {out_path}")
    except Exception as e:
        print(f"[!] Vectorization failed: {e}")

    print("\n[DONE]")


if __name__ == "__main__":
    main()
