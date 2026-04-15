"""CLI: cluster regions from feature vectors and export geo-mapped results.

Usage:
    gtfs-analyze --scale agglomeration
    gtfs-analyze --scale canton --method agglomerative --n-clusters 4
    gtfs-analyze --scale agglomeration --method dbscan --eps 0.5 --min-samples 3
"""

from __future__ import annotations

import argparse
import os

import numpy as np
import pandas as pd

from swiss_gtfs.config import VALID_SCALES, AnalysisConfig
from swiss_gtfs.analysis.cluster import cluster_regions, cluster_to_dataframe
from swiss_gtfs.analysis.geo_join import merge_clusters, plot_cluster_map
from swiss_gtfs.mappings.regions import get_mapping


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Cluster transit regions from feature vectors.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--scale", required=True, choices=VALID_SCALES)
    p.add_argument("--gtfs-version", required=True, metavar="YYYYMMDD",
                   help="GTFS version string to identify feature sets.")
    p.add_argument("--features-dir", default="outputs/features")
    p.add_argument("--geodata-dir", default="geodata/")
    p.add_argument("--output-dir", default="outputs/analysis")
    p.add_argument("--method", default="kmeans",
                   choices=["kmeans", "agglomerative", "dbscan"])
    p.add_argument("--feature-method", default="stats",
                   choices=["stats", "landscape"],
                   help="Which feature matrix file to load.")
    p.add_argument("--n-clusters", type=int, default=5)
    p.add_argument("--random-state", type=int, default=42)
    p.add_argument("--eps", type=float, default=0.5,
                   help="DBSCAN epsilon parameter.")
    p.add_argument("--min-samples", type=int, default=5,
                   help="DBSCAN min_samples parameter.")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    npz_path = os.path.join(
        args.features_dir, args.scale, args.gtfs_version,
        f"feature_matrix_{args.feature_method}.npz"
    )
    if not os.path.exists(npz_path):
        print(f"[!] Feature matrix not found: {npz_path}")
        print("    Run 'gtfs-features' first.")
        return

    data = np.load(npz_path, allow_pickle=True)
    city_keys = list(data["keys"])
    matrix = data["matrix"]
    print(f"[+] Loaded feature matrix: {matrix.shape} for {len(city_keys)} cities (version {args.gtfs_version})")

    extra_kwargs: dict = {}
    if args.method == "dbscan":
        extra_kwargs["eps"] = args.eps
        extra_kwargs["min_samples"] = args.min_samples

    labels = cluster_regions(
        matrix,
        method=args.method,
        n_clusters=args.n_clusters,
        random_state=args.random_state,
        **extra_kwargs,
    )

    results_df = cluster_to_dataframe(city_keys, labels)
    print(f"\n[+] Cluster distribution:\n{results_df['cluster'].value_counts().to_string()}")

    mapping = get_mapping(args.scale)
    results_df["shapefile_name"] = results_df["city_key"].map(mapping)

    out_dir = os.path.join(args.output_dir, args.scale, args.gtfs_version)
    os.makedirs(out_dir, exist_ok=True)

    csv_path = os.path.join(out_dir, f"clusters_{args.method}.csv")
    results_df.to_csv(csv_path, index=False)
    print(f"[+] Cluster table → {csv_path}")

    geodata_dir = args.geodata_dir

    try:
        merged = merge_clusters(results_df, args.scale, geodata_dir)
        map_path = os.path.join(out_dir, f"map_{args.method}.html")
        m = plot_cluster_map(merged, args.scale)
        m.save(map_path)
        print(f"[+] Map → {map_path}")
    except Exception as e:
        print(f"[!] Map export failed: {e}")

    print("\n[DONE]")


if __name__ == "__main__":
    main()
