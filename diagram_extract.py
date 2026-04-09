import os
import time
import argparse
import numpy as np
import networkx as nx
import gtfs2nx as gx
from ripser import ripser
from name_mappings import canton_names, agglomerations_names, district_names, comune_names

def compute_distance_matrix(G):
    nodes = list(G.nodes())
    n = len(nodes)

    # 1. All-Pairs Shortest Path
    path_lengths = dict(nx.all_pairs_dijkstra_path_length(G, weight='weight'))
    dist_matrix = np.full((n, n), np.inf)

    # 2. Fill matrix
    for i, u in enumerate(nodes):
        dist_matrix[i, i] = 0.0
        if u in path_lengths:
            for j, v in enumerate(nodes):
                if v in path_lengths[u]:
                    dist_matrix[i, j] = path_lengths[u][v]

    # 3. Symmetrize (Max)
    sym_matrix = np.maximum(dist_matrix, dist_matrix.T)
    return sym_matrix

def process_and_save_diagrams(city_key, raw_name, scale, in_base_dir, out_base_dir):
    gtfs_path = os.path.join(in_base_dir, scale, f"{city_key}.zip")
    out_dir = os.path.join(out_base_dir, scale)
    out_path = os.path.join(out_dir, f"{city_key}_diagrams.npz")

    # Skip if we already computed it (great for resuming interrupted runs!)
    if os.path.exists(out_path):
        print(f"  [>] {raw_name} already processed. Skipping.")
        return

    if not os.path.exists(gtfs_path):
        print(f"  [!] Missing GTFS file for {raw_name} ({gtfs_path}). Skipping.")
        return

    if os.path.exists(out_path):
        print(f"  [>] {raw_name} already processed. Skipping.")
        return


    print(f"\n==================================================")
    print(f"PROCESSING: {raw_name} [{scale.upper()}]")
    print(f"==================================================")

    try:
        # 1. Load Graph
        start_load = time.time()
        # time_window can be adjusted if you want the whole day instead of just morning peak
        G = gx.transit_graph(gtfs_path, time_window=('07:00', '10:00'))

        if len(G.nodes()) < 2:
            print(f"  [!] Graph too small ({len(G.nodes())} nodes). Skipping.")
            return

        print(f"  [+] Graph Loaded: {len(G.nodes())} nodes, {len(G.edges())} edges ({time.time() - start_load:.2f}s)")

        # 2. Distance Matrix
        start_dist = time.time()
        dist_matrix = compute_distance_matrix(G)
        print(f"  [+] Distance Matrix Computed ({time.time() - start_dist:.2f}s)")

        # 3. Ripser (NO THRESHOLD LIMIT)
        print(f"  [+] Running Ripser (Uncapped)...")
        start_rips = time.time()
        # By removing 'thresh', it computes up to the maximum finite distance
        rips_results = ripser(dist_matrix, distance_matrix=True, maxdim=1)
        dgms = rips_results['dgms']
        print(f"      -> Discovered {len(dgms[0])} H0 features and {len(dgms[1])} H1 loops ({time.time() - start_rips:.2f}s)")

        # 4. Save to Disk
        os.makedirs(out_dir, exist_ok=True)
        # We save H0 and H1 as distinct arrays inside the .npz file
        np.savez(out_path, h0=dgms[0], h1=dgms[1])
        print(f"  [+] Saved Diagrams to {out_path}")

    except Exception as e:
        print(f"  [XXX] ERROR processing {raw_name}: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute and save Persistence Diagrams for GTFS networks.")
    parser.add_argument('--scale', type=str, required=True, choices=['canton', 'agglomeration', 'district', 'commune', 'all'])
    parser.add_argument('--in_dir', type=str, default='Data', help="Base directory containing the filtered GTFS zips.")
    parser.add_argument('--out_dir', type=str, default='Persistence_Diagrams', help="Where to save the .npz diagram files.")

    args = parser.parse_args()

    mapping_dicts = {
        'canton': canton_names,
        'agglomeration': agglomerations_names,
        'district': district_names,
        'commune': comune_names
    }

    scales_to_run = list(mapping_dicts.keys()) if args.scale == 'all' else [args.scale]

    for current_scale in scales_to_run:
        mapping = mapping_dicts[current_scale]
        processed_raw_names = set()

        for clean_key, raw_name in mapping.items():
            if raw_name in processed_raw_names:
                continue
            processed_raw_names.add(raw_name)

            process_and_save_diagrams(clean_key, raw_name, current_scale, args.in_dir, args.out_dir)
