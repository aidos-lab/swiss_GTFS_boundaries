"""Shortest-path distance matrix computation on transit graphs.

Operates on a NetworkX graph and produces a numpy distance matrix
suitable for Ripser or other TDA tools.
"""

from __future__ import annotations

import numpy as np
import networkx as nx


def compute_distance_matrix(G: nx.Graph) -> np.ndarray:
    """Compute a symmetrised all-pairs shortest-path distance matrix.

    Parameters
    ----------
    G:
        A weighted NetworkX graph. Edge attribute 'weight' is used as
        the travel-time cost; missing weights default to 1.

    Returns
    -------
    ndarray of shape (n, n):
        Symmetric distance matrix where entry (i, j) is the shortest-path
        distance between nodes i and j.  Unreachable pairs are set to inf.
    """
    nodes = list(G.nodes())
    n = len(nodes)
    node_index = {node: i for i, node in enumerate(nodes)}

    path_lengths: dict = dict(nx.all_pairs_dijkstra_path_length(G, weight="weight"))

    dist = np.full((n, n), np.inf)
    np.fill_diagonal(dist, 0.0)

    for u, lengths in path_lengths.items():
        i = node_index[u]
        for v, d in lengths.items():
            j = node_index[v]
            dist[i, j] = d

    return np.maximum(dist, dist.T)
