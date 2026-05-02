"""Shortest-path distance matrix computation on transit graphs.

Operates on a NetworkX graph and produces a numpy distance matrix
suitable for Ripser or other TDA tools.
"""

from __future__ import annotations

import math

import numpy as np
import networkx as nx


def _edge_iter_with_keys(G: nx.Graph):
    """Yield edges as (u, v, key, data) for Graph/DiGraph/MultiGraph."""
    if G.is_multigraph():
        yield from G.edges(keys=True, data=True)
    else:
        for u, v, data in G.edges(data=True):
            yield u, v, None, data


def _coerce_edge_weight(data: dict) -> float:
    """Return a finite non-negative edge weight from known time columns."""
    for col in ("weight", "travel_time_sec", "travel_time", "min_transfer_time"):
        raw = data.get(col)

        if raw in (None, ""):
            continue

        try:
            value = float(raw)
        except (TypeError, ValueError):
            continue

        if math.isfinite(value):
            return value

    raise ValueError(f"Could not find a finite edge weight in attrs={data}")


def compute_distance_matrix(G: nx.Graph) -> np.ndarray:
    """Compute a symmetrised all-pairs shortest-path distance matrix."""

    G = G.copy()

    bad_edges = []

    for u, v, key, data in _edge_iter_with_keys(G):
        try:
            weight = _coerce_edge_weight(data)
        except ValueError as exc:
            bad_edges.append((u, v, key, str(exc)))
            continue

        if weight < 0:
            bad_edges.append((u, v, key, f"negative weight={weight}"))
            continue

        data["weight"] = weight

    if bad_edges:
        examples = "\n".join(str(edge) for edge in bad_edges[:20])
        raise ValueError(
            f"Invalid edge weights found: {len(bad_edges)} bad edges. "
            f"First examples:\n{examples}"
        )

    nodes = list(G.nodes())
    n = len(nodes)
    node_index = {node: i for i, node in enumerate(nodes)}

    path_lengths = dict(nx.all_pairs_dijkstra_path_length(G, weight="weight"))

    dist = np.full((n, n), np.inf)
    np.fill_diagonal(dist, 0.0)

    for u, lengths in path_lengths.items():
        i = node_index[u]
        for v, d in lengths.items():
            j = node_index[v]
            dist[i, j] = d

    return np.maximum(dist, dist.T)
