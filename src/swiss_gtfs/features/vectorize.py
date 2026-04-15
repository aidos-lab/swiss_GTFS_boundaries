"""Persistence diagram vectorization.

Converts H0/H1 persistence diagrams into fixed-length numeric feature
vectors using the Persistence Statistics method (summary statistics of
births, deaths, midpoints, and lifespans).

Also provides a landscape-based vectorization and a batch loader for
building a full feature matrix across a scale's cities.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
from scipy import stats

from swiss_gtfs.features.persistence import load_diagrams, diagram_path

# ---------------------------------------------------------------------------
# Per-diagram statistics vectorization (38 features per dimension)
# ---------------------------------------------------------------------------

_N_STATS_FEATURES = 38  # 4 metrics × 9 stats + count + entropy


def _stats_for_metric(metric: np.ndarray) -> list[float]:
    """Return 9 summary statistics for a single persistence metric array."""
    if len(metric) == 1:
        v = float(metric[0])
        return [v, 0.0, v, 0.0, 0.0, v, v, v, v]
    return [
        float(np.mean(metric)),
        float(np.std(metric)),
        float(np.median(metric)),
        float(stats.iqr(metric)),
        float(np.max(metric) - np.min(metric)),
        float(np.percentile(metric, 10)),
        float(np.percentile(metric, 25)),
        float(np.percentile(metric, 75)),
        float(np.percentile(metric, 90)),
    ]


def persistence_statistics(diagram: np.ndarray, inf_cap: float = 9000.0) -> np.ndarray:
    """Vectorize a single persistence diagram using summary statistics.

    Parameters
    ----------
    diagram:
        Array of shape (n_bars, 2) with columns [birth, death].
    inf_cap:
        Value to substitute for infinite death times.

    Returns
    -------
    Feature vector of length 38.
    """
    if len(diagram) == 0:
        return np.zeros(_N_STATS_FEATURES)

    diagram = np.where(np.isinf(diagram), inf_cap, diagram).astype(float)
    births    = diagram[:, 0]
    deaths    = diagram[:, 1]
    lifespans = deaths - births
    midpoints = (births + deaths) / 2.0

    features: list[float] = []
    for metric in (births, deaths, midpoints, lifespans):
        features.extend(_stats_for_metric(metric))

    features.append(float(len(diagram)))

    total_life = float(np.sum(lifespans))
    if total_life > 0:
        p_i = lifespans / total_life
        entropy = float(-np.sum(p_i * np.log(p_i + 1e-10)))
    else:
        entropy = 0.0
    features.append(entropy)

    return np.array(features)


def vectorize_city_stats(npz_path: str | Path) -> np.ndarray:
    """Load a .npz diagram file and return a concatenated H0+H1 stats vector."""
    diagrams = load_diagrams(npz_path)
    vec_h0 = persistence_statistics(diagrams["h0"])
    vec_h1 = persistence_statistics(diagrams["h1"])
    return np.concatenate([vec_h0, vec_h1])


# ---------------------------------------------------------------------------
# Persistence landscape vectorization
# ---------------------------------------------------------------------------

def _landscape_1d(
    diagram: np.ndarray,
    t_values: np.ndarray,
    k: int = 1,
    inf_cap: float = 9000.0,
) -> np.ndarray:
    """Compute the k-th persistence landscape at sample points t_values."""
    if len(diagram) == 0:
        return np.zeros_like(t_values)

    diagram = np.where(np.isinf(diagram), inf_cap, diagram).astype(float)
    births = diagram[:, 0]
    deaths = diagram[:, 1]
    mids  = (births + deaths) / 2.0
    halfs = (deaths - births) / 2.0
    # broadcast (n_bars, 1) against (1, T) → (n_bars, T)
    tents = np.maximum(0.0, halfs[:, None] - np.abs(t_values[None, :] - mids[:, None]))
    stack = np.sort(tents, axis=0)[::-1]
    if k - 1 < len(stack):
        return stack[k - 1]
    return np.zeros_like(t_values)


def vectorize_city_landscape(
    npz_path: str | Path,
    n_points: int = 500,
    n_landscapes: int = 1,
    inf_cap: float = 9000.0,
) -> np.ndarray:
    """Vectorize H0 and H1 diagrams using persistence landscapes."""
    diagrams = load_diagrams(npz_path)

    def _landscape_vec(dgm: np.ndarray) -> np.ndarray:
        all_finite = dgm[~np.isinf(dgm).any(axis=1)] if len(dgm) > 0 else dgm
        if len(all_finite) == 0:
            return np.zeros(n_points * n_landscapes)
        t_min = float(all_finite[:, 0].min())
        t_max = float(np.where(np.isinf(all_finite[:, 1]), inf_cap, all_finite[:, 1]).max())
        t_values = np.linspace(t_min, t_max, n_points)
        parts = [_landscape_1d(dgm, t_values, k=k, inf_cap=inf_cap)
                 for k in range(1, n_landscapes + 1)]
        return np.concatenate(parts)

    vec_h0 = _landscape_vec(diagrams["h0"])
    vec_h1 = _landscape_vec(diagrams["h1"])
    return np.concatenate([vec_h0, vec_h1])


# ---------------------------------------------------------------------------
# Batch feature matrix builder
# ---------------------------------------------------------------------------

def build_feature_matrix(
    scale: str,
    diagrams_dir: str,
    method: str = "stats",
    city_keys: list[str] | None = None,
    gtfs_version: str = "",
    **kwargs,
) -> tuple[list[str], np.ndarray]:
    """Build a feature matrix for all processed cities at a scale.

    Parameters
    ----------
    scale:
        Geographic scale (e.g. 'agglomeration').
    diagrams_dir:
        Directory containing per-city .npz diagram files.
    method:
        'stats' for persistence statistics, 'landscape' for landscapes.
    city_keys:
        Optional subset of city keys; defaults to all available .npz files.
    gtfs_version:
        GTFS version string for output directory segregation.
    **kwargs:
        Passed to the chosen vectorization function.

    Returns
    -------
    (keys, matrix) where keys is a list of city key strings and matrix is
    shape (n_cities, n_features).
    """
    scale_dir = os.path.join(diagrams_dir, scale, gtfs_version) if gtfs_version else os.path.join(diagrams_dir, scale)
    if not os.path.isdir(scale_dir):
        raise FileNotFoundError(f"Diagrams directory not found: {scale_dir}")

    vectorize_fn = {
        "stats":     vectorize_city_stats,
        "landscape": vectorize_city_landscape,
    }.get(method)
    if vectorize_fn is None:
        raise ValueError(f"Unknown method '{method}'. Choose 'stats' or 'landscape'.")

    available = {
        f.replace("_diagrams.npz", "")
        for f in os.listdir(scale_dir)
        if f.endswith("_diagrams.npz")
    }
    keys_to_process = [k for k in city_keys if k in available] if city_keys else sorted(available)

    keys_out: list[str] = []
    vectors: list[np.ndarray] = []

    for city_key in keys_to_process:
        npz_path = diagram_path(city_key, scale, diagrams_dir, gtfs_version)
        try:
            vec = vectorize_fn(npz_path, **kwargs)
            vec = np.nan_to_num(vec)
            keys_out.append(city_key)
            vectors.append(vec)
        except Exception as e:
            print(f"  [!] Failed to vectorize {city_key}: {e}")

    if not vectors:
        raise RuntimeError(f"No valid feature vectors produced for scale='{scale}' and version '{gtfs_version}'.")

    return keys_out, np.array(vectors)
