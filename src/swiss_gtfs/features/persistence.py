"""Persistent homology extraction using Ripser.

Computes H0 and H1 persistence diagrams from a pairwise distance matrix
and saves them as .npz files for downstream vectorization.
"""

from __future__ import annotations

import os
from pathlib import Path

import numpy as np
from ripser import ripser


def compute_persistence_diagrams(
    dist_matrix: np.ndarray,
    maxdim: int = 1,
    thresh: float | None = None,
) -> dict[str, np.ndarray]:
    """Run Ripser on a distance matrix and return H0/H1 diagrams.

    Parameters
    ----------
    dist_matrix:
        Square symmetric pairwise distance matrix.
    maxdim:
        Maximum homology dimension to compute (default: 1 for H0 + H1).
    thresh:
        Optional filtration threshold; None computes up to the maximum
        finite distance (recommended for uncapped persistence).

    Returns
    -------
    dict with keys 'h0' and 'h1', each a numpy array of shape (n_bars, 2).
    """
    kwargs: dict = {"distance_matrix": True, "maxdim": maxdim}
    if thresh is not None:
        kwargs["thresh"] = thresh

    results = ripser(dist_matrix, **kwargs)
    dgms = results["dgms"]
    return {"h0": dgms[0], "h1": dgms[1]}


def save_diagrams(diagrams: dict[str, np.ndarray], out_path: str | Path) -> None:
    """Save H0/H1 diagrams to a .npz file."""
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    np.savez(str(out_path), h0=diagrams["h0"], h1=diagrams["h1"])


def load_diagrams(path: str | Path) -> dict[str, np.ndarray]:
    """Load H0/H1 diagrams from a .npz file."""
    data = np.load(str(path))
    return {"h0": data["h0"], "h1": data["h1"]}


def diagram_path(city_key: str, scale: str, diagrams_dir: str, gtfs_version: str = "") -> str:
    """Return the canonical output path for a city's persistence diagrams."""
    if gtfs_version:
        return os.path.join(diagrams_dir, scale, gtfs_version, f"{city_key}_diagrams.npz")
    return os.path.join(diagrams_dir, scale, f"{city_key}_diagrams.npz")


def compute_and_save(
    city_key: str,
    scale: str,
    dist_matrix: np.ndarray,
    diagrams_dir: str,
    skip_existing: bool = True,
    gtfs_version: str = "",
) -> str | None:
    """Compute diagrams from a distance matrix and save to disk.

    Returns the output path, or None if skipped.
    """
    out_path = diagram_path(city_key, scale, diagrams_dir, gtfs_version)
    if skip_existing and os.path.exists(out_path):
        print(f"  [>] {city_key} diagrams already present. Skipping.")
        return out_path

    print(f"  [+] Running Ripser for {city_key} ...")
    diagrams = compute_persistence_diagrams(dist_matrix)
    n_h0 = len(diagrams["h0"])
    n_h1 = len(diagrams["h1"])
    print(f"      H0: {n_h0} bars  H1: {n_h1} loops")

    save_diagrams(diagrams, out_path)
    print(f"  [+] Saved → {out_path}")
    return out_path
