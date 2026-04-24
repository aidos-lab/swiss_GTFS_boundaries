"""Clustering wrappers for transit feature matrices.

Provides a unified interface for scikit-learn clustering methods with
standard scaling built in. Accepts any sklearn-compatible estimator.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler


def cluster_regions(
    feature_matrix: np.ndarray,
    method: str = "kmeans",
    n_clusters: int = 5,
    random_state: int = 42,
    **kwargs,
) -> np.ndarray:
    """Scale features and apply a clustering algorithm.

    Parameters
    ----------
    feature_matrix:
        Array of shape (n_cities, n_features).
    method:
        One of 'kmeans', 'agglomerative', 'dbscan'.
    n_clusters:
        Number of clusters (ignored for DBSCAN).
    random_state:
        Random seed for reproducible results.
    **kwargs:
        Additional keyword arguments forwarded to the sklearn estimator.

    Returns
    -------
    Integer array of cluster labels, shape (n_cities,).
    """
    from sklearn.cluster import KMeans, AgglomerativeClustering, DBSCAN

    _methods = {
        "kmeans":        KMeans,
        "agglomerative": AgglomerativeClustering,
        "dbscan":        DBSCAN,
    }
    if method not in _methods:
        raise ValueError(f"Unknown method '{method}'. Choose from {list(_methods)}.")

    EstimatorClass = _methods[method]

    params: dict = dict(kwargs)
    if method in ("kmeans", "agglomerative"):
        params.setdefault("n_clusters", n_clusters)
    if method == "kmeans":
        params.setdefault("random_state", random_state)

    print(f"  Running {EstimatorClass.__name__} with params: {params}")

    scaler = StandardScaler()
    scaled = scaler.fit_transform(feature_matrix)

    model = EstimatorClass(**params)
    labels = model.fit_predict(scaled)
    return labels


def cluster_to_dataframe(
    city_keys: list[str],
    labels: np.ndarray,
    label_col: str = "cluster",
) -> pd.DataFrame:
    """Combine city keys and cluster labels into a tidy DataFrame."""
    return pd.DataFrame({"city_key": city_keys, label_col: labels})
