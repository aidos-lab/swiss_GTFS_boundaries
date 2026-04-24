"""Graph serialization: GeoPackage, GraphML, and metadata sidecar.

Keeps all I/O concerns separate from the graph-building logic so that
build.py functions can be called without any side effects.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import networkx as nx
from geopandas import GeoDataFrame

from swiss_gtfs.config import GraphConfig

_SCALAR_TYPES = (str, int, float, bool)


def _format_flags(save_format: str) -> tuple[bool, bool]:
    """Return (need_gpkg, need_graphml) for a given save_format string."""
    return save_format in ("gpkg", "both"), save_format in ("graphml", "both")


# ---------------------------------------------------------------------------
# GraphML sanitization
# ---------------------------------------------------------------------------

def _sanitize_dict(d: dict) -> None:
    """Convert non-scalar attribute values to GraphML-compatible types in-place."""
    if "pos" in d and isinstance(d["pos"], (tuple, list)) and len(d["pos"]) == 2:
        d["x"] = float(d["pos"][0])
        d["y"] = float(d["pos"][1])
        del d["pos"]
    for k in list(d):
        v = d[k]
        if v is None:
            d[k] = ""
        elif not isinstance(v, _SCALAR_TYPES):
            d[k] = str(v)


def sanitize_for_graphml(G: nx.Graph) -> None:
    """Mutate G in-place so all node/edge/graph attributes are GraphML-safe."""
    for _, node_data in G.nodes(data=True):
        _sanitize_dict(node_data)
    for _, _, edge_data in G.edges(data=True):
        _sanitize_dict(edge_data)
    _sanitize_dict(G.graph)


# ---------------------------------------------------------------------------
# Artifact writing
# ---------------------------------------------------------------------------

def artifact_paths(city_key: str, scale: str, output_dir: str, gtfs_version: str) -> dict[str, str]:
    """Return the canonical output paths for a city's graph artifacts."""
    out_dir = os.path.join(output_dir, scale, gtfs_version)
    return {
        "dir":     out_dir,
        "gpkg":    os.path.join(out_dir, f"{city_key}.gpkg"),
        "graphml": os.path.join(out_dir, f"{city_key}.graphml"),
        "meta":    os.path.join(out_dir, f"{city_key}.json"),
    }


def outputs_exist(city_key: str, scale: str, output_dir: str, save_format: str, gtfs_version: str) -> bool:
    """Return True if all requested output files already exist."""
    paths = artifact_paths(city_key, scale, output_dir, gtfs_version)
    need_gpkg, need_graphml = _format_flags(save_format)
    gpkg_ok    = (not need_gpkg)    or os.path.exists(paths["gpkg"])
    graphml_ok = (not need_graphml) or os.path.exists(paths["graphml"])
    return gpkg_ok and graphml_ok


def save_graph_artifacts(
    city_key: str,
    scale: str,
    nodes_gdf: GeoDataFrame,
    edges_gdf: GeoDataFrame,
    cfg: GraphConfig,
    gtfs_version: str,
    extra_meta: dict[str, Any] | None = None,
) -> dict[str, str]:
    """Write graph outputs (GeoPackage, GraphML) and a metadata JSON sidecar.

    Parameters
    ----------
    city_key:
        Snake-case city identifier used for output filenames.
    scale:
        Geographic scale (e.g. 'agglomeration').
    nodes_gdf, edges_gdf:
        GeoDataFrames produced by build.build_summary_graph().
    cfg:
        GraphConfig controlling output format, directionality, etc.
    gtfs_version:
        YYYYMMDD version string written into the metadata sidecar.
    extra_meta:
        Optional dict merged into the metadata sidecar.

    Returns
    -------
    dict with keys 'gpkg', 'graphml', 'meta' pointing to written files.
    """
    paths = artifact_paths(city_key, scale, cfg.output_dir, gtfs_version)
    os.makedirs(paths["dir"], exist_ok=True)

    need_gpkg, need_graphml = _format_flags(cfg.save_format)

    written: dict[str, str] = {}

    if need_gpkg and not (cfg.skip_existing and os.path.exists(paths["gpkg"])):
        nodes_gdf.reset_index().to_file(paths["gpkg"], layer="nodes", driver="GPKG")
        edges_gdf.reset_index().to_file(paths["gpkg"], layer="edges", driver="GPKG")
        print(f"  [+] GeoPackage: {paths['gpkg']}")
        written["gpkg"] = paths["gpkg"]

    if need_graphml and not (cfg.skip_existing and os.path.exists(paths["graphml"])):
        from swiss_gtfs.graphs.build import gdfs_to_nx
        G = gdfs_to_nx(nodes_gdf, edges_gdf, directed=cfg.directed, keep_geom=False)
        sanitize_for_graphml(G)
        nx.write_graphml(G, paths["graphml"])
        print(f"  [+] GraphML:    {paths['graphml']}")
        written["graphml"] = paths["graphml"]

    meta: dict[str, Any] = {
        "city_key":       city_key,
        "scale":          scale,
        "gtfs_version":   gtfs_version,
        "start_time":     cfg.start_time,
        "end_time":       cfg.end_time,
        "calendar_start": cfg.calendar_start,
        "calendar_end":   cfg.calendar_end,
        "directed":       cfg.directed,
        "n_nodes":        len(nodes_gdf),
        "n_edges":        len(edges_gdf),
        "built_at":       datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    if extra_meta:
        meta.update(extra_meta)

    with open(paths["meta"], "w") as f:
        json.dump(meta, f, indent=2)
    written["meta"] = paths["meta"]

    return written
