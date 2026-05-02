"""Transit graph construction using city2graph.

Separates graph building (pure computation) from serialization so callers
can inspect or transform the GeoDataFrames before writing to disk.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import city2graph as c2g
import networkx as nx
import geopandas as gpd
from geopandas import GeoDataFrame
import zipfile
import pandas as pd
from shapely.geometry import LineString


def load_gtfs(filtered_zip: str | Path) -> object:
    """Load a filtered GTFS zip into a city2graph DuckDB connection."""
    return c2g.load_gtfs(str(filtered_zip))

def _normalise_edge_time_columns(edges_gdf: GeoDataFrame) -> GeoDataFrame:
    """Ensure all edges expose the same travel-time cost columns.

    City2Graph transit edges usually expose travel_time_sec.
    Injected transfer/walking edges historically exposed weight/travel_time.
    Downstream code uses weight, so every edge must have a finite weight
    whenever any equivalent travel-time column is available.

    This function is deliberately conservative: existing valid values are
    preserved, and only missing/NaN values are filled from equivalent columns.
    """
    edges_gdf = edges_gdf.copy()

    fallback = pd.Series(pd.NA, index=edges_gdf.index, dtype="Float64")

    for col in ("weight", "travel_time_sec", "travel_time", "min_transfer_time"):
        if col in edges_gdf.columns:
            fallback = fallback.fillna(pd.to_numeric(edges_gdf[col], errors="coerce"))

    for col in ("travel_time_sec", "weight", "travel_time"):
        if col in edges_gdf.columns:
            existing = pd.to_numeric(edges_gdf[col], errors="coerce")
            edges_gdf[col] = existing.fillna(fallback)
        else:
            edges_gdf[col] = fallback

    return edges_gdf

def build_summary_graph(
    filtered_zip: str | Path,
    start_time: str = "07:00:00",
    end_time: str = "10:00:00",
    calendar_start: str | None = None,
    calendar_end: str | None = None,
    directed: bool = True,
    use_frequencies: bool = True,
    inject_manual_transfers: bool = False,
) -> tuple[GeoDataFrame, GeoDataFrame]:
    """Build a transit summary graph from a filtered GTFS zip.

    Parameters
    ----------
    filtered_zip:
        Path to the filtered GTFS zip produced by the data/filtering stage.
    start_time, end_time:
        Morning-peak (or any) time window in HH:MM:SS format.
    calendar_start, calendar_end:
        Optional date range filter (YYYYMMDD strings).
    directed:
        Whether to build a directed graph.
    use_frequencies:
        Use GTFS frequencies.txt when available.

    Returns
    -------
    (nodes_gdf, edges_gdf):
        GeoDataFrames ready for serialization or downstream analysis.

    Raises
    ------
    ValueError
        If the resulting graph is too small to be useful (< 2 nodes or 0 edges).
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning, module="city2graph")

        con = load_gtfs(filtered_zip)
        nodes_gdf, edges_gdf = c2g.travel_summary_graph(
            con,
            start_time=start_time,
            end_time=end_time,
            calendar_start=calendar_start,
            calendar_end=calendar_end,
            as_nx=False,
            directed=directed,
            use_frequencies=use_frequencies,
        )

        edges_gdf = _normalise_edge_time_columns(edges_gdf)
    # --- INJECT WALKING TRANSFERS ---
    if inject_manual_transfers:
        try:
            with zipfile.ZipFile(filtered_zip, "r") as z:
                if "transfers.txt" in z.namelist():
                    print("  [>] Injecting walking transfers into graph.")
                    transfers = pd.read_csv(z.open("transfers.txt"))

                    if not transfers.empty:
                        # Create a fast lookup dictionary for node geometries.
                        # city2graph usually keeps the node ID in the index.
                        node_geoms = nodes_gdf.geometry.to_dict()

                        geometries = []
                        valid_sources = []
                        valid_targets = []
                        valid_weights = []

                        for _, row in transfers.iterrows():
                            src = str(row["from_stop_id"])
                            tgt = str(row["to_stop_id"])
                            wgt = float(row["min_transfer_time"])

                            if src in node_geoms and tgt in node_geoms:
                                p1 = node_geoms[src]
                                p2 = node_geoms[tgt]

                                if p1 and p2:
                                    geometries.append(LineString([p1, p2]))
                                    valid_sources.append(src)
                                    valid_targets.append(tgt)
                                    valid_weights.append(wgt)

                        if geometries:
                            transfer_gdf = gpd.GeoDataFrame(
                                {
                                    "source": valid_sources,
                                    "target": valid_targets,
                                    "travel_time_sec": valid_weights,
                                    "weight": valid_weights,
                                    "travel_time": valid_weights,
                                    "frequency": 1,
                                    "route_type": 99,  # 99 = walking indicator
                                },
                                geometry=geometries,
                                crs=edges_gdf.crs,
                            )

                            edges_gdf = pd.concat(
                                [edges_gdf, transfer_gdf],
                                ignore_index=True,
                            )

                            edges_gdf = _normalise_edge_time_columns(edges_gdf)

                            print(f"  [+] Added {len(geometries)} walking edges with real LineStrings.")
        except Exception as e:
            print(f"  [!] Failed to inject transfers: {e}")
    else:

        print("  [>] Swiss/native transfer mode: skipping manual transfer injection.")
    # --------------------------------

    if len(nodes_gdf) < 2 or len(edges_gdf) == 0:
        raise ValueError(
            f"Graph too small: {len(nodes_gdf)} nodes, {len(edges_gdf)} edges."
        )

    return nodes_gdf, edges_gdf


def gdfs_to_nx(
    nodes_gdf: GeoDataFrame,
    edges_gdf: GeoDataFrame,
    directed: bool = True,
    keep_geom: bool = False,
) -> nx.Graph:
    """Convert node/edge GeoDataFrames to a NetworkX graph."""
    return c2g.gdf_to_nx(
        nodes=nodes_gdf,
        edges=edges_gdf,
        keep_geom=keep_geom,
        directed=directed,
    )
