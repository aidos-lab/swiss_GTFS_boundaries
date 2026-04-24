"""Transit graph construction using city2graph.

Separates graph building (pure computation) from serialization so callers
can inspect or transform the GeoDataFrames before writing to disk.
"""

from __future__ import annotations

import warnings
from pathlib import Path

import city2graph as c2g
import networkx as nx
from geopandas import GeoDataFrame


def load_gtfs(filtered_zip: str | Path) -> object:
    """Load a filtered GTFS zip into a city2graph DuckDB connection."""
    return c2g.load_gtfs(str(filtered_zip))


def build_summary_graph(
    filtered_zip: str | Path,
    start_time: str = "07:00:00",
    end_time: str = "10:00:00",
    calendar_start: str | None = None,
    calendar_end: str | None = None,
    directed: bool = True,
    use_frequencies: bool = True,
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
