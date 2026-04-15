"""Transit graph folium visualization.

Extracted from experiments/GTFS_Graphs.ipynb. Produces interactive folium
maps coloured by transport mode and travel time.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

import branca.colormap as cm
import folium
import networkx as nx
import osmnx as ox
import pandas as pd


# ---------------------------------------------------------------------------
# Route-type colour helpers
# ---------------------------------------------------------------------------

_COLOR_MODE_LABEL: dict[str, str] = {
    "red":    "Train",
    "purple": "Metro",
    "orange": "Tram",
    "blue":   "Bus",
    "gray":   "Other",
}

_MODE_WEIGHT: dict[str, float] = {
    "red": 2.5, "purple": 2.5,
}


def _route_type_color(rtype: int | float) -> str:
    """Map a GTFS route_type integer to a colour string."""
    try:
        rtype = int(rtype)
    except (TypeError, ValueError):
        return "gray"
    if 100 <= rtype < 200:
        return "red"
    if 400 <= rtype < 500:
        return "purple"
    if 900 <= rtype < 1000:
        return "orange"
    if 700 <= rtype < 800:
        return "blue"
    return "gray"


# ---------------------------------------------------------------------------
# Main visualisation function
# ---------------------------------------------------------------------------

def visualize_graph(
    G: nx.DiGraph,
    gtfs_zip_path: str | Path,
    zoom_start: int = 13,
    tile: str = "CartoDB DarkMatter",
) -> folium.Map | None:
    """Render a gtfs2nx/city2graph transit graph as an interactive folium map.

    Edges are coloured by travel time; nodes are coloured by transport mode.

    Parameters
    ----------
    G:
        NetworkX directed graph with node attributes 'x'/'y' (lon/lat) and
        edge attribute 'weight' (travel time in seconds).
    gtfs_zip_path:
        Path to the original filtered GTFS zip — used to look up stop names
        and route types for tooltips.
    zoom_start:
        Initial map zoom level.
    tile:
        Folium tile provider string.

    Returns
    -------
    folium.Map or None if the graph has no valid coordinates.
    """
    G_multi = nx.MultiDiGraph(G)
    if "crs" not in G_multi.graph:
        G_multi.graph["crs"] = "EPSG:32632"
    G_proj = ox.project_graph(G_multi, to_crs="EPSG:4326")

    with zipfile.ZipFile(gtfs_zip_path, "r") as z:
        routes = pd.read_csv(z.open("routes.txt"), dtype=str)
        stops  = pd.read_csv(z.open("stops.txt"),  dtype=str)

    routes["route_type"] = pd.to_numeric(routes["route_type"], errors="coerce")
    stop_names  = stops.set_index("stop_id")["stop_name"].to_dict()
    route_types = routes.set_index("route_id")["route_type"].to_dict()

    travel_times = [
        data.get("weight", 0)
        for _, _, data in G_proj.edges(data=True)
        if data.get("weight", 0) > 0
    ]
    if travel_times:
        min_t, max_t = min(travel_times), max(travel_times)
    else:
        min_t, max_t = 0, 1

    time_cmap = cm.LinearColormap(
        colors=["#00b050", "#ffff00", "#ff0000"],
        vmin=min_t,
        vmax=max_t,
        caption="Travel time between stops (seconds)",
    )

    lats = [d.get("y") for _, d in G_proj.nodes(data=True) if d.get("y") is not None]
    lons = [d.get("x") for _, d in G_proj.nodes(data=True) if d.get("x") is not None]
    if not lats or not lons:
        print("Graph contains no valid coordinates.")
        return None

    center = (sum(lats) / len(lats), sum(lons) / len(lons))
    m = folium.Map(location=center, zoom_start=zoom_start, tiles=tile)
    if travel_times:
        m.add_child(time_cmap)

    for u, v, data in G_proj.edges(data=True):
        u_lat = G_proj.nodes[u].get("y")
        u_lon = G_proj.nodes[u].get("x")
        v_lat = G_proj.nodes[v].get("y")
        v_lon = G_proj.nodes[v].get("x")

        if u_lat is None or u_lon is None or v_lat is None or v_lon is None or u == v:
            continue

        u_route_id = str(u).split("@@")[1] if "@@" in str(u) else None
        u_type = route_types.get(u_route_id, 700)
        mode_color = _route_type_color(u_type)

        edge_time = data.get("weight", 0)
        line_color = time_cmap(edge_time) if edge_time > 0 else mode_color

        folium.PolyLine(
            locations=[(u_lat, u_lon), (v_lat, v_lon)],
            color=line_color,
            weight=_MODE_WEIGHT.get(mode_color, 1.0),
            opacity=0.8,
            tooltip=f"Travel Time: {int(edge_time)}s" if edge_time > 0 else "Transit Link",
        ).add_to(m)

    for node, data in G_proj.nodes(data=True):
        lat, lon = data.get("y"), data.get("x")
        if lat is None or lon is None:
            continue

        node_str = str(node)
        if "@@" in node_str:
            stop_id_part, route_id_part = node_str.split("@@", 1)
        else:
            stop_id_part, route_id_part = node_str, None

        name       = stop_names.get(stop_id_part, stop_id_part)
        rtype      = route_types.get(route_id_part, 700)
        node_color = _route_type_color(rtype)
        mode_label = _COLOR_MODE_LABEL.get(node_color, "Unknown")

        folium.CircleMarker(
            location=(lat, lon),
            radius=1.5 if node_color in ("red", "purple") else 1.0,
            color=node_color,
            fill=True,
            fill_color=node_color,
            fill_opacity=1.0,
            tooltip=f"{name} ({mode_label})",
        ).add_to(m)

    return m
