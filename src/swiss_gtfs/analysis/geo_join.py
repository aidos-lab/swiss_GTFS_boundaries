"""Geographic join and map rendering for cluster results.

Merges cluster labels onto boundary GeoDataFrames and produces
interactive folium choropleth maps.
"""

from __future__ import annotations

import os

import geopandas as gpd
import pandas as pd

from swiss_gtfs.mappings.regions import SHAPEFILE_CONFIG, get_mapping


def load_boundary_gdf(scale: str, geodata_dir: str) -> gpd.GeoDataFrame:
    """Load the boundary shapefile for a given scale.

    Parameters
    ----------
    scale:
        One of 'canton', 'agglomeration', 'district', 'commune'.
    geodata_dir:
        Root directory containing the boundary shapefiles.

    Returns
    -------
    GeoDataFrame of all regions in the shapefile.
    """
    subdir, shp_name, _ = SHAPEFILE_CONFIG[scale]
    shp_path = os.path.join(geodata_dir, subdir, shp_name)
    return gpd.read_file(shp_path)


def merge_clusters(
    results_df: pd.DataFrame,
    scale: str,
    geodata_dir: str,
    city_key_col: str = "city_key",
    label_col: str = "cluster",
    unprocessed_label: str = "Not Processed",
) -> gpd.GeoDataFrame:
    """Left-join cluster labels onto a boundary GeoDataFrame.

    The join key is the raw shapefile name (looked up from the region
    mapping), not the snake_case city_key, to ensure correct matching.

    Parameters
    ----------
    results_df:
        DataFrame with at least columns [city_key_col, label_col].
    scale:
        Geographic scale.
    geodata_dir:
        Root directory for boundary shapefiles.
    city_key_col:
        Column in results_df containing snake_case city keys.
    label_col:
        Column in results_df containing cluster labels.
    unprocessed_label:
        String to assign to unmatched regions.

    Returns
    -------
    Merged GeoDataFrame with a column matching label_col.
    """
    _, _, id_col = SHAPEFILE_CONFIG[scale]
    mapping = get_mapping(scale)

    enriched = results_df.copy()
    enriched["_shapefile_name"] = enriched[city_key_col].map(mapping)

    gdf = load_boundary_gdf(scale, geodata_dir)
    merged = gdf.merge(
        enriched[[city_key_col, "_shapefile_name", label_col]],
        left_on=id_col,
        right_on="_shapefile_name",
        how="left",
    ).drop(columns=["_shapefile_name"], errors="ignore")

    merged[label_col] = (
        merged[label_col]
        .astype(str)
        .replace("nan", unprocessed_label)
        .fillna(unprocessed_label)
    )

    return merged


def plot_cluster_map(
    merged_gdf: gpd.GeoDataFrame,
    scale: str,
    label_col: str = "cluster",
    cmap: str = "Set1",
    tiles: str = "CartoDB positron",
    fill_opacity: float = 0.7,
):
    """Render an interactive folium choropleth map of cluster assignments.

    Parameters
    ----------
    merged_gdf:
        GeoDataFrame produced by merge_clusters().
    scale:
        Geographic scale (used for the tooltip id column).
    label_col:
        Column containing cluster labels.
    cmap:
        Matplotlib colormap name for categorical colouring.
    tiles:
        Folium tile provider.
    fill_opacity:
        Polygon fill opacity.

    Returns
    -------
    folium.Map
    """
    _, _, id_col = SHAPEFILE_CONFIG[scale]

    return merged_gdf.explore(
        column=label_col,
        cmap=cmap,
        tooltip=[id_col, label_col],
        popup=True,
        tiles=tiles,
        style_kwds={"fillOpacity": fill_opacity, "weight": 1.0},
        name="Transit Topologies",
    )
