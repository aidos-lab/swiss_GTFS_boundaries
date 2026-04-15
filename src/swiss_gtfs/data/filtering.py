"""GTFS spatial filtering.

Clips a full Swiss GTFS feed to a named geographic boundary (canton,
agglomeration, district, or commune) and writes a filtered GTFS zip.

Uses a TemporaryDirectory for all intermediate files so the function is
safe to call from any working directory without mutating state.
"""

from __future__ import annotations

import io
import os
import zipfile
from pathlib import Path

import geopandas as gpd
import pandas as pd

from swiss_gtfs.mappings.regions import SHAPEFILE_CONFIG, get_mapping


def filter_gtfs_city(
    city_key: str,
    scale: str,
    in_dir: str,
    out_path: str,
    geodata_dir: str = "geodata/",
    _boundary: gpd.GeoDataFrame | None = None,
) -> None:
    """Filter a GTFS feed to a single city's boundary polygon.

    Parameters
    ----------
    city_key:
        The clean snake_case key from the region mapping (e.g. 'zurich').
    scale:
        One of 'canton', 'agglomeration', 'district', 'commune'.
    in_dir:
        Directory containing the extracted GTFS text files (stops.txt, etc.).
    out_path:
        Destination path for the filtered GTFS zip.
    geodata_dir:
        Root directory for boundary shapefiles.
    _boundary:
        Pre-loaded boundary GeoDataFrame (avoids re-reading the shapefile when
        called in a loop via filter_scale).

    Raises
    ------
    ValueError
        If the city key is not found in the boundary dataset.
    """
    mapping = get_mapping(scale)
    city_raw = mapping[city_key]

    subdir, shp_name, name_col = SHAPEFILE_CONFIG[scale]
    shp_path = os.path.join(geodata_dir, subdir, shp_name)
    boundary = gpd.read_file(shp_path) if _boundary is None else _boundary

    stops = pd.read_csv(os.path.join(in_dir, "stops.txt"), low_memory=False)
    stops_gdf = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops["stop_lon"], stops["stop_lat"]),
        crs="EPSG:4326",
    ).to_crs(boundary.crs)

    city_boundary = boundary[boundary[name_col] == city_raw]
    if city_boundary.empty:
        raise ValueError(f"Could not find '{city_raw}' in {shp_path} (column '{name_col}').")

    city_polygon = city_boundary.geometry.union_all()
    filtered_stops = stops_gdf[stops_gdf.geometry.within(city_polygon)]
    filtered_stops = filtered_stops.drop(columns=["geometry"])
    filtered_stop_ids = set(filtered_stops["stop_id"])

    stop_times = pd.read_csv(os.path.join(in_dir, "stop_times.txt"))
    filtered_stop_times = stop_times[stop_times["stop_id"].isin(filtered_stop_ids)]
    filtered_trip_ids = set(filtered_stop_times["trip_id"])

    trips = pd.read_csv(os.path.join(in_dir, "trips.txt"))
    filtered_trips = trips[trips["trip_id"].isin(filtered_trip_ids)]
    filtered_route_ids = set(filtered_trips["route_id"])
    filtered_service_ids = set(filtered_trips["service_id"])

    routes = pd.read_csv(os.path.join(in_dir, "routes.txt"))
    filtered_routes = routes[routes["route_id"].isin(filtered_route_ids)]
    filtered_agency_ids = set(filtered_routes["agency_id"])

    agency = pd.read_csv(os.path.join(in_dir, "agency.txt"))
    filtered_agency = agency[agency["agency_id"].isin(filtered_agency_ids)]

    calendar = pd.read_csv(os.path.join(in_dir, "calendar.txt"))
    filtered_calendar = calendar[calendar["service_id"].isin(filtered_service_ids)]

    calendar_dates = pd.read_csv(os.path.join(in_dir, "calendar_dates.txt"))
    filtered_calendar_dates = calendar_dates[
        calendar_dates["service_id"].isin(filtered_service_ids)
    ]

    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)

    tables = {
        "agency.txt": filtered_agency,
        "routes.txt": filtered_routes,
        "trips.txt": filtered_trips,
        "stop_times.txt": filtered_stop_times,
        "stops.txt": filtered_stops,
        "calendar.txt": filtered_calendar,
        "calendar_dates.txt": filtered_calendar_dates,
    }
    with zipfile.ZipFile(out_path, "w", compression=zipfile.ZIP_DEFLATED) as zip_out:
        for fname, df in tables.items():
            zip_out.writestr(fname, df.to_csv(index=False))


def filter_scale(
    scale: str,
    in_dir: str,
    out_dir: str,
    geodata_dir: str = "geodata/",
    cities: list[str] | None = None,
    gtfs_version: str = "",
) -> None:
    """Filter GTFS for all (or selected) cities at a given scale.

    Parameters
    ----------
    scale:
        Geographic scale to process.
    in_dir:
        Extracted GTFS source directory.
    out_dir:
        Base output directory; files are written to out_dir/{scale}/{version}/{city}.zip.
    geodata_dir:
        Root directory for boundary shapefiles.
    cities:
        Optional list of city keys to process; defaults to all.
    gtfs_version:
        GTFS version string for output directory segregation.
    """
    from swiss_gtfs.mappings.regions import resolve_cities

    subdir, shp_name, _ = SHAPEFILE_CONFIG[scale]
    boundary = gpd.read_file(os.path.join(geodata_dir, subdir, shp_name))

    scale_dir = os.path.join(out_dir, scale, gtfs_version) if gtfs_version else os.path.join(out_dir, scale)
    os.makedirs(scale_dir, exist_ok=True)

    city_pairs = resolve_cities(scale, cities)
    for city_key, raw_name in city_pairs:
        out_path = os.path.join(scale_dir, f"{city_key}.zip")
        print(f"  Filtering [{scale}]: {raw_name} → {out_path}")
        try:
            filter_gtfs_city(city_key, scale, in_dir, out_path, geodata_dir, _boundary=boundary)
        except Exception as e:
            print(f"  [!] ERROR filtering {raw_name}: {e}")
