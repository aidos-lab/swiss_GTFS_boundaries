"""GTFS spatial filtering.

Clips a full Swiss GTFS feed to a named geographic boundary (canton,
agglomeration, district, or commune) and writes a filtered GTFS zip.

Uses a TemporaryDirectory for all intermediate files so the function is
safe to call from any working directory without mutating state.
"""

from __future__ import annotations
import unicodedata
import io
import os
import zipfile
from pathlib import Path
import geopandas as gpd
import pandas as pd
from swiss_gtfs.mappings.regions import SHAPEFILE_CONFIG, get_mapping
import numpy as np
from scipy.spatial import cKDTree

def generate_synthetic_transfers(
    stops_df: pd.DataFrame,
    max_dist_m: float = 200.0,
    speed_m_s: float = 1.2
) -> pd.DataFrame:
    """Generate walking transfers using L1 distance via KDTree."""
    print(f"  [>] Generating L1 walking grid for {len(stops_df)} stops...")
    if stops_df.empty:
        return pd.DataFrame()

    # 1. Safely extract coordinates
    lons = pd.to_numeric(stops_df["stop_lon"]).values
    lats = pd.to_numeric(stops_df["stop_lat"]).values
    stop_ids = stops_df["stop_id"].values

    # 2. Use GeoPandas' automatic UTM estimator for perfect meters anywhere on Earth
    gdf = gpd.GeoDataFrame(
        geometry=gpd.points_from_xy(lons, lats),
        crs="EPSG:4326"
    ).to_crs(gpd.GeoDataFrame(geometry=gpd.points_from_xy(lons, lats), crs="EPSG:4326").estimate_utm_crs())

    coords = np.column_stack((gdf.geometry.x, gdf.geometry.y))

    # 3. Build KDTree and query (L1 distance, p=1)
    tree = cKDTree(coords)
    pairs = tree.query_pairs(r=max_dist_m, p=1, output_type='ndarray')

    if len(pairs) == 0:
        print("  [!] No pairs found within walking distance.")
        return pd.DataFrame()

    i = pairs[:, 0]
    j = pairs[:, 1]

    # 4. Calculate actual L1 distance and time
    dist = np.abs(coords[i, 0] - coords[j, 0]) + np.abs(coords[i, 1] - coords[j, 1])
    walking_times = np.round(dist / speed_m_s).astype(int)

    # Add a base penalty of 60 seconds for crossing streets/waiting
    walking_times += 60

    # 5. GTFS transfers are directed, so duplicate them both ways (i->j and j->i)
    from_ids = np.concatenate([stop_ids[i], stop_ids[j]])
    to_ids = np.concatenate([stop_ids[j], stop_ids[i]])
    times = np.concatenate([walking_times, walking_times])

    transfers_df = pd.DataFrame({
        "from_stop_id": from_ids,
        "to_stop_id": to_ids,
        "transfer_type": 2,
        "min_transfer_time": times
    })

    print(f"  [+] Injected {len(transfers_df)} synthetic transfer edges!")
    return transfers_df


def _norm_boundary_text(value) -> str:
    """Normalize boundary labels for robust shapefile matching."""
    if pd.isna(value):
        return ""
    return unicodedata.normalize("NFC", str(value)).replace("\xa0", " ").strip()


def _resolve_boundary_column(
    boundary: gpd.GeoDataFrame,
    preferred_col: str,
    city_raw: str,
    scale: str,
) -> str:
    """Resolve the boundary-name column robustly across shapefile versions.

    Keeps the configured column when present. For agglomerations, supports
    both older AgglName-style schemas and current ANAME-style schemas.
    """
    if preferred_col in boundary.columns:
        return preferred_col

    # Scale-specific aliases first.
    aliases_by_scale = {
        "agglomeration": ["AgglName", "ANAME", "AggloName", "AGGLNAME"],
        "commune": ["GDENAME", "GDE_NAME", "NAME", "Name"],
        "district": ["BEZNAME", "BEZ_NAME", "NAME", "Name"],
        "canton": ["KTKZ", "KTNAME", "NAME", "Name"],
        "region": ["Region", "REGION", "NOM_REG"],
        "provincia": ["Provincia", "PROVINCIA", "NOM_PROV"],
        "comuna": ["Comuna", "COMUNA", "NOM_COM"],
    }

    for col in aliases_by_scale.get(scale, []):
        if col in boundary.columns:
            return col

    # Last-resort: find a column whose values contain the requested name.
    target = _norm_boundary_text(city_raw)
    target_lower = target.lower()

    for col in boundary.columns:
        if col == boundary.geometry.name:
            continue

        values = boundary[col].dropna().map(_norm_boundary_text)

        if (values == target).any():
            return col

        if values.astype(str).str.lower().str.contains(target_lower, regex=False).any():
            return col

    raise KeyError(
        f"Could not resolve boundary name column. "
        f"Configured column '{preferred_col}' not found. "
        f"Scale={scale!r}, city_raw={city_raw!r}. "
        f"Available columns: {list(boundary.columns)}"
    )


def _select_city_boundary(
    boundary: gpd.GeoDataFrame,
    preferred_col: str,
    city_raw: str,
    scale: str,
    shp_path: str,
) -> gpd.GeoDataFrame:
    """Select boundary rows robustly across shapefile versions."""
    name_col = _resolve_boundary_column(
        boundary=boundary,
        preferred_col=preferred_col,
        city_raw=city_raw,
        scale=scale,
    )

    target = _norm_boundary_text(city_raw)
    values = boundary[name_col].map(_norm_boundary_text)

    city_boundary = boundary[values == target]

    # Fallback for labels with suffixes/variants.
    if city_boundary.empty:
        target_lower = target.lower()
        city_boundary = boundary[
            values.astype(str).str.lower().str.contains(target_lower, regex=False)
        ]

    if city_boundary.empty:
        available = (
            boundary[name_col]
            .dropna()
            .map(_norm_boundary_text)
            .sort_values()
            .unique()
            .tolist()
        )

        raise ValueError(
            f"Could not find {city_raw!r} in {shp_path}. "
            f"Configured column={preferred_col!r}, resolved column={name_col!r}. "
            f"First available values: {available[:50]}"
        )

    if name_col != preferred_col:
        print(
            f"  [>] Boundary column '{preferred_col}' not found; "
            f"using '{name_col}' for scale='{scale}'."
        )

    return city_boundary


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

    city_boundary = _select_city_boundary(
        boundary=boundary,
        preferred_col=name_col,
        city_raw=city_raw,
        scale=scale,
        shp_path=shp_path,
    )

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

    # --- ADD THIS BLOCK FOR CHILE ---

    freq_path = os.path.join(in_dir, "frequencies.txt")
    if os.path.exists(freq_path):
        frequencies = pd.read_csv(freq_path)
        filtered_frequencies = frequencies[frequencies["trip_id"].isin(filtered_trip_ids)]
    else:
        filtered_frequencies = None

    shapes_path = os.path.join(in_dir, "shapes.txt")
    if os.path.exists(shapes_path):
        shapes = pd.read_csv(shapes_path)
        filtered_shape_ids = set(filtered_trips["shape_id"].dropna())
        filtered_shapes = shapes[shapes["shape_id"].isin(filtered_shape_ids)]
    else:
        filtered_shapes = None

    # --- ADD THIS BLOCK FOR SYNTHETIC TRANSFERS ---
    transfers_path = os.path.join(in_dir, "transfers.txt")
    if os.path.exists(transfers_path):
        transfers = pd.read_csv(transfers_path)
        filtered_transfers = transfers[
            transfers["from_stop_id"].isin(filtered_stops["stop_id"]) &
            transfers["to_stop_id"].isin(filtered_stops["stop_id"])
        ]
    else:
        print("  [>] transfers.txt missing. Generating L1 synthetic walking grid (200m)...")
        filtered_transfers = generate_synthetic_transfers(filtered_stops, max_dist_m=200.0)
    # --------------------------------

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

# IMPORTANT: Add the optional files if they are not None/empty
    if filtered_frequencies is not None and not filtered_frequencies.empty:
        tables["frequencies.txt"] = filtered_frequencies
    if filtered_shapes is not None and not filtered_shapes.empty:
        tables["shapes.txt"] = filtered_shapes
    if filtered_transfers is not None and not filtered_transfers.empty:
        tables["transfers.txt"] = filtered_transfers # This line ensures it gets zipped!

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
