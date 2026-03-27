import pandas as pd
import os
import zipfile
import geopandas as gpd
import argparse


#####
def filter_fun(city: str, scale: str, in_path: str, out_path: str, geodata_dir_path: str = 'geodata/'):
    if scale == 'canton':
        from name_mappings import canton_names as city_names
        boundary = gpd.read_file(geodata_dir_path + "boundaries/" + "Cantons_G0_18600101.shp")
        name_col = 'KTKZ'

    elif scale == 'agglomeration':
        from name_mappings import agglomerations_names as city_names
        boundary = gpd.read_file(geodata_dir_path + "agglomerations/" + "k4a24_12.shp")
        name_col = 'AgglName'

    elif scale == 'district':
        from name_mappings import district_names as city_names
        boundary = gpd.read_file(geodata_dir_path + "boundaries/" + "Districts_G0_18600101.shp")
        name_col = 'BEZNAME'

    elif scale == 'commune':
        from name_mappings import comune_names as city_names
        boundary = gpd.read_file(geodata_dir_path + "boundaries/" + "Communes_G0_18600101.shp")
        name_col = 'GDENAME'

    else:
        print('scale not supported')
        raise ValueError

    #####
    if not os.path.exists('gtfs_temp'):
        os.makedirs('gtfs_temp')
        print(f"Temp directory created at: gtfs_temp")
    else:
        # Instead of sys.exit(), we just empty the temp dir so the loop can keep going
        for file in os.listdir("gtfs_temp"):
            os.remove(f"gtfs_temp/{file}")

    city = city_names[city]
    #####
    stops = pd.read_csv(in_path + "/stops.txt")
    stops_gdf = gpd.GeoDataFrame(
        stops,
        geometry=gpd.points_from_xy(stops['stop_lon'], stops['stop_lat']),
        crs="EPSG:4326"  # Specify that the raw numbers are standard lat/lon
    )

    # 2. Reproject the points to match the CRS of your 'boundary' GeoDataFrame
    stops_gdf = stops_gdf.to_crs(boundary.crs)
    city_boundary = boundary[boundary[name_col] == city]

    if city_boundary.empty:
        raise ValueError(f"Could not find '{city}' in the boundary dataset.")

    city_polygon = city_boundary.geometry.union_all()

    filtered_stops = stops_gdf[stops_gdf.geometry.within(city_polygon)]

    filtered_stops = pd.DataFrame(filtered_stops).drop(columns=['geometry'])

    filtered_stops_ids = set(filtered_stops['stop_id'])
    #####
    stop_times = pd.read_csv(in_path + '/stop_times.txt')
    filtered_stop_times = stop_times[stop_times['stop_id'].isin(filtered_stops_ids)]
    filtered_trip_ids = set(filtered_stop_times['trip_id'])
    #####
    trips = pd.read_csv(in_path + '/trips.txt')
    filtered_trips = trips[trips['trip_id'].isin(filtered_trip_ids)]
    filtered_route_ids = set(filtered_trips['route_id'])
    filtered_service_ids = set(filtered_trips['service_id'])
    #####
    routes = pd.read_csv(in_path + '/routes.txt')
    filtered_routes = routes[routes['route_id'].isin(filtered_route_ids)]
    filtered_agency_ids = set(filtered_routes['agency_id'])
    #####
    agency = pd.read_csv(in_path + '/agency.txt')
    filtered_agency = agency[agency['agency_id'].isin(filtered_agency_ids)]
    #####
    calendar = pd.read_csv(in_path + '/calendar.txt')
    filtered_calendar = calendar[calendar['service_id'].isin(filtered_service_ids)]
    #####
    calendar_dates = pd.read_csv(in_path + '/calendar_dates.txt')
    filtered_calendar_dates = calendar_dates[calendar_dates['service_id'].isin(filtered_service_ids)]
    #####
    with zipfile.ZipFile(out_path, 'w') as zip_out:
        filtered_agency.to_csv("gtfs_temp/agency.txt", index=False)
        zip_out.write("gtfs_temp/agency.txt", "agency.txt")

        filtered_routes.to_csv("gtfs_temp/routes.txt", index=False)
        zip_out.write("gtfs_temp/routes.txt", "routes.txt")

        filtered_trips.to_csv("gtfs_temp/trips.txt", index=False)
        zip_out.write("gtfs_temp/trips.txt", "trips.txt")

        filtered_stop_times.to_csv("gtfs_temp/stop_times.txt", index=False)
        zip_out.write("gtfs_temp/stop_times.txt", "stop_times.txt")

        filtered_stops.to_csv("gtfs_temp/stops.txt", index=False)
        zip_out.write("gtfs_temp/stops.txt", "stops.txt")

        filtered_calendar.to_csv("gtfs_temp/calendar.txt", index=False)
        zip_out.write("gtfs_temp/calendar.txt", "calendar.txt")

        filtered_calendar_dates.to_csv("gtfs_temp/calendar_dates.txt", index=False)
        zip_out.write("gtfs_temp/calendar_dates.txt", "calendar_dates.txt")
    #####
    for file in os.listdir("gtfs_temp"):
        os.remove(f"gtfs_temp/{file}")
    os.rmdir("gtfs_temp")
    #####


if __name__ == '__main__':
    from name_mappings import canton_names, agglomerations_names, district_names, comune_names

    parser = argparse.ArgumentParser(description="Filter GTFS by geography.")
    parser.add_argument('--scale', type=str, required=True, choices=['canton', 'agglomeration', 'district', 'commune'])
    parser.add_argument('--in_path', type=str, default='Data/gtfs_fp2026_20260325')
    args = parser.parse_args()

    # Match the scale to the correct dictionary
    mapping_dicts = {
        'canton': canton_names,
        'agglomeration': agglomerations_names,
        'district': district_names,
        'commune': comune_names
    }

    mapping = mapping_dicts[args.scale]

    # Create the output folder (e.g., Data/agglomeration/)
    scale_dir = f"Data/{args.scale}"
    os.makedirs(scale_dir, exist_ok=True)

    processed_raw_names = set()

    # Loop through the dictionary
    for clean_key, raw_name in mapping.items():
        # Skip duplicates (e.g., Zurich and ZH mapped to same agglomeration)
        if raw_name in processed_raw_names:
            continue

        processed_raw_names.add(raw_name)
        out_path = f"{scale_dir}/{clean_key}.zip"

        print(f"Processing [{args.scale}]: {raw_name} -> {out_path}")

        try:
            filter_fun(clean_key, args.scale, args.in_path, out_path)
        except Exception as e:
            print(f"  -> ERROR processing {raw_name}: {e}")