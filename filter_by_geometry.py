import pandas as pd
import os
import zipfile
import sys
import geopandas as gpd

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
        # If it exists, print a message and exit
        print(f"The directory gtfs_temp already exists. Please delete it and try again.")
        sys.exit()

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
    in_path = 'Data/gtfs_fp2026_20260325'
    out_path = 'Data/fribourg_coords.zip'
    city = 'fribourg'
    scale = 'agglomeration'
    filter_fun(city, scale, in_path, out_path)