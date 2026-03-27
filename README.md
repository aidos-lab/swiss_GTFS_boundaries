# swiss_GTFS_boundaries

Repository designed to spatially filter Swiss public transit GTFS feeds using different regional aggregation scales. 

By intersecting raw GTFS data (stops, routes, trips, etc.) with official Swiss boundary shapefiles, these scripts extract localized transit networks for specific areas. The output is a clean, ready-to-use GTFS `.zip` file restricted to the chosen boundary, which can then be parsed into directed graphs for topological data analysis or routing.

The available geographic aggregation scales are: **Cantons**, **Communes**, **Districts**, and **Agglomerations**.

## Data Sources

You will need to download the raw GTFS and Shapefile data to run these scripts:
* **Swiss GTFS Data:** https://opentransportdata.swiss/en/cookbook/timetable-cookbook/gtfs/
* **Agglomerations Boundaries:** https://data.geo.admin.ch/browser/index.html#/collections/ch.bfs.generalisierte-grenzen_agglomerationen_g1
* **Cantons, Districts, and Communes Boundaries:** https://data.geo.admin.ch/browser/index.html#/collections/ch.bfs.historisierte-administrative_grenzen_g0/items/historisierte-administrative_grenzen_g0_1860-01-01?.asset=asset-historisierte-administrative_grenzen_g0_1860-01-01_2056-shp-zip

## Repository Structure

* `filter_by_geometry.py`: A targeted script used to filter the GTFS dataset for a single, hardcoded city/region and scale at a time.
* `filter_by_geometry_scale.py`: A Command Line Interface (CLI) batch-processing script. It iterates through all regions within a defined scale (e.g., all cantons) and automatically generates a filtered GTFS `.zip` file for each one.
* `name_mappings.py`: Contains the dictionaries that map human-readable location names (e.g., "Zürich", "ZH") to their exact internal ID codes used in the Swiss shapefiles.
* `experiments/GTFS_Graphs.ipynb`: A Jupyter Notebook used to load the filtered `.zip` files via `gtfs2nx`, convert them into `networkx` directed graphs, and visualize the multi-modal transit networks (colored by transport mode and travel time) on interactive `folium` maps.
* `requirements.txt`: The Python dependencies required to run the spatial intersections and graph visualizations.
