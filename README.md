# swiss-gtfs

A modular Python pipeline for extracting, building, and analysing Swiss public transit networks from GTFS data.

The pipeline is structured around three explicit stages:

1. **Filter** — clip the national GTFS feed to a Swiss geographic boundary (canton, agglomeration, district, or commune)
2. **Build** — construct a weighted transit summary graph using [city2graph](https://github.com/c2g-lab/city2graph), compute shortest-path distance matrices, and extract persistence diagrams
3. **Analyse** — vectorize topological features and cluster regions by transit topology

---

## Requirements

- Python 3.13+
- [uv](https://docs.astral.sh/uv/) (recommended) or pip

---

## Installation

```bash
git clone https://github.com/your-org/swiss_GTFS_boundaries
cd swiss_GTFS_boundaries
uv pip install -e .
```

This registers four CLI commands: `gtfs-filter`, `gtfs-build`, `gtfs-features`, `gtfs-analyze`.

---

## Data Sources

The pipeline can auto-download boundaries from the Swiss federal STAC API. GTFS feeds are downloaded from opentransportdata.swiss.

To run offline or pin specific versions, download manually:

| Dataset | Source |
|---|---|
| Swiss GTFS timetable | [opentransportdata.swiss](https://opentransportdata.swiss/en/cookbook/timetable-cookbook/gtfs/) |
| Agglomeration boundaries | [data.geo.admin.ch — agglomerations](https://data.geo.admin.ch/browser/index.html#/collections/ch.bfs.generalisierte-grenzen_agglomerationen_g1) |
| Canton / District / Commune boundaries | [data.geo.admin.ch — historisierte Grenzen](https://data.geo.admin.ch/browser/index.html#/collections/ch.bfs.historisierte-administrative_grenzen_g0) |

Place extracted GTFS text files under `Data/gtfs_fp<year>_<date>/` and shapefiles under `geodata/boundaries/` and `geodata/agglomerations/`.

---

## Pipeline

### Stage 1 — Filter GTFS by boundary

Clips the national GTFS feed to each region's boundary polygon and writes a filtered `.zip` per city.

```bash
# All agglomerations
gtfs-filter --scale agglomeration

# Specific cities and GTFS version
gtfs-filter --scale agglomeration --cities zurich bern fribourg --gtfs-version 20260408

# Custom paths
gtfs-filter --scale canton \
  --gtfs-path Data/gtfs_fp2026_20260408 \
  --data-dir Data \
  --geodata-dir geodata/
```

Output: `Data/{scale}/{version}/{city}.zip`

---

### Stage 2 — Build transit graphs

Downloads GTFS if needed, ensures boundary shapefiles are present, runs city2graph to build a travel-summary graph, and serialises outputs.

```bash
# Build graphs (downloads latest GTFS automatically)
gtfs-build --scale agglomeration

# Use local GTFS, process a subset, skip re-filtering
gtfs-build --scale agglomeration \
  --gtfs-path Data/gtfs_fp2026_20260408 \
  --cities zurich bern fribourg \
  --skip-filter \
  --output-dir outputs/graphs

# Pin a specific GTFS version
gtfs-build --scale canton --gtfs-version 20260408

# List available GTFS versions
gtfs-build --list-versions
```

**Key options:**

| Flag | Default | Description |
|---|---|---|
| `--scale` | required | `canton`, `agglomeration`, `district`, `commune` |
| `--gtfs-version`| None | Pin to a specific GTFS release (`YYYYMMDD`) |
| `--cities` | all | Specific city keys to process |
| `--start-time` / `--end-time` | `07:00:00` / `10:00:00` | Time window for service |
| `--save-format` | `both` | `gpkg`, `graphml`, or `both` |
| `--undirected` | directed | Build undirected graph |
| `--skip-filter` | off | Reuse existing filtered zips |
| `--force` | off | Overwrite existing graph outputs |

Output per city: `outputs/graphs/{scale}/{version}/{city}.gpkg`, `.graphml`, `.json`

---

### Stage 3 — Extract features

Builds gtfs2nx transit graphs, computes all-pairs shortest-path distance matrices, runs [Ripser](https://github.com/scikit-tda/ripser.py) to extract H0/H1 persistence diagrams, and vectorizes them into a feature matrix.

```bash
# Persistence statistics vectorization (default)
gtfs-features --scale agglomeration --gtfs-version 20260408

# Persistence landscape vectorization
gtfs-features --scale agglomeration --gtfs-version 20260408 --method landscape --n-points 500

# Specific cities, custom dirs
gtfs-features --scale agglomeration \
  --gtfs-version 20260408 \
  --cities zurich bern fribourg \
  --gtfs-dir Data \
  --diagrams-dir outputs/diagrams \
  --features-dir outputs/features
```

Output:
- `outputs/diagrams/{scale}/{version}/{city}_diagrams.npz` — H0 and H1 bars per city
- `outputs/features/{scale}/{version}/feature_matrix_{method}.npz` — full feature matrix

---

### Stage 4 — Cluster and map

Loads a feature matrix, scales features, applies a clustering method, and exports a geo-mapped HTML choropleth.

```bash
# KMeans (default)
gtfs-analyze --scale agglomeration --gtfs-version 20260408 --n-clusters 5

# Agglomerative clustering
gtfs-analyze --scale agglomeration --gtfs-version 20260408 --method agglomerative --n-clusters 4

# DBSCAN
gtfs-analyze --scale agglomeration --gtfs-version 20260408 --method dbscan --eps 0.5 --min-samples 3

# Use landscape features
gtfs-analyze --scale agglomeration --gtfs-version 20260408 --feature-method landscape
```

Output:
- `outputs/analysis/{scale}/{version}/clusters_{method}.csv`
- `outputs/analysis/{scale}/{version}/map_{method}.html`

---

## Notebooks

Three notebooks in `notebooks/` provide interactive exploration at each stage. They import from the package and contain no business logic.

| Notebook | Purpose |
|---|---|
| `01_graph_exploration.ipynb` | Load filtered GTFS, build graph, render folium transit map |
| `02_feature_exploration.ipynb` | Inspect persistence diagrams, visualise PCA of feature space |
| `03_analysis_maps.ipynb` | Cluster regions, render choropleth, UMAP scatter, DBSCAN comparison |

---

## Repository structure

```
src/
  swiss_gtfs/
    config.py              # Typed dataclasses for each pipeline stage
    mappings/
      regions.py           # Region name dicts and shapefile config for all scales
    data/
      gtfs_source.py       # GTFS version discovery, download, extraction
      boundaries.py        # STAC API boundary download
      filtering.py         # Spatial GTFS clipping to boundary polygon
    graphs/
      build.py             # city2graph travel_summary_graph → (nodes_gdf, edges_gdf)
      io.py                # Serialise to GeoPackage / GraphML + metadata sidecar
      distances.py         # All-pairs shortest-path distance matrix
      visualize.py         # Folium transit map renderer
    features/
      persistence.py       # Ripser H0/H1 diagram extraction
      vectorize.py         # Persistence statistics and landscape vectorization
    analysis/
      cluster.py           # KMeans / Agglomerative / DBSCAN with standard scaling
      geo_join.py          # Merge cluster labels onto boundary GeoDataFrame
    cli/
      filter_gtfs.py       # gtfs-filter entry point
      build_graphs.py      # gtfs-build entry point
      build_features.py    # gtfs-features entry point
      run_analysis.py      # gtfs-analyze entry point
notebooks/
  01_graph_exploration.ipynb
  02_feature_exploration.ipynb
  03_analysis_maps.ipynb
outputs/                   # Written at runtime, not tracked in git
  graphs/{scale}/{version}/          # .gpkg, .graphml, .json per city
  diagrams/{scale}/{version}/        # .npz per city (H0 + H1 persistence bars)
  features/{scale}/{version}/        # feature_matrix_{method}.npz
  analysis/{scale}/{version}/        # clusters_{method}.csv, map_{method}.html
Data/                      # Raw and filtered GTFS (not tracked in git)
geodata/                   # Boundary shapefiles (not tracked in git)
```

---

## Geographic scales

| Scale | Coverage | Key |
|---|---|---|
| `agglomeration` | ~78 Swiss urban agglomerations | snake_case name, e.g. `zurich`, `bern`, `st_gallen` |
| `canton` | 26 cantons | abbreviation or full name, e.g. `zh`, `zurich`, `be` |
| `district` | ~100 districts | snake_case name, e.g. `affoltern`, `bern`, `zurich` |
| `commune` | ~2000 communes | snake_case name |

All keys are defined in `src/swiss_gtfs/mappings/regions.py`.

---

## Example: full run from scratch

```bash
# 1. Download and filter (auto-downloads latest GTFS + boundaries)
gtfs-filter --scale agglomeration --cities zurich bern lausanne --gtfs-version 20260408

# 2. Build graphs
gtfs-build --scale agglomeration \
  --cities zurich bern lausanne \
  --skip-filter \
  --gtfs-version 20260408

# 3. Extract features
gtfs-features --scale agglomeration --cities zurich bern lausanne --gtfs-version 20260408

# 4. Cluster
gtfs-analyze --scale agglomeration --n-clusters 3 --gtfs-version 20260408
```

Or to run the full agglomeration scale in one pass:

```bash
gtfs-build --scale agglomeration --gtfs-version 20260408 && \
gtfs-features --scale agglomeration --gtfs-version 20260408 && \
gtfs-analyze --scale agglomeration --n-clusters 5 --gtfs-version 20260408
```
