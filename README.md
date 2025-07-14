# DuckDB Spatial GeoJSON Analysis

A static web application that performs spatial queries and geoprocessing directly in the browser using DuckDB WebAssembly.

## Overview

This application performs spatial queries of GeoJSON data without requiring server-side processing. All computations run entirely in the browser using web assembly.

## Technology Stack

- **DuckDB WASM**: SQL engine 
- **Leaflet.js**: Interactive map visualization
- **D3.js**: Dynamic table rendering and data visualization

## Getting Started

### Running Locally

Since this is a static web application that uses ES6 modules, you need to serve it over HTTP:

```bash
# Using Python
python -m http.server 8000

# Using Node.js
npx serve .

# Using PHP
php -S localhost:8000
```

Then open http://localhost:8000 in your browser.

### Usage

1. **Set Data Source**: Enter a GeoJSON URL in the source field
2. **Write Queries**: Use SQL with `{SOURCE}` placeholder for the data source
3. **Execute**: Run spatial analysis queries in real-time
4. **Visualize**: View results in both table and map formats

### Example Queries

```sql
-- Calculate areas of polygons
WITH datasource AS (SELECT * FROM ST_Read({SOURCE}))
SELECT properties.name, 
       ST_Area(geom) as area_sq_degrees,
       ST_AsGeoJSON(geom) as geometry
FROM datasource 
ORDER BY area_sq_degrees DESC 
LIMIT 10;

-- Find features within bounding box
WITH datasource AS (SELECT * FROM ST_Read({SOURCE}))
SELECT properties.name, ST_AsGeoJSON(geom) as geometry
FROM datasource 
WHERE ST_Within(geom, ST_MakeEnvelope(-123, 48, -121, 49));

-- Calculate centroids
WITH datasource AS (SELECT * FROM ST_Read({SOURCE}))
SELECT properties.name, 
       ST_AsText(ST_Centroid(geom)) as centroid
FROM datasource;
```

## Spatial Functions

The application provides access to DuckDB's spatial extension functions:

- `ST_Read()` - Read spatial data from remote sources
- `ST_AsGeoJSON()` - Convert geometry to GeoJSON
- `ST_Area()` - Calculate area of geometries
- `ST_Centroid()` - Find centroid of geometries
- `ST_Within()` - Spatial containment testing
- `ST_MakeEnvelope()` - Create bounding box geometries
- `ST_Distance()` - Calculate spatial distances

## Browser Requirements

- Modern browser with WebAssembly support
- JavaScript ES6 modules support
- CORS-enabled for fetching remote GeoJSON files

## Development

The application consists of three main files:

- `index.html` - Main interface with Bootstrap UI
- `script.js` - JavaScript module handling DuckDB initialization and spatial operations
- `style.css` - Custom styling for table and map components

---

