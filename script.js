document.addEventListener('DOMContentLoaded', async () => {
    const executeButton = document.getElementById('execute-button');
    const sqlQuery = document.getElementById('sql-query');
    const geojsonSource = document.getElementById('geojson-source');
    const tableContainer = document.getElementById('table-container');
    const statusMessage = document.getElementById('status-message');
    const downloadCsvButton = document.getElementById('download-csv-button');

    const query1Button = document.getElementById('query1-button');
    const query2Button = document.getElementById('query2-button');
    const query3Button = document.getElementById('query3-button');
    
    const tablePanel = document.getElementById('table-panel');
    const mapPanel = document.getElementById('map-panel');
    const mapContainer = document.getElementById('map-container');

    let db;
    let currentData = [];
    let sortColumn = null;
    let sortDirection = 'asc';
    let currentPage = 1;
    const rowsPerPage = 10;
    let spatialEnabled = false;
    let map = null;
    let currentMapLayer = null;

    async function init() {
        try {
            const JSDELIVR_BUNDLES = window.duckdb.getJsDelivrBundles();
            const bundle = await window.duckdb.selectBundle(JSDELIVR_BUNDLES);
            const worker = await window.duckdb.createWorker(bundle.mainWorker);
            const logger = new window.duckdb.ConsoleLogger();
            db = new window.duckdb.AsyncDuckDB(logger, worker);
            await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
            
            // Load spatial extension
            await loadSpatialExtension();
            
            executeButton.disabled = false;
            updateStatusMessage();
        } catch (e) {
            statusMessage.textContent = `Error initializing DuckDB: ${e.toString()}`;
            console.error(e);
        }
    }

    async function loadSpatialExtension() {
        try {
            statusMessage.textContent = 'Loading spatial extension...';
            const conn = await db.connect();
            await conn.query('LOAD spatial;');
            
            // Test spatial extension is working
            await conn.query('SELECT ST_Point(0, 0) as test_point;');
            conn.close();
            
            spatialEnabled = true;
            console.log('Spatial extension loaded successfully');
        } catch (e) {
            spatialEnabled = false;
            console.warn('Spatial extension not available:', e.message);
        }
    }

    function updateStatusMessage() {
        if (spatialEnabled) {
            statusMessage.textContent = 'DuckDB with spatial extension is ready. Enter a query and click Execute.';
            // Update button labels for spatial queries
            query1Button.textContent = 'Largest Grid Cells';
            query2Button.textContent = 'Grid Centroids (48-49°N)';
            query3Button.textContent = 'Grids in Bounding Box';
        } else {
            statusMessage.textContent = 'DuckDB is ready (spatial extension unavailable). Enter a query and click Execute.';
            // Update button labels for fallback queries
            query1Button.textContent = 'Grid Sample';
            query2Button.textContent = 'Count Grid Cells';
            query3Button.textContent = 'Grid Properties';
        }
    }

    await init();

    // Initialize map immediately
    function initializeMap() {
        if (!map) {
            map = L.map(mapContainer).setView([48.5, -122], 8);
            L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
                attribution: '© OpenStreetMap contributors'
            }).addTo(map);
        }
    }

    // Initialize map on page load
    initializeMap();
    
    // Trigger map resize after a short delay to ensure proper sizing
    setTimeout(() => map.invalidateSize(), 100);

    // Update query when source changes
    function updateQueryWithSource() {
        const currentQuery = sqlQuery.value;
        if (currentQuery && currentQuery.includes('{SOURCE}')) {
            // Query already has placeholder, no need to update
            return;
        }
        
        // If query is empty or doesn't have placeholder, set default query
        if (!currentQuery.trim()) {
            sqlQuery.value = sqlQuery.placeholder;
        }
    }

    // Auto-update query when source field changes
    geojsonSource.addEventListener('input', updateQueryWithSource);
    geojsonSource.addEventListener('change', updateQueryWithSource);

    // Set default query on page load if query is empty
    if (!sqlQuery.value.trim()) {
        sqlQuery.value = sqlQuery.placeholder;
    }

    // Geometry detection and parsing functions
    function detectGeometryColumns(data) {
        if (!data || data.length === 0) return [];
        
        const columns = Object.keys(data[0]);
        const geometryColumns = [];
        
        console.log('Checking columns:', columns);
        
        // Check for common geometry column names
        if (columns.includes('geom')) {
            console.log('Found geom column');
            geometryColumns.push({ name: 'geom', type: 'geometry' });
        }
        if (columns.includes('geometry')) {
            console.log('Found geometry column');
            geometryColumns.push({ name: 'geometry', type: 'geojson_string' });
        }
        
        // Check for WKT columns (containing geometry text)
        columns.forEach(col => {
            const sampleValue = data[0][col];
            const preview = typeof sampleValue === 'string' ? 
                sampleValue.substring(0, 50) + '...' : 
                String(sampleValue);
            console.log(`Checking column ${col}:`, typeof sampleValue, preview);
            
            if (typeof sampleValue === 'string' && isWKT(sampleValue)) {
                console.log(`Found WKT column: ${col}`);
                geometryColumns.push({ name: col, type: 'wkt' });
            }
        });
        
        // Also check for any column that looks like it contains geometry data
        columns.forEach(col => {
            const sampleValue = data[0][col];
            if (typeof sampleValue === 'object' && sampleValue && 
                (sampleValue.type || sampleValue.coordinates)) {
                console.log(`Found GeoJSON column: ${col}`);
                geometryColumns.push({ name: col, type: 'geojson' });
            }
        });
        
        return geometryColumns;
    }

    function isWKT(str) {
        if (typeof str !== 'string') return false;
        // More flexible WKT detection - look for geometry type keywords
        const wktPattern = /^(POINT|LINESTRING|POLYGON|MULTIPOINT|MULTILINESTRING|MULTIPOLYGON|GEOMETRYCOLLECTION)\s*\(/i;
        const result = wktPattern.test(str.trim());
        console.log(`Testing WKT for "${str.substring(0, 30)}...": ${result}`);
        return result;
    }

    function parseWKTToGeoJSON(wktString) {
        try {
            // Simple WKT parser for basic geometries
            const wkt = wktString.trim();
            
            if (wkt.startsWith('POINT')) {
                const coords = wkt.match(/POINT\s*\(\s*([^)]+)\)/i);
                if (coords) {
                    const [lon, lat] = coords[1].split(/\s+/).map(Number);
                    return {
                        type: 'Point',
                        coordinates: [lon, lat]
                    };
                }
            }
            
            if (wkt.startsWith('POLYGON')) {
                // Basic polygon parsing - simplified for demo
                const coords = wkt.match(/POLYGON\s*\(\s*\(([^)]+)\)\s*\)/i);
                if (coords) {
                    const points = coords[1].split(',').map(point => {
                        const [lon, lat] = point.trim().split(/\s+/).map(Number);
                        return [lon, lat];
                    });
                    return {
                        type: 'Polygon',
                        coordinates: [points]
                    };
                }
            }
        } catch (e) {
            console.warn('Failed to parse WKT:', wktString, e);
        }
        return null;
    }

    function convertDataToGeoJSON(data, geometryColumns) {
        console.log('convertDataToGeoJSON called with:', { data: data.length, geometryColumns });
        if (!geometryColumns.length) return null;
        
        const features = [];
        
        data.forEach((row, index) => {
            const geomCol = geometryColumns[0]; // Use first geometry column
            let geometry = null;
            
            console.log(`Processing row ${index}, geomCol:`, geomCol, 'value:', row[geomCol.name]);
            
            if (geomCol.type === 'wkt' && row[geomCol.name]) {
                geometry = parseWKTToGeoJSON(row[geomCol.name]);
                console.log('Parsed WKT to geometry:', geometry);
            } else if (geomCol.type === 'geojson_string' && row[geomCol.name]) {
                // Handle GeoJSON string from ST_AsGeoJSON()
                try {
                    geometry = JSON.parse(row[geomCol.name]);
                    console.log('Parsed GeoJSON string to geometry:', geometry);
                } catch (e) {
                    console.error('Failed to parse GeoJSON string:', e);
                }
            } else if (geomCol.type === 'geometry' && row[geomCol.name]) {
                // Handle native geometry from ST_Read - it might be in WKB or GeoJSON format
                const geomData = row[geomCol.name];
                console.log('Native geometry data:', typeof geomData, geomData);
                
                if (typeof geomData === 'object' && geomData.type) {
                    // Already GeoJSON format
                    geometry = geomData;
                } else if (typeof geomData === 'string' && isWKT(geomData)) {
                    // WKT string
                    geometry = parseWKTToGeoJSON(geomData);
                }
                console.log('Processed native geometry:', geometry);
            }
            
            if (geometry) {
                const properties = { ...row };
                delete properties[geomCol.name]; // Remove geom from properties
                
                features.push({
                    type: 'Feature',
                    geometry: geometry,
                    properties: {
                        ...properties,
                        _row_id: index // Add row identifier for selection sync
                    }
                });
            }
        });
        
        console.log('Created GeoJSON with', features.length, 'features');
        return {
            type: 'FeatureCollection',
            features: features
        };
    }

    // Map rendering functions
    function renderMapData(geoJsonData) {
        if (!map || !geoJsonData || !geoJsonData.features.length) return;
        
        // Clear existing layer
        if (currentMapLayer) {
            map.removeLayer(currentMapLayer);
        }
        
        // Style function for features
        function getFeatureStyle(feature) {
            const properties = feature.properties;
            
            // Color based on area if available
            let color = '#3388ff';
            if (properties.area_sq_degrees) {
                const area = parseFloat(properties.area_sq_degrees);
                if (area > 0.1) color = '#ff6b6b';
                else if (area > 0.01) color = '#4ecdc4';
                else color = '#45b7d1';
            }
            
            return {
                color: color,
                weight: 2,
                opacity: 0.8,
                fillOpacity: 0.4
            };
        }
        
        // Point style function
        function getPointStyle(feature) {
            const properties = feature.properties;
            let radius = 5;
            
            if (properties.area_sq_degrees) {
                const area = parseFloat(properties.area_sq_degrees);
                radius = Math.min(Math.max(area * 1000, 3), 15);
            }
            
            return {
                radius: radius,
                fillColor: getFeatureStyle(feature).color,
                color: '#000',
                weight: 1,
                opacity: 1,
                fillOpacity: 0.7
            };
        }
        
        // Create layer with popups
        currentMapLayer = L.geoJSON(geoJsonData, {
            style: getFeatureStyle,
            pointToLayer: function(feature, latlng) {
                return L.circleMarker(latlng, getPointStyle(feature));
            },
            onEachFeature: function(feature, layer) {
                // Create popup content
                const props = feature.properties;
                let popupContent = '<div class="popup-content">';
                
                Object.keys(props).forEach(key => {
                    if (key !== '_row_id' && props[key] !== null) {
                        popupContent += `<strong>${key}:</strong> ${props[key]}<br>`;
                    }
                });
                
                popupContent += '</div>';
                layer.bindPopup(popupContent);
                
                // Highlight on hover
                layer.on('mouseover', function(e) {
                    this.setStyle({
                        weight: 4,
                        opacity: 1
                    });
                });
                
                layer.on('mouseout', function(e) {
                    currentMapLayer.resetStyle(this);
                });
            }
        }).addTo(map);
        
        // Fit map to data bounds
        if (currentMapLayer.getBounds().isValid()) {
            map.fitBounds(currentMapLayer.getBounds(), { padding: [20, 20] });
        }
    }

    function updateMapData() {
        const geometryColumns = detectGeometryColumns(currentData);
        const hasGeometry = geometryColumns.length > 0;
        
        console.log('Detected geometry columns:', geometryColumns);
        console.log('Current data sample:', currentData.slice(0, 2));
        console.log('Available columns:', currentData.length > 0 ? Object.keys(currentData[0]) : 'No data');
        if (currentData.length > 0) {
            console.log('Sample values:', currentData[0]);
        }
        
        // Always try to render geometry data if available
        if (hasGeometry) {
            const geoJsonData = convertDataToGeoJSON(currentData, geometryColumns);
            if (geoJsonData) {
                console.log('Rendering map data:', geoJsonData);
                renderMapData(geoJsonData);
            }
        } else {
            console.log('No geometry data detected');
            // Clear the map if no geometry data
            if (currentMapLayer) {
                map.removeLayer(currentMapLayer);
                currentMapLayer = null;
            }
        }
    }

    // Function to substitute source URL in query templates
    function substituteSource(queryTemplate) {
        const sourceUrl = geojsonSource.value.trim();
        if (!sourceUrl) {
            alert('Please enter a GeoJSON source URL');
            return null;
        }
        return queryTemplate.replace(/\{SOURCE\}/g, `'${sourceUrl}'`);
    }

    // Event listeners for pre-defined query buttons
    query1Button.addEventListener('click', () => {
        sqlQuery.value = query1Button.dataset.query;
        executeButton.click();
    });

    query2Button.addEventListener('click', () => {
        sqlQuery.value = query2Button.dataset.query;
        executeButton.click();
    });

    query3Button.addEventListener('click', () => {
        sqlQuery.value = query3Button.dataset.query;
        executeButton.click();
    });

    executeButton.addEventListener('click', async () => {
        let query = sqlQuery.value;
        if (!query) {
            alert('Please enter a SQL query.');
            return;
        }

        // Substitute {SOURCE} placeholders with actual URL
        if (query.includes('{SOURCE}')) {
            const sourceUrl = geojsonSource.value.trim();
            if (!sourceUrl) {
                alert('Please enter a GeoJSON source URL');
                return;
            }
            query = query.replace(/\{SOURCE\}/g, `'${sourceUrl}'`);
        }

        try {
            const conn = await db.connect();
            const result = await conn.query(query);
            conn.close();

            if (result.numRows > 0) {
                currentData = result.toArray().map(row => row.toJSON());
                console.log('Query executed, new data:', currentData);
                sortColumn = null;
                sortDirection = 'asc';
                currentPage = 1;
                renderTable();
                console.log('About to call updateMapData...');
                updateMapData();
                console.log('updateMapData called');
                downloadCsvButton.style.display = 'block'; // Show the download button
            } else {
                tableContainer.innerHTML = '<p>No results found.</p>';
                downloadCsvButton.style.display = 'none'; // Hide the download button
            }
        } catch (error) {
            console.error(error);
            tableContainer.innerHTML = `<p>Error: ${error.message}</p>`;
        }
    });

    function renderTable() {
        tableContainer.innerHTML = ''; // Clear previous table

        if (currentData.length === 0) {
            tableContainer.innerHTML = '<p>No results found.</p>';
            return;
        }

        const allColumns = Object.keys(currentData[0]);
        // Filter out geometry columns from table display
        const columns = allColumns.filter(col => {
            // Hide geometry, geom, and any column with WKT/GeoJSON data
            if (col === 'geometry' || col === 'geom') return false;
            
            const sampleValue = currentData[0][col];
            if (typeof sampleValue === 'string' && isWKT(sampleValue)) return false;
            if (typeof sampleValue === 'object' && sampleValue instanceof Uint8Array) return false;
            
            return true;
        });

        // Sort data
        let sortedData = [...currentData];
        if (sortColumn) {
            sortedData.sort((a, b) => {
                const valA = a[sortColumn];
                const valB = b[sortColumn];

                if (valA < valB) return sortDirection === 'asc' ? -1 : 1;
                if (valA > valB) return sortDirection === 'asc' ? 1 : -1;
                return 0;
            });
        }

        // Paginate data
        const totalPages = Math.ceil(sortedData.length / rowsPerPage);
        const startIndex = (currentPage - 1) * rowsPerPage;
        const endIndex = startIndex + rowsPerPage;
        const paginatedData = sortedData.slice(startIndex, endIndex);

        const table = d3.select(tableContainer).append('table').attr('class', 'table table-striped table-hover');
        const thead = table.append('thead');
        const tbody = table.append('tbody');

        // Append the header row
        thead.append('tr')
            .selectAll('th')
            .data(columns)
            .enter()
            .append('th')
            .text(d => d)
            .on('click', (event, column) => {
                if (sortColumn === column) {
                    sortDirection = sortDirection === 'asc' ? 'desc' : 'asc';
                } else {
                    sortColumn = column;
                    sortDirection = 'asc';
                }
                renderTable();
            });

        // Append the data rows
        const rows = tbody.selectAll('tr')
            .data(paginatedData)
            .enter()
            .append('tr');

        rows.selectAll('td')
            .data(row => columns.map(column => row[column]))
            .enter()
            .append('td')
            .html(d => {
                if (typeof d === 'number') {
                    return d3.format(".4~f")(d);
                }
                return d;
            });

        // Pagination controls
        const paginationDiv = d3.select(tableContainer).append('div').attr('class', 'pagination mt-3');

        paginationDiv.append('button')
            .attr('class', 'btn btn-secondary me-2')
            .attr('disabled', currentPage === 1 ? true : null)
            .text('Previous')
            .on('click', () => {
                currentPage--;
                renderTable();
            });

        paginationDiv.append('span').text(`Page ${currentPage} of ${totalPages}`);

        paginationDiv.append('button')
            .attr('class', 'btn btn-secondary ms-2')
            .attr('disabled', currentPage === totalPages ? true : null)
            .text('Next')
            .on('click', () => {
                currentPage++;
                renderTable();
            });
    }

    downloadCsvButton.addEventListener('click', () => {
        if (currentData.length === 0) {
            alert('No data to download.');
            return;
        }

        const columns = Object.keys(currentData[0]);
        let csvContent = columns.join(',') + '\n';

        currentData.forEach(row => {
            const rowValues = columns.map(col => {
                let value = row[col];
                if (typeof value === 'string' && value.includes(',')) {
                    value = `"${value.replace(/"/g, '""')}"`; // Enclose in double quotes and escape existing double quotes
                }
                return value;
            });
            csvContent += rowValues.join(',') + '\n';
        });

        const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
        const link = document.createElement('a');
        if (link.download !== undefined) { // Feature detection for download attribute
            const url = URL.createObjectURL(blob);
            link.setAttribute('href', url);
            link.setAttribute('download', 'query_results.csv');
            link.style.visibility = 'hidden';
            document.body.appendChild(link);
            link.click();
            document.body.removeChild(link);
            URL.revokeObjectURL(url);
        } else {
            alert('Your browser does not support downloading files directly. Please copy the data manually.');
        }
    });
});