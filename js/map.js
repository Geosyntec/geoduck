// Global variables
let map = null;
let marker = null;
let selectedCoordinates = null;
let db = null;
let currentData = null;

// Convert relative path to absolute URL for DuckDB WASM
function toAbsoluteUrl(path) {
    if (path.startsWith('http://') || path.startsWith('https://')) {
        return path;
    }
    return new URL(path, window.location.href).href;
}

// Initialize map function
function initializeMap() {
    // Check if map container exists
    const mapContainer = document.getElementById('map-container');
    if (!mapContainer) {
        console.error('Map container with ID "map-container" not found');
        return;
    }

    // Check if Leaflet is loaded
    if (typeof L === 'undefined') {
        console.error('Leaflet library not loaded');
        return;
    }

    // If map already exists, just refresh it
    if (map) {
        map.invalidateSize();
        return;
    }

    try {
        // Create the map instance centered on Puget Sound
        map = L.map('map-container').setView([47.6062, -122.3321], 9);

        // Add tile layer (OpenStreetMap)
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors',
            maxZoom: 19
        }).addTo(map);

        // Add click event listener
        map.on('click', async function(e) {
            const lat = e.latlng.lat.toFixed(6);
            const lng = e.latlng.lng.toFixed(6);
            
            // Add marker at clicked location
            if (marker) {
                map.removeLayer(marker);
            }
            marker = L.marker([lat, lng])
                .addTo(map)
                .bindPopup(`<b>Clicked Location</b><br>Lat: ${lat}<br>Lng: ${lng}`)
                .openPopup();
            
            // Query the grid data for this location
            await findContainingGrid(lat, lng, `Location at ${lat}, ${lng}`);
            updateStatus(`Querying grid data for ${lat}, ${lng}`);
        });

        // Position zoom controls
        map.zoomControl.setPosition('bottomright');

        console.log('Map initialized successfully');
        
    } catch (error) {
        console.error('Error initializing map:', error);
    }
}


// Force map initialization (call this if map doesn't appear)
function forceInitializeMap() {
    // Destroy existing map if it exists
    if (map) {
        map.remove();
        map = null;
    }
    
    // Wait a moment then initialize
    setTimeout(() => {
        initializeMap();
    }, 100);
}

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Wait for all resources to load
    setTimeout(() => {
        initializeMap();
    }, 500);
});

// Also try to initialize when window loads (backup)
window.addEventListener('load', function() {
    if (!map) {
        setTimeout(() => {
            initializeMap();
        }, 200);
    }
});

// Initialize DuckDB WASM with spatial extension
async function initializeDuckDB() {
    try {
        const bundle = await window.duckdb.selectBundle(window.duckdb.getJsDelivrBundles());
        const worker = await window.duckdb.createWorker(bundle.mainWorker);
        db = new window.duckdb.AsyncDuckDB(new window.duckdb.ConsoleLogger(), worker);
        await db.instantiate(bundle.mainModule, bundle.pthreadWorker);
        
        // Install and load spatial extension
        const conn = await db.connect();
        await conn.query("INSTALL spatial");
        await conn.query("LOAD spatial");
        conn.close();
        
        updateStatus('DuckDB WASM initialized with spatial extension');
        document.getElementById('execute-button').disabled = false;
    } catch (error) {
        console.error('Error initializing DuckDB:', error);
        updateStatus('Error initializing DuckDB: ' + error.message);
    }
}

// Update status message
function updateStatus(message) {
    const statusElement = document.getElementById('status-message');
    if (statusElement) {
        statusElement.textContent = message;
    }
}

// Geocoding helper functions
async function searchWithNominatim(searchTerm) {
    try {
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(searchTerm)}&limit=1&countrycodes=us&viewbox=-124.848974,49.002494,-116.916,45.543541&bounded=1`);
        if (!response.ok) throw new Error(`Nominatim API error: ${response.status}`);
        
        const data = await response.json();
        if (data && data.length > 0) {
            const result = data[0];
            return {
                lat: parseFloat(result.lat),
                lng: parseFloat(result.lon),
                display_name: result.display_name,
                source: 'Nominatim'
            };
        }
        return null;
    } catch (error) {
        console.warn('Nominatim geocoding failed:', error);
        return null;
    }
}

async function geocodeLocation(searchTerm) {
    const result = await searchWithNominatim(searchTerm);
    if (result) {
        console.log(`Geocoded "${searchTerm}" using ${result.source}`);
        return result;
    }
    
    return null;
}

// Autocomplete functionality
let autocompleteTimeout = null;
let currentSuggestions = [];
let selectedSuggestionIndex = -1;

async function getSuggestionsFromNominatim(query) {
    try {
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(query)}&limit=10&countrycodes=us&viewbox=-124.848974,49.002494,-116.916,45.543541&bounded=1&addressdetails=1`);
        if (!response.ok) return [];
        
        const data = await response.json();
        if (!data) return [];
        
        // Filter to include addresses and cities
        const filteredResults = data.filter(result => {
            const address = result.address;
            return address && (
                address.house_number || 
                address.building || 
                address.street || 
                address.road ||
                address.city ||
                address.town ||
                address.village
            );
        });
        
        // Deduplicate results by name and coordinates
        const uniqueResults = [];
        const seen = new Set();
        
        for (const result of filteredResults) {
            const name = result.display_name;
            const lat = parseFloat(result.lat).toFixed(4);
            const lng = parseFloat(result.lon).toFixed(4);
            const key = `${name}|${lat},${lng}`;
            
            if (!seen.has(key)) {
                seen.add(key);
                uniqueResults.push({
                    name: name,
                    lat: parseFloat(result.lat),
                    lng: parseFloat(result.lon)
                });
            }
        }
        
        return uniqueResults.slice(0, 5);
    } catch (error) {
        console.warn('Nominatim suggestions failed:', error);
        return [];
    }
}

async function getSuggestions(query) {
    if (query.length < 2) return [];
    
    // Use Nominatim for suggestions
    const suggestions = await getSuggestionsFromNominatim(query);
    return suggestions;
}

function showSuggestions(suggestions) {
    const dropdown = document.getElementById('suggestions-dropdown');
    if (!dropdown) return;
    
    if (suggestions.length === 0) {
        dropdown.style.display = 'none';
        return;
    }
    
    currentSuggestions = suggestions;
    selectedSuggestionIndex = -1;
    
    dropdown.innerHTML = suggestions.map((suggestion, index) => {
        return `
            <div class="suggestion-item" data-index="${index}">
                <div class="suggestion-name">${suggestion.name}</div>
            </div>
        `;
    }).join('');
    
    dropdown.style.display = 'block';
    
    // Add click handlers to suggestion items
    dropdown.querySelectorAll('.suggestion-item').forEach(item => {
        item.addEventListener('click', () => {
            const index = parseInt(item.dataset.index);
            selectSuggestion(index);
        });
    });
}

function hideSuggestions() {
    const dropdown = document.getElementById('suggestions-dropdown');
    if (dropdown) {
        dropdown.style.display = 'none';
    }
    currentSuggestions = [];
    selectedSuggestionIndex = -1;
}

function selectSuggestion(index) {
    if (index < 0 || index >= currentSuggestions.length) return;
    
    const suggestion = currentSuggestions[index];
    const searchInput = document.getElementById('address-search');
    
    if (searchInput) {
        searchInput.value = suggestion.name;
    }
    
    hideSuggestions();
    
    // Trigger search with selected suggestion
    performSearch(suggestion);
}

async function performSearch(suggestion) {
    const { lat, lng, name } = suggestion;
    
    // Center map and add marker
    if (map) {
        map.setView([lat, lng], 10);
        
        if (marker) {
            map.removeLayer(marker);
        }
        marker = L.marker([lat, lng])
            .addTo(map)
            .bindPopup(`<b>${name}</b><br>Lat: ${lat}<br>Lng: ${lng}`)
            .openPopup();
    }
    
    // Find which grid cell contains this point
    await findContainingGrid(lat, lng, name);
    updateStatus(`Found: ${name}`);
}

function updateSuggestionHighlight() {
    const dropdown = document.getElementById('suggestions-dropdown');
    if (!dropdown) return;
    
    // Remove existing highlights
    dropdown.querySelectorAll('.suggestion-item').forEach((item, index) => {
        item.classList.toggle('highlighted', index === selectedSuggestionIndex);
    });
    
    // Scroll highlighted item into view
    if (selectedSuggestionIndex >= 0) {
        const highlightedItem = dropdown.querySelector('.suggestion-item.highlighted');
        if (highlightedItem) {
            highlightedItem.scrollIntoView({ block: 'nearest' });
        }
    }
}

// Handle address search
async function handleAddressSearch() {
    const searchInput = document.getElementById('address-search');
    const searchTerm = searchInput ? searchInput.value.trim() : '';
    
    if (!searchTerm) {
        alert('Please enter an address or place name');
        return;
    }

    if (!db) {
        alert('DuckDB not initialized yet. Please wait.');
        return;
    }

    try {
        updateStatus('Searching for location...');
        
        // Use Nominatim geocoding
        const result = await geocodeLocation(searchTerm);
        
        if (result) {
            const { lat, lng, display_name } = result;
            
            // Center map and add marker
            if (map) {
                map.setView([lat, lng], 10);
                
                if (marker) {
                    map.removeLayer(marker);
                }
                marker = L.marker([lat, lng])
                    .addTo(map)
                    .bindPopup(`<b>${display_name}</b><br>Lat: ${lat}<br>Lng: ${lng}`)
                    .openPopup();
            }
            
            // Now find which grid cell contains this point
            await findContainingGrid(lat, lng, display_name);
            
        } else {
            alert('Location not found. Please try a different search term.');
            updateStatus('Ready');
        }
    } catch (error) {
        console.error('Error searching for location:', error);
        alert('Error searching for location. Please try again.');
        updateStatus('Ready');
    }
}

// Find grid cell containing the specified coordinates
async function findContainingGrid(lat, lng, locationName) {
    const sourceInput = document.getElementById('geojson-source');
    const geojsonPath = sourceInput ? sourceInput.value.trim() : '';

    if (!geojsonPath) {
        updateStatus(`Found: ${locationName} (no grid data source specified)`);
        return;
    }

    try {
        updateStatus('Finding containing grid cell...');

        const geojsonUrl = toAbsoluteUrl(geojsonPath);
        const conn = await db.connect();

        // Query to find grid cells that contain the point
        const query = `
            WITH grid_data AS (
                SELECT * FROM ST_Read('${geojsonUrl}')
            )
            SELECT 
                grid_id,
                lat_vector,
                lon_vector,
                ghs_vector,
                ST_AsGeoJSON(geom) as geometry,
               
            FROM grid_data 
            WHERE ST_Within(ST_Point(${lng}, ${lat}), geom)
            LIMIT 1
        `;
        
        const result = await conn.query(query);
        const gridData = result.toArray().map(row => row.toJSON());
        conn.close();
        
        if (gridData && gridData.length > 0) {
            const gridCell = gridData[0];
            
            // Update marker popup with grid information
            if (marker) {
                const popupContent = `
                    <div>
                        <b>${locationName}</b><br>
                        <b>Coordinates:</b> ${lat}, ${lng}<br>
                        <hr>
                        <b>Grid Cell:</b> ${gridCell.grid_id}<br>
                        <b>Geohash:</b> ${gridCell.ghs_vector}<br>
                       
                    </div>
                `;
                marker.bindPopup(popupContent).openPopup();
            }
            
            // Visualize the containing grid cell
            visualizeGridCell(gridCell);
            
            // Display grid info in results table
            displayResults([gridCell]);
            
            updateStatus(`Found: ${locationName} in grid cell ${gridCell.grid_id}`);
            
        } else {
            updateStatus(`Found: ${locationName} (no containing grid cell found)`);
            
            // Still update marker popup even if no grid found
            if (marker) {
                const popupContent = `
                    <div>
                        <b>${locationName}</b><br>
                        <b>Coordinates:</b> ${lat}, ${lng}<br>
                        <i>No containing grid cell found</i>
                    </div>
                `;
                marker.bindPopup(popupContent).openPopup();
            }
        }
        
    } catch (error) {
        console.error('Error finding containing grid:', error);
        updateStatus(`Found: ${locationName} (error querying grid data)`);
        
        // Update marker popup with error info
        if (marker) {
            const popupContent = `
                <div>
                    <b>${locationName}</b><br>
                    <b>Coordinates:</b> ${lat}, ${lng}<br>
                    <i>Error querying grid data: ${error.message}</i>
                </div>
            `;
            marker.bindPopup(popupContent).openPopup();
        }
    }
}

// Visualize a single grid cell on the map
function visualizeGridCell(gridCell) {
    if (!map || !gridCell || !gridCell.geometry) return;
    
    // Clear existing grid layers (but keep marker and base layer)
    map.eachLayer(layer => {
        if (layer.options && layer.options.isGridLayer) {
            map.removeLayer(layer);
        }
    });
    
    try {
        const geometry = typeof gridCell.geometry === 'string' ? 
            JSON.parse(gridCell.geometry) : gridCell.geometry;
        
        const gridLayer = L.geoJSON(geometry, {
            style: {
                color: '#ff6b35',
                weight: 3,
                opacity: 0.9,
                fillColor: '#ff6b35',
                fillOpacity: 0.2
            },
            isGridLayer: true  // Custom property to identify grid layers
        });
        
        gridLayer.addTo(map);
        
        // Fit map to show both the point and the grid cell
        const bounds = gridLayer.getBounds();
        if (bounds.isValid()) {
            map.fitBounds(bounds, { padding: [20, 20] });
        }
        
    } catch (error) {
        console.warn('Error visualizing grid cell:', error);
    }
}

// Execute SQL query
async function executeQuery() {
    if (!db) {
        alert('DuckDB not initialized yet. Please wait.');
        return;
    }
    
    const sourceInput = document.getElementById('geojson-source');
    const queryInput = document.getElementById('sql-query');

    const sourcePath = sourceInput ? sourceInput.value.trim() : '';
    const queryTemplate = queryInput ? queryInput.value.trim() : '';

    if (!sourcePath || !queryTemplate) {
        alert('Please provide both a GeoJSON source and SQL query');
        return;
    }

    // Replace {SOURCE} placeholder with absolute URL
    const sourceUrl = toAbsoluteUrl(sourcePath);
    const query = queryTemplate.replace(/\{SOURCE\}/g, `'${sourceUrl}'`);
    
    try {
        updateStatus('Executing query...');
        const conn = await db.connect();
        const result = await conn.query(query);
        const data = result.toArray().map(row => row.toJSON());
        conn.close();
        
        currentData = data;
        displayResults(data);
        visualizeOnMap(data);
        updateStatus(`Query completed. ${data.length} rows returned.`);
        
    } catch (error) {
        console.error('Query error:', error);
        alert('Query error: ' + error.message);
        updateStatus('Query failed');
    }
}

// Display results in table
function displayResults(data) {
    const container = document.getElementById('table-container');
    if (!container || !data || data.length === 0) {
        if (container) container.innerHTML = '<p>No data to display</p>';
        return;
    }
    
    const columns = Object.keys(data[0]);
    
    let html = '<table class="table table-striped table-hover"><thead><tr>';
    columns.forEach(col => {
        html += `<th>${col}</th>`;
    });
    html += '</tr></thead><tbody>';
    
    data.slice(0, 100).forEach(row => { // Limit to 100 rows for performance
        html += '<tr>';
        columns.forEach(col => {
            let value = row[col];
            if (typeof value === 'string' && value.length > 100) {
                value = value.substring(0, 100) + '...';
            }
            html += `<td>${value || ''}</td>`;
        });
        html += '</tr>';
    });
    
    html += '</tbody></table>';
    container.innerHTML = html;
    
    // Show download button
    const downloadBtn = document.getElementById('download-csv-button');
    if (downloadBtn) {
        downloadBtn.style.display = 'block';
    }
}

// Visualize GeoJSON data on map
function visualizeOnMap(data) {
    if (!map || !data || data.length === 0) return;
    
    // Clear existing layers except base map and marker
    map.eachLayer(layer => {
        if (layer !== map._layers[Object.keys(map._layers)[0]] && layer !== marker) {
            map.removeLayer(layer);
        }
    });
    
    // Look for geometry column
    const geometryCol = Object.keys(data[0]).find(col => 
        col.toLowerCase().includes('geometry') || col.toLowerCase().includes('geom')
    );
    
    if (!geometryCol) return;
    
    const geojsonLayer = L.geoJSON([], {
        style: {
            color: '#3388ff',
            weight: 2,
            opacity: 0.8,
            fillOpacity: 0.3
        },
        onEachFeature: (feature, layer) => {
            if (feature.properties) {
                let popupContent = '<div>';
                Object.keys(feature.properties).forEach(key => {
                    popupContent += `<b>${key}:</b> ${feature.properties[key]}<br>`;
                });
                popupContent += '</div>';
                layer.bindPopup(popupContent);
            }
        }
    });
    
    // Add features to layer
    data.forEach(row => {
        try {
            const geom = row[geometryCol];
            if (geom) {
                const feature = typeof geom === 'string' ? JSON.parse(geom) : geom;
                if (feature.type === 'Feature' || feature.type === 'FeatureCollection') {
                    geojsonLayer.addData(feature);
                } else {
                    // Create feature from geometry
                    geojsonLayer.addData({
                        type: 'Feature',
                        geometry: feature,
                        properties: Object.keys(row).reduce((props, key) => {
                            if (key !== geometryCol) props[key] = row[key];
                            return props;
                        }, {})
                    });
                }
            }
        } catch (e) {
            console.warn('Error processing geometry:', e);
        }
    });
    
    geojsonLayer.addTo(map);
    
    // Fit map to data bounds
    if (geojsonLayer.getBounds().isValid()) {
        map.fitBounds(geojsonLayer.getBounds());
    }
}

// Download CSV
function downloadCSV() {
    if (!currentData || currentData.length === 0) {
        alert('No data to download');
        return;
    }
    
    const columns = Object.keys(currentData[0]);
    let csv = columns.join(',') + '\n';
    
    currentData.forEach(row => {
        const values = columns.map(col => {
            const value = row[col];
            if (typeof value === 'string' && value.includes(',')) {
                return `"${value.replace(/"/g, '""')}"`;
            }
            return value || '';
        });
        csv += values.join(',') + '\n';
    });
    
    const blob = new Blob([csv], { type: 'text/csv' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'query_results.csv';
    a.click();
    URL.revokeObjectURL(url);
}

// Set example queries
function setExampleQuery(queryText) {
    const queryInput = document.getElementById('sql-query');
    if (queryInput) {
        queryInput.value = queryText;
    }
}

// Theme management
function initializeTheme() {
    // Check for saved theme preference or default to light
    const savedTheme = localStorage.getItem('theme') || 'light';
    setTheme(savedTheme);
    
    // Add event listener to theme toggle button
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.addEventListener('click', toggleTheme);
    }
}

function setTheme(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
    
    // Update button icon
    const themeToggle = document.getElementById('theme-toggle');
    if (themeToggle) {
        themeToggle.textContent = theme === 'dark' ? '☀️' : '🌙';
        themeToggle.title = theme === 'dark' ? 'Switch to light mode' : 'Switch to dark mode';
    }
}

function toggleTheme() {
    const currentTheme = document.documentElement.getAttribute('data-theme') || 'light';
    const newTheme = currentTheme === 'dark' ? 'light' : 'dark';
    setTheme(newTheme);
}

// Initialize everything when DOM is ready
document.addEventListener('DOMContentLoaded', function() {
    // Initialize theme first
    initializeTheme();
    
    // Initialize map
    setTimeout(() => {
        initializeMap();
        initializeDuckDB();
    }, 500);
    
    // Bind event handlers
    const searchBtn = document.getElementById('search-button');
    if (searchBtn) {
        searchBtn.addEventListener('click', handleAddressSearch);
    }
    
    const addressInput = document.getElementById('address-search');
    if (addressInput) {
        // Handle input for autocomplete
        addressInput.addEventListener('input', (e) => {
            const query = e.target.value.trim();
            
            // Clear existing timeout
            if (autocompleteTimeout) {
                clearTimeout(autocompleteTimeout);
            }
            
            // Hide suggestions if query is too short
            if (query.length < 2) {
                hideSuggestions();
                return;
            }
            
            // Debounce the suggestions request
            autocompleteTimeout = setTimeout(async () => {
                const suggestions = await getSuggestions(query);
                showSuggestions(suggestions);
            }, 300); // 300ms delay
        });
        
        // Handle keyboard navigation
        addressInput.addEventListener('keydown', (e) => {
            const dropdown = document.getElementById('suggestions-dropdown');
            const isDropdownVisible = dropdown && dropdown.style.display === 'block';
            
            if (!isDropdownVisible) {
                if (e.key === 'Enter') {
                    e.preventDefault();
                    handleAddressSearch();
                }
                return;
            }
            
            switch (e.key) {
                case 'ArrowDown':
                    e.preventDefault();
                    selectedSuggestionIndex = Math.min(selectedSuggestionIndex + 1, currentSuggestions.length - 1);
                    updateSuggestionHighlight();
                    break;
                    
                case 'ArrowUp':
                    e.preventDefault();
                    selectedSuggestionIndex = Math.max(selectedSuggestionIndex - 1, -1);
                    updateSuggestionHighlight();
                    break;
                    
                case 'Enter':
                    e.preventDefault();
                    if (selectedSuggestionIndex >= 0) {
                        selectSuggestion(selectedSuggestionIndex);
                    } else {
                        hideSuggestions();
                        handleAddressSearch();
                    }
                    break;
                    
                case 'Escape':
                    e.preventDefault();
                    hideSuggestions();
                    break;
            }
        });
        
        // Hide suggestions when input loses focus (with delay for clicks)
        addressInput.addEventListener('blur', () => {
            setTimeout(() => {
                hideSuggestions();
            }, 150);
        });
    }
    
    const executeBtn = document.getElementById('execute-button');
    if (executeBtn) {
        executeBtn.addEventListener('click', executeQuery);
    }
    
    const downloadBtn = document.getElementById('download-csv-button');
    if (downloadBtn) {
        downloadBtn.addEventListener('click', downloadCSV);
    }
    
    // Bind example query buttons
    document.querySelectorAll('[data-query]').forEach(btn => {
        btn.addEventListener('click', () => {
            setExampleQuery(btn.dataset.query);
        });
    });
    
    // Hide suggestions when clicking outside
    document.addEventListener('click', (e) => {
        const addressInput = document.getElementById('address-search');
        const dropdown = document.getElementById('suggestions-dropdown');
        
        if (addressInput && dropdown && 
            !addressInput.contains(e.target) && 
            !dropdown.contains(e.target)) {
            hideSuggestions();
        }
    });
});