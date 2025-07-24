<script>

// Global variables
let map = null;
let marker = null;
let selectedCoordinates = null;

// Initialize map function
function initializeMap() {
    // Check if map container exists
    const mapContainer = document.getElementById('map');
    if (!mapContainer) {
        console.error('Map container with ID "map" not found');
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
        // Create the map instance
        map = L.map('map').setView([39.8283, -98.5795], 4);

        // Add tile layer (OpenStreetMap)
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '© OpenStreetMap contributors',
            maxZoom: 19
        }).addTo(map);

        // Add click event listener
        map.on('click', function(e) {
            const lat = e.latlng.lat.toFixed(6);
            const lng = e.latlng.lng.toFixed(6);
            updateLocation(lat, lng, `Location at ${lat}, ${lng}`);
        });

        // Position zoom controls
        map.zoomControl.setPosition('bottomright');

        console.log('Map initialized successfully');
        
    } catch (error) {
        console.error('Error initializing map:', error);
    }
}

// Update location function
function updateLocation(lat, lng, name) {
    selectedCoordinates = {lat: lat, lng: lng};

    // Remove existing marker
    if (marker) {
        map.removeLayer(marker);
    }

    // Add new marker
    marker = L.marker([lat, lng]).addTo(map);

    // Update UI elements
    const latDisplay = document.getElementById('display-lat');
    const lngDisplay = document.getElementById('display-lng');
    const nameInput = document.getElementById('location-name');
    const infoPanel = document.getElementById('location-info-panel');

    if (latDisplay) latDisplay.textContent = lat;
    if (lngDisplay) lngDisplay.textContent = lng;
    if (nameInput) nameInput.value = name;
    if (infoPanel) infoPanel.classList.add('show');

    // Center map on selected location
    map.setView([lat, lng], Math.max(map.getZoom(), 10));
}

// Search location function
async function searchLocation() {
    const searchInput = document.getElementById('search-location');
    const searchTerm = searchInput ? searchInput.value.trim() : '';
    
    if (!searchTerm) {
        alert('Please enter a location to search for');
        return;
    }

    try {
        const response = await fetch(`https://nominatim.openstreetmap.org/search?format=json&q=${encodeURIComponent(searchTerm)}&limit=1`);
        const data = await response.json();

        if (data && data.length > 0) {
            const result = data[0];
            const lat = parseFloat(result.lat);
            const lng = parseFloat(result.lon);
            const name = result.display_name;

            updateLocation(lat, lng, name);
        } else {
            alert('Location not found. Please try a different search term.');
        }
    } catch (error) {
        console.error('Error searching for location:', error);
        alert('Error searching for location. Please try again.');
    }
}

// Clear location function
function clearLocation() {
    // Remove marker
    if (marker && map) {
        map.removeLayer(marker);
        marker = null;
    }

    // Clear data
    selectedCoordinates = null;

    // Reset UI
    const latDisplay = document.getElementById('display-lat');
    const lngDisplay = document.getElementById('display-lng');
    const nameInput = document.getElementById('location-name');
    const searchInput = document.getElementById('search-location');
    const infoPanel = document.getElementById('location-info-panel');

    if (latDisplay) latDisplay.textContent = '-';
    if (lngDisplay) lngDisplay.textContent = '-';
    if (nameInput) nameInput.value = '';
    if (searchInput) searchInput.value = '';
    if (infoPanel) infoPanel.classList.remove('show');
}

// Handle search on Enter key
function handleSearchKeypress(event) {
    if (event.key === 'Enter') {
        searchLocation();
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

</script>