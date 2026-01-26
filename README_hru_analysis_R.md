# HRU Climate Data Analysis - R Implementation

This R script (`hru_analysis.R`) replicates the functionality of the browser-based HRU analysis tool (`hru_analysis.html`) for analyzing climate data from multiple models and Hydrologic Response Units (HRUs).

## Features

- **Multi-model analysis**: Compare data across different climate models
- **HRU-based analysis**: Analyze multiple HRU types with area weighting
- **Temporal aggregation**: Daily, monthly, and yearly aggregations
- **Flow calculations**: Converts surface runoff + interflow from mm to L/s
- **Ensemble statistics**: Calculate min, max, and mean across models
- **Visualization**: Individual model plots or ensemble range plots
- **Data export**: Save results as CSV and high-resolution PNG plots

## Required R Packages

```r
install.packages(c("arrow", "dplyr", "jsonlite", "lubridate", 
                   "ggplot2", "httr", "readr", "purrr", "tidyr", "scales"))
```

## Data Sources

The script reads configuration from JSON files:
- `model_names.json`: Available climate models
- `grid_names.json`: Available grid locations  
- `hrus.json`: HRU definitions with labels

Climate data is loaded from Google Cloud Storage Parquet files at:
`https://storage.googleapis.com/climate_ts/{model}/results/{grid}/{hru_name}.parquet`

## Usage

### Basic Example

```r
# Load the script
source("hru_analysis.R")

# Load configuration data
config <- load_config_data()

# Define analysis parameters
selected_models <- c("WRF-NARR_HIS", "ccsm4_RCP85_PREC_6km") 
selected_grid <- "R10C29"
selected_hrus <- data.frame(
  hru_name = c("hru000", "hru100"),
  area = c(1000000, 500000),  # areas in m²
  label = c("Outwash, Forest, Flat", "Till, Forest, Flat")
)

# Run analysis
results <- run_hru_analysis(
  selected_models = selected_models,
  selected_grid = selected_grid, 
  selected_hrus = selected_hrus,
  aggregation = "monthly",     # "daily", "monthly", or "yearly"
  chart_type = "individual",   # "individual" or "range" 
  output_prefix = "my_analysis"
)
```

### Quick Test

Run the example script to test functionality:

```r
source("example_hru_analysis.R")
```

## Function Reference

### Core Functions

- `load_config_data()`: Load model, grid, and HRU configurations
- `run_hru_analysis()`: Main analysis function
- `load_multi_model_data()`: Load and combine Parquet data
- `aggregate_flow_data()`: Temporal aggregation
- `calculate_ensemble_stats()`: Cross-model statistics
- `plot_individual_models()`: Individual model visualization  
- `plot_ensemble_range()`: Ensemble range visualization

### Parameters

**aggregation options:**
- `"daily"`: Daily sums
- `"monthly"`: Monthly averages
- `"yearly"`: Yearly averages

**chart_type options:**
- `"individual"`: Show each model as separate line
- `"range"`: Show ensemble min/max range with mean

## Output Files

The analysis generates:
- `{prefix}_{aggregation}_{chart_type}.png`: High-resolution plot
- `{prefix}_{aggregation}_data.csv`: Aggregated time series data
- `{prefix}_{aggregation}_ensemble.csv`: Ensemble statistics (for range plots)

## Flow Calculation

Surface runoff + interflow is converted from mm to L/s using:

```
flow_ls = (suro + ifwo) * area_m² / 3600
```

Where:
- `suro`: Surface runoff (mm)
- `ifwo`: Interflow (mm)  
- `area_m²`: HRU area in square meters
- Division by 3600 converts mm·m²/hour to L/s

## Time Conversion

The `ix` time index (hours since 1981-01-01 00:00:00 UTC) is converted to datetime using:

```r
ix_to_datetime <- function(ix) {
  start_time <- as.POSIXct("1981-01-01 00:00:00", tz = "UTC")
  start_time + hours(ix)
}
```

## Error Handling

The script includes robust error handling for:
- Missing or inaccessible Parquet files
- Missing required columns (suro, ifwo, ix)
- Network connectivity issues
- Invalid parameter combinations

## Comparison to HTML Version

This R implementation provides equivalent functionality to `hru_analysis.html`:

| Feature | HTML Version | R Version |
|---------|-------------|-----------|
| Data source | DuckDB WASM | arrow package |
| Multi-model | ✓ | ✓ |
| HRU weighting | ✓ | ✓ |
| Flow conversion | ✓ | ✓ |
| Time aggregation | ✓ | ✓ |
| Ensemble stats | ✓ | ✓ |
| Interactive plots | ✓ | Static (ggplot2) |
| Data export | CSV | CSV + PNG |
| Web interface | ✓ | Command line |