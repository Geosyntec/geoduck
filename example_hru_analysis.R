# Example: Run HRU Climate Data Analysis
# This script demonstrates how to use the hru_analysis.R functions

# Source the main analysis script
source("hru_analysis.R")

# Load configuration data
cat("Loading configuration data...\n")
config <- load_config_data()

cat("Available models:\n")
cat(paste(config$models, collapse = "\n"), "\n\n")

cat("Available grids:\n") 
cat(paste(config$grids, collapse = ", "), "\n\n")

cat("Available HRUs (first 10):\n")
print(head(config$hrus, 10))

# Define analysis parameters
selected_models <- c("WRF-NARR_HIS", "ccsm4_RCP85_PREC_6km", "access1.3_RCP85_PREC_6km")
selected_grid <- "R10C29"  # First grid in the list
selected_hrus <- data.frame(
  hru_name = c("hru000", "hru100"),
  area = c(1000000, 500000),  # areas in m² - example areas
  label = c("Outwash, Forest, Flat", "Till, Forest, Flat")
)

cat("\n=== Running Example Analysis ===\n")

# Example 1: Individual models, daily aggregation (limited data for testing)
cat("Example 1: Individual models, daily aggregation\n")
results_individual <- run_hru_analysis(
  selected_models = selected_models[1:2],  # Just first 2 models for faster testing
  selected_grid = selected_grid,
  selected_hrus = selected_hrus[1,],  # Just first HRU for testing
  aggregation = "monthly",  # Monthly for fewer data points
  chart_type = "individual",
  output_prefix = "test_individual"
)

# Example 2: Ensemble range, monthly aggregation  
cat("\nExample 2: Ensemble range, monthly aggregation\n")
results_ensemble <- run_hru_analysis(
  selected_models = selected_models[1:2],
  selected_grid = selected_grid,
  selected_hrus = selected_hrus[1,],
  aggregation = "monthly",
  chart_type = "range", 
  output_prefix = "test_ensemble"
)

cat("\n=== Analysis Examples Complete ===\n")
cat("Check the generated PNG files and CSV files for results.\n")