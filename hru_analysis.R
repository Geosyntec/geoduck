# HRU Climate Data Analysis - R Implementation
# Replicates functionality from hru_analysis.html

# Required packages
library(arrow)       # For reading Parquet files
library(dplyr)       # Data manipulation
library(jsonlite)    # Read JSON configuration files
library(lubridate)   # Date handling
library(ggplot2)     # Plotting
library(httr)        # HTTP requests
library(readr)       # CSV output
library(purrr)       # Functional programming
library(tidyr)       # Data reshaping

# Configuration and Data Loading Functions ----

#' Load configuration data from JSON files
load_config_data <- function() {
  cat("Loading configuration data...\n")
  
  # Load model names
  model_data <- fromJSON("model_names.json")
  model_names <- model_data$model_names
  
  # Load grid names
  grid_data <- fromJSON("grid_names.json")
  grid_names <- grid_data$grid_names
  
  # Load HRU data
  hru_data <- fromJSON("hrus.json")
  
  list(
    models = model_names,
    grids = grid_names, 
    hrus = hru_data
  )
}

#' Convert time index (ix) to datetime
#' @param ix Time index (hours since 1981-01-01)
#' @return POSIXct datetime
ix_to_datetime <- function(ix) {
  start_time <- as.POSIXct("1981-01-01 00:00:00", tz = "UTC")
  start_time + hours(ix)
}

#' Build Parquet URL for model/grid/HRU combination
#' @param model Model name
#' @param grid Grid name  
#' @param hru_name HRU name
#' @return URL string
build_parquet_url <- function(model, grid, hru_name) {
  paste0("https://storage.googleapis.com/climate_ts/", model, "/results/", grid, "/", hru_name, ".parquet")
}

#' Check if Parquet URL is accessible
#' @param url Parquet URL
#' @return logical
check_url_accessible <- function(url) {
  tryCatch({
    response <- HEAD(url)
    status_code(response) == 200
  }, error = function(e) FALSE)
}

# Data Loading and Processing Functions ----

#' Load data for a single model/HRU combination
#' @param model Model name
#' @param grid Grid name
#' @param hru_name HRU name
#' @param hru_area HRU area in m²
#' @return Data frame with processed flow data
load_model_hru_data <- function(model, grid, hru_name, hru_area) {
  url <- build_parquet_url(model, grid, hru_name)
  
  cat(sprintf("Loading data: %s -> %s/%s\n", model, grid, hru_name))
  
  tryCatch({
    # Check if URL is accessible first
    if (!check_url_accessible(url)) {
      warning(sprintf("URL not accessible: %s", url))
      return(NULL)
    }
    
    # Read Parquet file
    df <- read_parquet(url)
    
    # Check required columns exist
    required_cols <- c("ix", "suro", "ifwo")
    missing_cols <- setdiff(required_cols, names(df))
    if (length(missing_cols) > 0) {
      warning(sprintf("Missing columns in %s: %s", url, paste(missing_cols, collapse = ", ")))
      return(NULL)
    }
    
    # Process data
    df_processed <- df %>%
      filter(!is.na(ix), !is.na(suro), !is.na(ifwo)) %>%
      mutate(
        datetime = ix_to_datetime(ix),
        # Convert from mm to L/s: (mm * m²) / 3600
        flow_ls = (suro + ifwo) * hru_area / 3600,
        model_name = model,
        hru_name = hru_name,
        hru_area = hru_area
      ) %>%
      select(datetime, ix, flow_ls, model_name, hru_name, hru_area)
    
    return(df_processed)
    
  }, error = function(e) {
    warning(sprintf("Error loading %s: %s", url, e$message))
    return(NULL)
  })
}

#' Load data for multiple model/HRU combinations
#' @param models Vector of model names
#' @param grid Grid name
#' @param hru_config Data frame with hru_name and area columns
#' @return Combined data frame
load_multi_model_data <- function(models, grid, hru_config) {
  cat(sprintf("Loading data for %d models and %d HRUs...\n", length(models), nrow(hru_config)))
  
  all_data <- list()
  
  for (model in models) {
    for (i in 1:nrow(hru_config)) {
      hru_name <- hru_config$hru_name[i]
      hru_area <- hru_config$area[i]
      
      data <- load_model_hru_data(model, grid, hru_name, hru_area)
      if (!is.null(data)) {
        all_data[[paste(model, hru_name, sep = "_")]] <- data
      }
    }
  }
  
  if (length(all_data) == 0) {
    stop("No data loaded successfully")
  }
  
  # Combine all data
  combined_data <- bind_rows(all_data)
  
  cat(sprintf("Loaded %d total records\n", nrow(combined_data)))
  
  return(combined_data)
}

# Analysis Functions ----

#' Aggregate data by time period and model
#' @param data Data frame with datetime, flow_ls, model_name columns
#' @param aggregation One of "daily", "monthly", "yearly"
#' @return Aggregated data frame
aggregate_flow_data <- function(data, aggregation = "daily") {
  cat(sprintf("Aggregating data by %s periods...\n", aggregation))
  
  if (aggregation == "daily") {
    data_agg <- data %>%
      group_by(model_name, date = as.Date(datetime)) %>%
      summarise(
        total_flow = sum(flow_ls, na.rm = TRUE),
        avg_flow = mean(flow_ls, na.rm = TRUE),
        min_flow = min(flow_ls, na.rm = TRUE),
        max_flow = max(flow_ls, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(date, model_name)
      
  } else if (aggregation == "monthly") {
    data_agg <- data %>%
      group_by(model_name, date = floor_date(datetime, "month")) %>%
      summarise(
        total_flow = mean(flow_ls, na.rm = TRUE),  # Average for monthly (like HTML version)
        avg_flow = mean(flow_ls, na.rm = TRUE),
        min_flow = min(flow_ls, na.rm = TRUE),
        max_flow = max(flow_ls, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(date, model_name)
      
  } else if (aggregation == "yearly") {
    data_agg <- data %>%
      group_by(model_name, date = floor_date(datetime, "year")) %>%
      summarise(
        total_flow = mean(flow_ls, na.rm = TRUE),  # Average for yearly (like HTML version)  
        avg_flow = mean(flow_ls, na.rm = TRUE),
        min_flow = min(flow_ls, na.rm = TRUE),
        max_flow = max(flow_ls, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(date, model_name)
  } else {
    stop("Aggregation must be one of: daily, monthly, yearly")
  }
  
  return(data_agg)
}

#' Calculate ensemble statistics across models
#' @param data Aggregated data frame
#' @return Data frame with ensemble statistics
calculate_ensemble_stats <- function(data) {
  cat("Calculating ensemble statistics...\n")
  
  ensemble_stats <- data %>%
    group_by(date) %>%
    summarise(
      model_count = n(),
      ensemble_min = min(total_flow, na.rm = TRUE),
      ensemble_max = max(total_flow, na.rm = TRUE),
      ensemble_mean = mean(total_flow, na.rm = TRUE),
      ensemble_median = median(total_flow, na.rm = TRUE),
      .groups = "drop"
    )
  
  return(ensemble_stats)
}

# Visualization Functions ----

#' Create individual model time series plot
#' @param data Aggregated data frame
#' @param title Plot title
#' @return ggplot object
plot_individual_models <- function(data, title = "Multi-Model Comparison: Surface Runoff + Interflow") {
  
  # Truncate long model names for legend
  data_plot <- data %>%
    mutate(
      model_display = ifelse(nchar(model_name) > 25, 
                           paste0(substr(model_name, 1, 25), "..."), 
                           model_name)
    )
  
  p <- ggplot(data_plot, aes(x = date, y = total_flow, color = model_display)) +
    geom_line(size = 0.8, alpha = 0.8) +
    labs(
      title = title,
      x = "Date", 
      y = "Surface Runoff + Interflow (L/s)",
      color = "Climate Model"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, hjust = 0.5),
      legend.position = "bottom",
      legend.title = element_text(size = 10),
      legend.text = element_text(size = 8),
      axis.title = element_text(size = 11, face = "bold"),
      panel.grid.minor = element_blank()
    ) +
    guides(color = guide_legend(ncol = 3, override.aes = list(size = 1)))
  
  return(p)
}

#' Create ensemble range plot
#' @param data Aggregated data frame  
#' @param ensemble_stats Ensemble statistics
#' @param title Plot title
#' @return ggplot object
plot_ensemble_range <- function(data, ensemble_stats, title = "Model Ensemble Range: Surface Runoff + Interflow") {
  
  p <- ggplot(ensemble_stats, aes(x = date)) +
    geom_ribbon(aes(ymin = ensemble_min, ymax = ensemble_max), 
                alpha = 0.3, fill = "steelblue") +
    geom_line(aes(y = ensemble_mean), color = "red", size = 1.2) +
    geom_line(aes(y = ensemble_min), color = "steelblue", size = 0.8, linetype = "dashed") +
    geom_line(aes(y = ensemble_max), color = "steelblue", size = 0.8, linetype = "dashed") +
    labs(
      title = title,
      x = "Date",
      y = "Surface Runoff + Interflow (L/s)"
    ) +
    theme_minimal() +
    theme(
      plot.title = element_text(size = 14, hjust = 0.5),
      axis.title = element_text(size = 11, face = "bold"),
      panel.grid.minor = element_blank()
    ) +
    annotate("text", x = min(ensemble_stats$date), y = max(ensemble_stats$ensemble_max), 
             label = "Model Range", hjust = 0, vjust = 1, color = "steelblue", size = 3) +
    annotate("text", x = min(ensemble_stats$date), y = max(ensemble_stats$ensemble_mean), 
             label = "Ensemble Mean", hjust = 0, vjust = 1, color = "red", size = 3)
  
  return(p)
}

# Main Analysis Function ----

#' Run HRU climate data analysis
#' @param selected_models Vector of model names
#' @param selected_grid Grid name
#' @param selected_hrus Data frame with hru_name, area, and label columns  
#' @param aggregation Time aggregation level ("daily", "monthly", "yearly")
#' @param chart_type Chart type ("individual" or "range")
#' @param output_prefix Prefix for output files
#' @return List with processed data and plots
run_hru_analysis <- function(selected_models, selected_grid, selected_hrus, 
                           aggregation = "daily", chart_type = "individual",
                           output_prefix = "hru_analysis") {
  
  cat("=== HRU Climate Data Analysis ===\n")
  cat(sprintf("Models: %s\n", paste(selected_models, collapse = ", ")))
  cat(sprintf("Grid: %s\n", selected_grid))
  cat(sprintf("HRUs: %s\n", paste(selected_hrus$hru_name, collapse = ", ")))
  cat(sprintf("Total Area: %s m²\n", scales::comma(sum(selected_hrus$area))))
  cat(sprintf("Aggregation: %s\n", aggregation))
  cat(sprintf("Chart Type: %s\n", chart_type))
  cat("\n")
  
  # Load raw data
  raw_data <- load_multi_model_data(selected_models, selected_grid, selected_hrus)
  
  # Data summary
  date_range <- range(raw_data$datetime, na.rm = TRUE)
  cat("=== Data Summary ===\n")
  cat(sprintf("Total Records: %s\n", scales::comma(nrow(raw_data))))
  cat(sprintf("Date Range: %s to %s\n", 
              format(date_range[1], "%Y-%m-%d"), 
              format(date_range[2], "%Y-%m-%d")))
  cat(sprintf("Models in Data: %d\n", length(unique(raw_data$model_name))))
  cat(sprintf("HRUs in Data: %d\n", length(unique(raw_data$hru_name))))
  cat("\n")
  
  # Aggregate data
  agg_data <- aggregate_flow_data(raw_data, aggregation)
  
  # Calculate ensemble statistics
  ensemble_stats <- calculate_ensemble_stats(agg_data)
  
  # Create plots
  if (chart_type == "individual") {
    plot_title <- sprintf("Multi-Model Comparison: Surface Runoff + Interflow (%s aggregation)", aggregation)
    plot_obj <- plot_individual_models(agg_data, plot_title)
  } else {
    plot_title <- sprintf("Model Ensemble Range: Surface Runoff + Interflow (%s aggregation)", aggregation)
    plot_obj <- plot_ensemble_range(agg_data, ensemble_stats, plot_title)
  }
  
  # Save outputs
  cat("Saving outputs...\n")
  
  # Save plot
  plot_filename <- sprintf("%s_%s_%s.png", output_prefix, aggregation, chart_type)
  ggsave(plot_filename, plot_obj, width = 12, height = 8, dpi = 300)
  cat(sprintf("Plot saved: %s\n", plot_filename))
  
  # Save data
  csv_filename <- sprintf("%s_%s_data.csv", output_prefix, aggregation)
  write_csv(agg_data, csv_filename)
  cat(sprintf("Data saved: %s\n", csv_filename))
  
  # Save ensemble statistics
  if (chart_type == "range") {
    ensemble_filename <- sprintf("%s_%s_ensemble.csv", output_prefix, aggregation)
    write_csv(ensemble_stats, ensemble_filename)
    cat(sprintf("Ensemble stats saved: %s\n", ensemble_filename))
  }
  
  cat("Analysis complete!\n\n")
  
  return(list(
    raw_data = raw_data,
    aggregated_data = agg_data,
    ensemble_stats = ensemble_stats,
    plot = plot_obj
  ))
}

# Example Usage ----

if (FALSE) {
  # Load configuration data
  config <- load_config_data()
  
  # Define analysis parameters
  selected_models <- c("WRF-NARR_HIS", "ccsm4_RCP85_PREC_6km", "access1.3_RCP85_PREC_6km")
  selected_grid <- "R10C29"
  selected_hrus <- data.frame(
    hru_name = c("hru000", "hru100"),
    area = c(1000000, 500000),  # areas in m²
    label = c("Outwash, Forest, Flat", "Till, Forest, Flat")
  )
  
  # Run analysis - Individual models, daily aggregation
  results_daily_individual <- run_hru_analysis(
    selected_models = selected_models,
    selected_grid = selected_grid,
    selected_hrus = selected_hrus,
    aggregation = "daily",
    chart_type = "individual",
    output_prefix = "example_daily"
  )
  
  # Run analysis - Ensemble range, monthly aggregation  
  results_monthly_range <- run_hru_analysis(
    selected_models = selected_models,
    selected_grid = selected_grid,
    selected_hrus = selected_hrus,
    aggregation = "monthly", 
    chart_type = "range",
    output_prefix = "example_monthly"
  )
  
  # Display plots
  print(results_daily_individual$plot)
  print(results_monthly_range$plot)
}