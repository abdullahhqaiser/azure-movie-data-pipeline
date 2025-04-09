# YTS Movies Data Pipeline

This repository contains a data pipeline for loading and processing movie data from the YTS API. The pipeline uses ThreadPool for efficient data fetching and creates a dimensional data model in Databricks.

## Overview

The pipeline follows these steps:
1. Fetch data from YTS API using ThreadPool for parallel processing
2. Clean and transform raw data
3. Create dimensional tables and fact table for analytics
4. Support incremental loading to process only new data

## Threadpools Working

![ChatGPT Image Apr 10, 2025, 01_46_39 AM](https://github.com/user-attachments/assets/a7b41796-576e-46c7-95eb-4c165cc79eb2)


*This diagram shows the complete data flow from YTS API through Bronze, Silver, and Gold layers.*

## Components

### Local Data Loading Script

`data_loading.py` contains the standalone script for fetching data from the YTS API:

- Uses ThreadPoolExecutor for parallel API requests
- Tracks progress using a config file
- Handles errors and retries
- Updates configuration after successful execution

### Databricks Notebooks

The Databricks notebooks handle the ETL process in a sequential pipeline:

1. **Data Ingestion** (`1_data_ingestion.py`)
   - Fetches movie data from YTS API
   - Uses parallel processing with ThreadPool
   - Stores raw data in the Bronze layer

2. **Data Cleaning** (`2_data_cleaning.py`)
   - Handles missing values
   - Normalizes data formats
   - Outputs clean data to the Silver layer

3. **Dimension Tables** (Notebooks 3-9)
   - Creates and updates dimension tables:
     - `dim_genre.py` - Movie genres
     - `dim_movie.py` - Movie details
     - `dim_date.py` - Date dimension
     - `dim_language.py` - Language dimension
     - `dim_runtime.py` - Runtime dimension
     - `dim_rating.py` - Rating dimension
     - `dim_released_year.py` - Release year dimension

4. **Fact Table** (`10_fact_table.py`)
   - Creates the central fact table connecting all dimensions
   - Handles incremental updates

5. **Initial Setup** (`initial_preps.py`)
   - Creates catalogs and schemas
   - Sets up table structures

## Configuration

The pipeline uses two configuration files:

1. **config.yaml** (local)
   - Tracks the last processed page
   - Sets API request parameters
   - Configures parallel processing

2. **config.json** (Databricks)
   - Tracks the last processed date for incremental loads
   - Stores API connection details

## Data Model

The pipeline creates a star schema with:
- A central fact table (`fact_movies`)
- Multiple dimension tables for analytics
- A junction table for many-to-many relationship (movie-genre)

## Usage

### Local Data Fetching

```python
# Run the local data loading script
python data_loading.py
```

### Databricks Pipeline

1. Run the notebooks in sequence (1-10)
2. For first-time setup, run `initial_preps.py` to create the database structure

## Incremental Processing

The pipeline supports incremental processing:
- Tracks the last processed date
- Only processes new movies on each run
- Updates dimension tables with new values
- Appends new records to the fact table

The pipeline is designed to be run on a schedule, processing only new data since the last run.
