# YTS Movies Data Pipeline

This repository contains a data pipeline for processing movie data from the YTS API. The pipeline is implemented using Databricks notebooks orchestrated by Azure Data Factory, with data stored in a three-tier architecture (Bronze, Silver, Gold) on Azure Data Lake Storage Gen2.

## Architecture Overview

The pipeline follows a modern data architecture approach:

1. **Data Extraction**: Fetches movie data from the YTS API using ThreadPool for parallel processing
2. **Bronze Layer**: Raw data storage in its original format 
3. **Silver Layer**: Cleaned and transformed data
4. **Gold Layer**: Dimensional model with fact tables and dimension tables
5. **Analytics**: Connect to Gold layer via Synapse Analytics SQL Serverless for analytics and Power BI visualization

## Architechture Diagram

![yts_azure_project (2)](https://github.com/user-attachments/assets/9665ce59-1db7-47f0-b2aa-273ff45b67a5)


The complete data flow follows these steps:
1. Fetch data from YTS API using ThreadPool for parallel API requests
2. Store raw data in Bronze container
3. Clean and transform data and store in Silver container
4. Create dimension and fact tables in Gold container
5. Connect to Gold layer via Synapse Analytics for queries and analytics

## Components

### Databricks Notebooks

The data processing logic is implemented in a series of Databricks notebooks:

1. **Initial Setup** (`initial_preps.py`)
   - Creates required catalogs and schemas
   - Sets up table structures for the dimensional model

2. **Data Ingestion** (`1_data_ingestion.py`)
   - Fetches movie data from YTS API
   - Uses ThreadPoolExecutor for parallel processing
   - Stores raw data in the Bronze layer

3. **Data Cleaning** (`2_data_cleaning.py`)
   - Handles missing values
   - Normalizes data formats
   - Outputs clean data to the Silver layer

4. **Dimension Tables** (Notebooks 3-9)
   - Creates and updates dimension tables:
     - `3_dim_genre.py` - Movie genres and genre junction table
     - `4_dim_movie.py` - Movie details
     - `5_dim_date.py` - Date dimension
     - `6_dim_language.py` - Language dimension
     - `7_dim_runtime.py` - Runtime dimension
     - `8_dim_rating.py` - Rating dimension
     - `9_dim_released_year.py` - Release year dimension

5. **Fact Table** (`10_fact_table.py`)
   - Creates the central fact table connecting all dimensions
   - Handles incremental updates

### Azure Resources

- **Azure Data Lake Storage Gen2**: Three containers for the different data layers
  - Bronze: Raw data storage
  - Silver: Cleaned data
  - Gold: Dimensional model

- **Azure Data Factory**: Orchestrates the pipeline execution
  - Schedules daily pipeline runs
  - Manages dependencies between notebooks
  - Tracks execution status and history

- **Azure Synapse Analytics**: SQL Serverless pool for querying data
  - Connects to Gold layer for analytics
  - Supports Power BI integration for dashboards

## Data Model

The dimensional model in the Gold layer follows a star schema design:

- **Fact Table**: `fact_movies` - Contains movie facts with keys to all dimensions
- **Primary Dimension Tables**:
  - `dim_movie` - Core movie details
  - `dim_genre` - Movie genres
  - `genre_junction` - Many-to-many relationship between movies and genres
- **Additional Dimension Tables**:
  - `dim_date` - Date-related attributes
  - `dim_language` - Language attributes
  - `dim_rating` - Rating bands
  - `dim_runtime` - Runtime categories
  - `dim_released_year` - Release years

## Incremental Loading

The pipeline supports incremental loading:
- Tracks the last processed date in a configuration file
- Only processes new movies on each run
- Updates dimension tables with new values
- Appends new records to the fact table

## Configuration

The pipeline uses configuration files to track state:
- `config.yaml` - Used by the local script (for development)
- `config.json` - Used in production for the Databricks notebooks

## Getting Started

### Prerequisites

- Azure subscription with access to:
  - Azure Data Lake Storage Gen2
  - Azure Databricks
  - Azure Data Factory
  - Azure Synapse Analytics

### Deployment Steps

1. Create Azure resources (Data Lake Storage, Databricks, Data Factory, Synapse)
2. Set up storage containers (Bronze, Silver, Gold)
3. Import Databricks notebooks to your workspace
4. Run `initial_preps.py` to set up database structure
5. Create a pipeline in Azure Data Factory to orchestrate the notebooks
6. Set up a trigger for daily execution

## Usage

The pipeline is scheduled to run daily via Azure Data Factory triggers. It will:
1. Check for new movies in the YTS API
2. Process only the new data
3. Update the dimensional model in the Gold layer
4. Make the latest data available for analytics via Synapse

For manual execution or development, you can run the notebooks in sequence through the Databricks interface.
