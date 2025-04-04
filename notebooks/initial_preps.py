# Databricks notebook source
# MAGIC %md
# MAGIC # Catalog creation

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG yts_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema yts_catalog.bronze;
# MAGIC create schema yts_catalog.silver;
# MAGIC create schema yts_catalog.gold;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating tables for dim_genre and genre_junction

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE yts_catalog.gold.dim_genre (
# MAGIC   genre_id INT,
# MAGIC   genre_name STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_genre';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE yts_catalog.gold.genre_junction (
# MAGIC   movie_id BIGINT,
# MAGIC   genre_id INT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/genre_junction/';
# MAGIC
# MAGIC
# MAGIC create or replace table yts_catalog.gold.dim_movie(
# MAGIC   movie_key bigint,
# MAGIC   slug STRING,
# MAGIC   url STRING,
# MAGIC   title STRING,
# MAGIC   description_full string,
# MAGIC   summary string,
# MAGIC   synopsis string,
# MAGIC   background_image_original string,
# MAGIC   large_cover_image string
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_movie';
# MAGIC
# MAGIC
# MAGIC CREATE or replace table yts_catalog.gold.dim_date (
# MAGIC   date_key INT NOT NULL,
# MAGIC   date TIMESTAMP NOT NULL,
# MAGIC   date_iso STRING,
# MAGIC   date_full STRING,
# MAGIC   day_of_week STRING,
# MAGIC   day_of_month INT,
# MAGIC   day_of_year INT,
# MAGIC   week_of_year INT,
# MAGIC   month INT,
# MAGIC   month_name STRING,
# MAGIC   quarter INT,
# MAGIC   year INT,
# MAGIC   is_weekend BOOLEAN
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_date';
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE or replace table yts_catalog.gold.dim_language (
# MAGIC   language_key BIGINT,
# MAGIC   language STRING
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_language';
# MAGIC
# MAGIC CREATE or replace table yts_catalog.gold.dim_runtime (
# MAGIC   runtime_key BIGINT,
# MAGIC   runtime BIGINT
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_runtime';
# MAGIC
# MAGIC CREATE or replace table yts_catalog.gold.dim_rating (
# MAGIC   rating_key BIGINT,
# MAGIC   rating DOUBLE
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_rating';
# MAGIC
# MAGIC CREATE or replace table yts_catalog.gold.dim_released_year (
# MAGIC   year_key BIGINT,
# MAGIC   year BIGINT
# MAGIC ) USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/dim_released_year';
# MAGIC
# MAGIC CREATE OR REPLACE TABLE yts_catalog.gold.fact_movies (
# MAGIC     movie_id BIGINT,
# MAGIC     imdb_code STRING,
# MAGIC     movie_key BIGINT,
# MAGIC     date_key INT,
# MAGIC     language_key BIGINT,
# MAGIC     runtime_key BIGINT,
# MAGIC     rating_key BIGINT,
# MAGIC     year_key BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC LOCATION 'abfss://gold@ytsstorageaccount.dfs.core.windows.net/fact_movies';
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists yts_catalog.gold.dim_movie;
# MAGIC drop table if exists yts_catalog.gold.dim_genre;
# MAGIC drop table if exists yts_catalog.gold.dim_date;
# MAGIC drop table if exists yts_catalog.gold.dim_language;
# MAGIC drop table if exists yts_catalog.gold.dim_runtime;
# MAGIC drop table if exists yts_catalog.gold.dim_rating;
# MAGIC drop table if exists yts_catalog.gold.dim_released_year;
# MAGIC drop table if exists yts_catalog.gold.genre_junction;
# MAGIC drop table if exists yts_catalog.gold.fact_movies;
# MAGIC drop table if exists yts_catalog.silver.cleaned_data;
