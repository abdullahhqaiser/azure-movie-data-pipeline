# Databricks notebook source
# MAGIC %md
# MAGIC ## Imports goes here

# COMMAND ----------

import requests
import sys
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema Definition

# COMMAND ----------

from pyspark.sql.types import *

schema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("url", StringType(), nullable=True),
    StructField("imdb_code", StringType(), nullable=True),
    StructField("title", StringType(), nullable=True),
    StructField("title_english", StringType(), nullable=True),
    StructField("title_long", StringType(), nullable=True),
    StructField("slug", StringType(), nullable=True),
    StructField("year", IntegerType(), nullable=True),
    StructField("rating", DoubleType(), nullable=True),
    StructField("runtime", IntegerType(), nullable=True),
    StructField("genres", ArrayType(StringType()), nullable=True),
    StructField("summary", StringType(), nullable=True),
    StructField("description_full", StringType(), nullable=True),
    StructField("synopsis", StringType(), nullable=True),
    StructField("yt_trailer_code", StringType(), nullable=True),
    StructField("language", StringType(), nullable=True),
    StructField("mpa_rating", StringType(), nullable=True),
    StructField("background_image", StringType(), nullable=True),
    StructField("background_image_original", StringType(), nullable=True),
    StructField("small_cover_image", StringType(), nullable=True),
    StructField("medium_cover_image", StringType(), nullable=True),
    StructField("large_cover_image", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("torrents", ArrayType(StructType([
        StructField("url", StringType(), nullable=True),
        StructField("hash", StringType(), nullable=True),
        StructField("quality", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("is_repack", StringType(), nullable=True),
        StructField("video_codec", StringType(), nullable=True),
        StructField("bit_depth", StringType(), nullable=True),
        StructField("audio_channels", StringType(), nullable=True),
        StructField("seeds", IntegerType(), nullable=True),
        StructField("peers", IntegerType(), nullable=True),
        StructField("size", StringType(), nullable=True),
        StructField("size_bytes", LongType(), nullable=True),
        StructField("date_uploaded", StringType(), nullable=True),
        StructField("date_uploaded_unix", LongType(), nullable=True)
    ])), nullable=True),
    StructField("date_uploaded", StringType(), nullable=True),
    StructField("date_uploaded_unix", LongType(), nullable=True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data ingestion script - ThreadPools

# COMMAND ----------

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_total_pages(limit, order_by, yts_url) -> int:
    """Calculate total available pages based on movie count."""
    url = f"{yts_url}?page=1&order_by={order_by}&limit={limit}"
    response = requests.get(url=url)
    response.raise_for_status()
    total_movies = response.json()["data"]["movie_count"]
    return (total_movies + limit - 1) // limit


def get_data(page, limit, order_by, yts_url) -> list:
    """Fetch movie data for a specific page."""
    url = f"{yts_url}?page={page}&order_by={order_by}&limit={limit}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    return data["data"]["movies"]


def fetch_movies(config_path="config.json"):
    """Main function to fetch movies using configuration parameters."""
    # Load configuration
    file_contents = dbutils.fs.head(config_path)
    config = json.loads(file_contents)

    # Extract parameters from config
    yts_url = config["yts_url"]
    limit = config["limit"]
    order_by = config["order_by"]
    max_workers = config["max_workers"]
    last_page_processed = config.get("last_page_processed", 0)

    # Get total pages available
    max_page = get_total_pages(limit, order_by, yts_url)
    logging.info(f"Total pages available: {max_page}")

    # No new pages to process
    if last_page_processed >= max_page:
        logging.info("No new pages to process.")
        return

    # Process new pages
    any_movies_found = False
    all_movies = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Start from the next unprocessed page
        pages_to_process = range(last_page_processed + 1, max_page + 1)

        # Create futures dictionary
        futures = {
            executor.submit(get_data, page, limit, order_by, yts_url): page
            for page in pages_to_process
        }

        # Log thread creation
        for page in pages_to_process:
            logging.info(f"Thread created for page {page}")

        # Process completed futures
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                movies = future.result()
                if movies:
                    any_movies_found = True
                    all_movies.extend(movies)
                    logging.info(f"Page {page_num}: {len(movies)} movies retrieved")
                else:
                    logging.info(f"Page {page_num}: No movies found")
            except Exception as e:
                logging.error(f"Page {page_num} failed: {str(e)}")
                break

        # save movies list as json
      
        # convert rating to float
        all_movies = [
                {**movie, 'rating': float(movie['rating'])} for movie in all_movies
        ]
        df = spark.createDataFrame(all_movies)
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("path", "abfss://bronze@ytsstorageaccount.dfs.core.windows.net/raw_data")\
            .saveAsTable("yts_catalog.bronze.raw_data")


    # Update configuration
    if any_movies_found:
        config["last_page_processed"] = max_page
        logging.info(f"Updated last processed page to {max_page}")
    else:
        logging.info("No new movies found; configuration not updated.")

    # Save updated configuration
    config_str = json.dumps(config, indent=4)
    dbutils.fs.put(config_path, config_str, overwrite=True)
    logging.info("Configuration saved.")

path = "abfss://configs@ytsstorageaccount.dfs.core.windows.net/config.json"
fetch_movies(config_path=path)
