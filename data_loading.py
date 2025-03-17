import requests
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
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


def fetch_movies(config_path="config.yaml"):
    """Main function to fetch movies using configuration parameters."""
    # Load configuration
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)

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
            logging.info(f"Thread created for page: {page}")

        # Process completed futures
        for future in as_completed(futures):
            page_num = futures[future]
            try:
                movies = future.result()
                if movies:
                    any_movies_found = True
                    logging.info(f"Page {page_num}: {len(movies)} movies retrieved")
                    # Process data here (e.g., save to DB/file)
                else:
                    logging.info(f"Page {page_num}: No movies found")
            except Exception as e:
                logging.error(f"Page {page_num} failed: {str(e)}")
                break

    # Update configuration
    if any_movies_found:
        config["last_page_processed"] = max_page
        logging.info(f"Updated last processed page to {max_page}")
    else:
        logging.info("No new movies found; configuration not updated.")

    # Save updated configuration
    with open(config_path, "w") as file:
        yaml.safe_dump(config, file, default_flow_style=False)


if __name__ == "__main__":
    fetch_movies()
