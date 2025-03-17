import requests
import yaml
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


def get_total_pages(movies_per_page=50, order_by="asc", yts_url="", page=1) -> int:
    url = yts_url + f"?page={page}&order_by={order_by}&limit={movies_per_page}"
    response = requests.get(url=url)
    response.raise_for_status()
    total_movies = response.json()["data"]["movie_count"]
    return (total_movies + movies_per_page - 1) // movies_per_page


def get_data(page: int, movies_per_page=50, order_by="asc", yts_url="") -> list:
    url = yts_url + f"?page={page}&order_by={order_by}&limit={movies_per_page}"

    response = requests.get(url)
    response.raise_for_status()  # Raise HTTP errors
    data = response.json()
    return data["data"]["movies"]


if __name__ == "__main__":

    with open("params.yaml", "r") as file:
        config = yaml.safe_load(file)

    # url params
    YTS_URL = config["yts_url"]
    MOVIES_PER_PAGE = config["limit"]
    ORDER_BY = config["order_by"]
    MAX_WORKERS = config["max_workers"]
    # incremental params
    initial_page = config["initial_page"]
    current_page = config["current_page"]

    start_page = get_total_pages(
        movies_per_page=MOVIES_PER_PAGE, order_by=ORDER_BY, yts_url=YTS_URL
    )
    if current_page < start_page:
        current_page = start_page

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {
            executor.submit(
                get_data,
                page,
                MOVIES_PER_PAGE,
                ORDER_BY,
                YTS_URL,
            ): page
            for page in range(initial_page, current_page + 1)
        }
        for page in range(initial_page, current_page + 1):
            logging.info(f"Thread created for page: {page}")

        for future in as_completed(futures):
            page_num = futures[future]
            try:
                movies = future.result()
                logging.info(f"Page {page_num}: {len(movies)} movies retrieved")
                # Process data here (e.g., save to DB/file)
            except Exception as e:
                logging.error(f"Page {page_num} failed: {str(e)}")

    print(len(futures))
    config["initial_page"] = current_page + 1
    config["current_page"] = current_page + 1
    with open("params.yaml", "w") as file:
        yaml.safe_dump(config, file, default_flow_style=False)
