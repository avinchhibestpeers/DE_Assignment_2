import asyncio
import logging
from pathlib import Path

from src.async_download.pipeline import url_to_csv_pipeline

logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(name)s: %(message)s",
    datefmt="%d-%m-%Y %I:%M:%S %p",
    level=logging.INFO
)

logger = logging.getLogger(__name__)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]


def main():
    # your code here
    dir_path = Path("downloads")
    csv_file_paths = asyncio.run(url_to_csv_pipeline(download_uris, dir_path))

    logger.info(f"Following files downloaded successfully: \n{csv_file_paths}")

if __name__ == "__main__":
    main()
