from pathlib import Path
import logging

from src.async_download.fetch_files import fetch_all_files
from src.async_download.unzip_files import unzip_all_files
logger = logging.getLogger(__name__)


async def url_to_csv_pipeline(urls: list[str], dir_path: str|Path) -> list[str]:
    """
    For each url run url to csv pipeline.
    """
    if isinstance(dir_path, str):
        dir_path = Path(dir_path)

    # create dir if not exist
    dir_path.mkdir(parents=True, exist_ok=True)

    # pipeline
    file_paths = await fetch_all_files(urls, dir_path)
    file_paths = unzip_all_files(file_paths, dir_path)

    return file_paths


# if __name__ == "__main__":
#     download_uris = [
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
#     ]

#     asyncio.run(url_to_csv_pipeline(download_uris, "downloads"))
    