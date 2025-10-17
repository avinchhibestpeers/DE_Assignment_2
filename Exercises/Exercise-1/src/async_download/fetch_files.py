import aiohttp
import asyncio
import aiofiles
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


async def fetch_file(
    url: str, dir_path: Path, session: aiohttp.ClientSession, *, timeout: float = 150.0
) -> Path:
    """
    - Fetch url and store in dir
    - Returns file path
    """
    file_name = url.split("/")[-1]
    file_path = dir_path / file_name

    # send request
    async with session.get(url, timeout=timeout) as response:
        if not response.ok:
            raise ConnectionError(
                f"Status: {response.status} \nReason: {response.reason} \nRequest info: {response.request_info}"
            )

        data = await response.content.read()

        async with aiofiles.open(file_path, mode="wb") as writer:
            await writer.write(data)

    return file_path


async def fetch_all_files(urls: list[str], dir_path: str | Path) -> list[Path]:
    """
    - Fetch files from urls.
    - Return their file path.
    """

    if isinstance(dir_path, str):
        dir_path = Path(dir_path)

    dir_path.mkdir(parents=True, exist_ok=True)

    async with aiohttp.ClientSession() as session:
        croutines = [fetch_file(url, dir_path, session) for url in urls]

        croutines_res = await asyncio.gather(*croutines, return_exceptions=True)

        file_paths = []
        # filter out fail fetch file requests
        for i, file_path in enumerate(croutines_res):
            if isinstance(file_path, TimeoutError):
                logger.warning(f"Time out Error for url: {urls[i]}")
            elif isinstance(file_path, Exception):
                logger.warning(f"Some error occured in url: {urls[i]} \n{file_path}")
            else:
                file_paths.append(file_path)

        return file_paths


# async def main():
#     download_uris = [
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
#         "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
#     ]

#     print(await fetch_all_files(download_uris, dir_path="downloads"))


# if __name__ == "__main__":
#     asyncio.run(main())

