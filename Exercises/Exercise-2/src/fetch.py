import os
from pathlib import Path
import requests
import threading
from concurrent.futures import ThreadPoolExecutor

# local local of thread
thread_local = threading.local()


def fetch_html_file(url: str, file_path: str | Path):
    """
    Fetch html file from url
    Args:
        url(str)
        file_path(str|Path) File path to store html file
    Returns:
        None
    """

    if isinstance(file_path, str):
        file_path = Path(file_path)

    response = requests.get(url)

    with file_path.open("wb") as file:
        file.write(response.content)


def get_session_for_thread():
    """
    Get or create request.session for thread
    """
    if not hasattr(thread_local, "session"):
        thread_local.session = requests.session()
    return thread_local.session


def download_csv_file(url: str, dir_path: Path) -> Path:
    """
    Download and store single csv file.
    """
    session = get_session_for_thread()

    file_path = dir_path / url.split("/")[-1]
    response = session.get(url)

    if not response.ok:
        raise ConnectionError(f"Status: {response.status_code} Reason: {response.reason} Header: {response.headers}")
    with file_path.open("wb") as file:
        file.write(response.content)

    return file_path


def download_csv_files(base_url: str, csv_file_names: list[str], dir_path: str | Path):
    """
    Multithreading download files.
    """
    if isinstance(dir_path, str):
        dir_path = Path(dir_path)
    
    # make dir if not exist
    dir_path.mkdir(exist_ok=True)

    urls = [os.path.join(base_url, file_name) for file_name in csv_file_names]

    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = []
        for url in urls:
            future = executor.submit(download_csv_file, url, dir_path)
            futures.append(future)
        
    file_paths = []
    for future in futures:
        try:
            file_path = future.result()
            file_paths.append(file_path)
        except Exception as e:
            print(f"ConnectionError: \n{e}") 

            
    return file_paths


# if __name__ == "__main__":
#     # Test your code here..
#     fetch_html_file("https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/", "index.html")

#     filenames = [
#         "94707099999.csv",
#         "94846099999.cs",
#         "94998099999.csv",
#         "95602099999.csv",
#         "95957099999.csv",
#         "96481099999.csv",
#         "96791099999.csv",
#         "97300099999.csv",
#         "97530099999.csv",
#         "97686099999.csv",
#         "99999904236.csv",
#         "99999963895.csv",
#         "99999964756.csv",
#         "99999994078.csv",
#     ]

#     base_url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
#     paths = download_csv_files(base_url, filenames, "downloads")
