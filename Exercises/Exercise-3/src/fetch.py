import gzip
from pathlib import Path
import requests



def extract_file_uris(url: str) -> list[str]:
    """
    downloads and decode .gz file
    """
    response = requests.get(url)
    data_bytes = gzip.decompress(response.content)
    data = data_bytes.decode("utf-8")

    return data.split()


def download_and_print_s3_file(url: str, dir_path: str|Path) -> Path:
    """
    Download gz file store it and print its content in streaming.
    """
    response = requests.get(url, timeout=100)

    if isinstance(dir_path, str):
        dir_path = Path(dir_path)
    
    dir_path.mkdir(exist_ok=True)
    
    compress_file_path = dir_path / url.split('/')[-1]

    with compress_file_path.open("wb") as file:
        file.write(response.content)

    with gzip.open(compress_file_path, mode='rt') as file:
        for line in file:
            print(line)
    






