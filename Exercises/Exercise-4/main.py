from pprint import pprint

from src.json_to_csv import json_to_csv
from src.search_json import search_json_files

def main():
    data_dir = "data"
    des_dir = "downloads"

    json_file_paths = search_json_files(data_dir)
    csv_file_paths = json_to_csv(json_file_paths, des_dir)

    pprint(csv_file_paths)


if __name__ == "__main__":
    main()
