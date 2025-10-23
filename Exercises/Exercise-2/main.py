from datetime import datetime
from pprint import pprint


from src.analytics import max_hourly_bulb_temp_all
from src.fetch import fetch_html_file, download_csv_files
from src.html_parsing import extract_table_and_filter
def main():
    url = "https://www.ncei.noaa.gov/data/local-climatological-data/access/2021/"
    html_file_path = "index.html"
    csv_downloads = "downloads"

    # pipeline
    fetch_html_file(url, html_file_path)
    csv_filenames = extract_table_and_filter(html_file_path, datetime.strptime("2024-01-19 14:51", "%Y-%m-%d %H:%M"))
    csv_file_paths = download_csv_files(url, csv_filenames, dir_path=csv_downloads)
    values = max_hourly_bulb_temp_all(csv_file_paths) 
    pprint(values)

    


if __name__ == "__main__":
    main()
