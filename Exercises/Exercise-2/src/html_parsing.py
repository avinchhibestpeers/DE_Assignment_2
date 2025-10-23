from pathlib import Path
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

def extract_table_and_filter(html_file_path: str|Path, filter_datetime: datetime) -> list[str]:
    """
    Extract table out of html file and return name of files.
    """
    if isinstance(html_file_path, str):
        html_file_path = Path(html_file_path)
    
    # extract table from html
    with html_file_path.open("r") as file:
        table = pd.read_html(file, skiprows=[1,2])[0]
    
    table["Last modified"] = pd.to_datetime(table['Last modified'])

    # filter by timestamp
    filtered_rows = table[table['Last modified'] == filter_datetime]
    csv_file_names = filtered_rows['Name'].dropna().to_list()
    return csv_file_names
    


# if __name__ == "__main__":
#     extract_table_and_filter("index.html", datetime.strptime("2024-01-19 14:51", "%Y-%m-%d %H:%M"))