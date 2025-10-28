from datetime import datetime
import csv
import re
from pathlib import Path


class CSVExtractor:
    def __init__(self, file_path: str | Path):
        if isinstance(file_path, str):
            file_path = Path(file_path)

        self.file_path = file_path
        self.raw_data = []
        self.fieldnames = []

        self.read_csv()

    def read_csv(self):
        """
        Read csv file and store it data into obj attributes.
        """
        with self.file_path.open("r") as file:
            reader = csv.DictReader(file, skipinitialspace=True)
            self.fieldnames = reader.fieldnames

            for row in reader:
                self.raw_data.append(row)

    def csv_to_tuples(self) -> list[tuple]:
        """
        Clean csv data.
        """
        cleaned_data = []
        for row in self.raw_data:
            cleaned_row = self.clean_row(row, self.fieldnames)
            cleaned_data.append(cleaned_row)

        return cleaned_data

    def clean_row(self, raw_row: dict[str], field_order: list[str]) -> tuple:
        """
        Converter rows values in apropirate from eg data or int
        """
        clean_row = []
        for field in field_order:
            datapoint = raw_row[field]
            datapoint = datapoint.strip()

            # if datapoint empty
            if len(datapoint) == 0:
                datapoint = None
            # check of integer string
            elif datapoint.isnumeric():
                datapoint = int(datapoint)
            # check for date string
            elif len(datapoint) == 10 and re.match(r"^\d{4}.\d{2}.\d{2}$", datapoint):
                datapoint = datapoint.replace(datapoint[4], "-")
                datapoint = datetime.strptime(datapoint, "%Y-%m-%d")
                datapoint = datapoint.date()

            clean_row.append(datapoint)

        return tuple(clean_row)


# if __name__ == "__main__":
#     extractor = CSVExtractor('data/transactions.csv')
#     from pprint import pprint
#     print(extractor.fieldnames)
#     pprint(extractor.csv_to_tuples())
