from pathlib import Path
from pyspark.sql import SparkSession
from pprint import pprint

from src.analysis import q1, q2, q3, q4, q5, q6
from src.clean import clean_pipeline
from src.unzip import unzip_csv_file


def analysis_pipeline(zip_file_path: str|Path, spark: SparkSession):
    csv_file_path = unzip_csv_file(zip_file_path)
    df = spark.read.csv(str(csv_file_path), header=True, inferSchema=True)

    df = clean_pipeline(df)

    result_dir = Path("result")

    result_dir.mkdir(exist_ok=True)

    result_paths = []
    
    result_paths.append(q1(df, result_dir))
    result_paths.append(q2(df, result_dir))
    result_paths.append(q4(df, result_dir))
    result_paths.append(q5(df, result_dir))
    result_paths.append(q6(df, result_dir))

    print("here are all result paths.")
    pprint(result_paths)

