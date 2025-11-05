from pyspark.sql import SparkSession

from src.pipeline import analysis_pipeline


def main():
    spark = SparkSession.builder.master("spark://developer:7077").appName("Exercise6").getOrCreate()

    zip_file_path = "data/Divvy_Trips_2019_Q4.zip"
    try:
        analysis_pipeline(zip_file_path, spark)
    except Exception as e:
        print("Exception: ", e)
    finally:
        spark.stop()
    



if __name__ == "__main__":
    main()
