from pyspark.sql import DataFrame
import pyspark.sql.functions as sf

def clean_trip_duration(df: DataFrame):
    """
    - Change `tripduration` column from `str` datatype to `double`.
    - Also remove all duration greater than 24 hours.
    """
    df = df.withColumn("tripduration", sf.regexp_replace("tripduration", r",", ""))
    df = df.withColumn("tripduration", df.tripduration.astype("double"))
    return df.filter(sf.date_diff("end_time", "start_time") < 1)

def add_date_column(df: DataFrame):
    """
    Add `date` column extracted from column `start_time`.
    """
    df = df.withColumn("date", sf.to_date("start_time"))
    return df


def clean_pipeline(df: DataFrame):
    df = clean_trip_duration(df)
    df = add_date_column(df).persist()
    return df



# if __name__ == '__main__':
#     from pyspark.sql import SparkSession
#     try:
#         spark = SparkSession.builder.master("spark://developer:7077").getOrCreate()
#         df_q4 = spark.read.csv("data/Divvy_Trips_2019_Q4.csv", header=True, inferSchema=True)

#         df = clean_pipeline(df_q4)

#         df.printSchema()
#     except Exception as e:
#         print(e)
#     finally:
#         spark.stop()