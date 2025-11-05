import os
from datetime import timedelta, datetime
from pathlib import Path
from pyspark.sql import DataFrame
import pyspark.sql.functions as sf
from pyspark.sql.window import Window


def q1(df: DataFrame, des_dir: Path):
    """
    What is the average trip duration per day?
    """
    res = (
        df.groupBy("date")
        .agg(sf.avg("tripduration").alias("avg_trip_duration"))
        .orderBy("date")
    )
    path = os.path.join(str(des_dir), "q1.csv")
    res.write.csv(path, header=True)
    return path


def q2(df: DataFrame, des_dir: Path):
    """
    How many trips were taken each day?
    """
    res = df.groupBy("date").agg(sf.count("*").alias("trip_count")).orderBy("date")

    path = os.path.join(str(des_dir), "q2.csv")
    res.write.csv(path, header=True)
    return path


def q3(df: DataFrame, des_dir: Path):
    """
    What was the most popular starting trip station for each month?
    """
    # groupby month, start trip station, count trips
    trip_cnt = df.groupBy(
        sf.date_format("date", "yyyy-MM").alias("month"),
        "from_station_id",
        "from_station_name",
    ).agg(sf.count("*").alias("trip_count"))
    # rank window paritition by month rank counts in desc order
    # filter rows with rank 1 and order by month
    trip_cnt_ranking = (
        trip_cnt.withColumn(
            "rank",
            sf.row_number().over(
                Window.orderBy(sf.desc("trip_count")).partitionBy("month")
            ),
        )
        .filter(sf.col("rank") == 1)
        .orderBy("month")
        .select("month", "from_station_id", "from_station_name")
    )

    path = os.path.join(str(des_dir), "q3.csv")
    trip_cnt_ranking.write.csv(path, header=True)
    return path


def q4(df: DataFrame, des_dir: Path):
    """
    What were the top 3 trip stations each day for the last two weeks?
    """
    last_date_df = df.select(sf.max("date").alias("last_date"))
    last_date = last_date_df.first()["last_date"]

    start_date = last_date - timedelta(weeks=2, days=-1)

    # filter data and groupby date and from_station, count values
    trip_count = (
        df.filter((df.date >= start_date) & (df.date <= last_date))
        .groupBy("date", "from_station_id", "from_station_name")
        .agg(sf.count("*").alias("trip_count"))
    )

    win = Window.orderBy(sf.desc("trip_count")).partitionBy("date")
    rank_trips = (
        trip_count.withColumn("rank", sf.row_number().over(win))
        .filter(sf.col("rank") <= 3)
        .orderBy("date", "rank")
    )

    path = os.path.join(str(des_dir), "q4.csv")
    rank_trips.write.csv(path, header=True)
    return path


def q5(df: DataFrame, des_dir: Path):
    """
    Do Males or Females take longer trips on average?
    """
    res = (
        df.groupBy("gender")
        .agg(sf.avg("tripduration").alias("avg_trip_duration"))
        .filter(df.gender.isNotNull())
    )

    path = os.path.join(str(des_dir), "q5.csv")
    res.write.csv(path, header=True)
    return path


def q6(df: DataFrame, des_dir: Path):
    """
    What is the top 10 ages of those that take the longest trips, and shortest?
    """
    curr_year = int(datetime.strftime(datetime.now(), "%Y"))
    trip_by_age = (
        df.filter(df.birthyear.isNotNull())
        .groupBy((curr_year - df.birthyear).alias("age"))
        .agg(sf.avg("tripduration").alias("avg_trip_duration"))
    ).persist()

    top_10_longest_trips = (
        trip_by_age.select("age", "avg_trip_duration")
        .orderBy(sf.desc("avg_trip_duration"), sf.asc("age"))
        .limit(10)
    )

    top_10_smallest_trips = (
        trip_by_age.select("age", "avg_trip_duration")
        .orderBy(sf.asc("avg_trip_duration"), sf.asc("age"))
        .limit(10)
    )
    longest_path = os.path.join(str(des_dir), "q6_longest.csv")
    smallest_path = os.path.join(str(des_dir), "q6_smallest.csv")
    top_10_longest_trips.write.csv(longest_path, header=True)
    top_10_smallest_trips.write.csv(smallest_path, header=True)

    return longest_path, smallest_path
