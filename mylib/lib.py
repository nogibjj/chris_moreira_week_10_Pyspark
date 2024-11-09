import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType
)

OUTPUT_DIR = "output"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

def start_spark(app_name="SpotifyApp"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def end_spark(spark):
    spark.stop()

def extract(url, file_path, directory="data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def load_data(spark, data="data/Spotify_Most_Streamed_Songs.csv"):
    schema = StructType([
        StructField("track_name", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_count", IntegerType(), True),
        StructField("released_year", IntegerType(), True),
        StructField("released_month", IntegerType(), True),
        StructField("released_day", IntegerType(), True),
        StructField("in_spotify_playlists", IntegerType(), True),
        StructField("in_spotify_charts", IntegerType(), True),
        StructField("streams", IntegerType(), True),
        StructField("in_apple_playlists", IntegerType(), True),
        StructField("key", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("danceability_percent", IntegerType(), True),
        StructField("valence_percent", IntegerType(), True),
        StructField("energy_percent", IntegerType(), True),
        StructField("acousticness_percent", IntegerType(), True),
        StructField("instrumentalness_percent", IntegerType(), True),
        StructField("liveness_percent", IntegerType(), True),
        StructField("speechiness_percent", IntegerType(), True),
        StructField("cover_url", StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv(data)
    df.limit(10).toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "load_data_output.csv"), index=False)
    return df

def query(spark, df, query_text, view_name="SpotifyData"):
    df.createOrReplaceTempView(view_name)
    result_df = spark.sql(query_text)
    result_df.limit(10).toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "query_output.csv"), index=False)
    return result_df

def describe(df):
    summary_df = df.describe()
    summary_df.limit(10).toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "describe_output.csv"), index=False)
    return summary_df

def example_transform(df):
    df = df.withColumn(
        "Popularity_Category",
        F.when(F.col("streams") > 1000000000, "Ultra Popular")
         .when((F.col("streams") > 500000000) &
               (F.col("streams") <= 1000000000),
               "Very Popular")
         .when((F.col("streams") > 100000000) &
               (F.col("streams") <= 500000000),
               "Popular")
         .otherwise("Less Popular")
    )
    df.limit(10).toPandas().to_csv(
        os.path.join(OUTPUT_DIR, "transform_output.csv"), index=False)
    return df
