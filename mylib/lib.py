import os
import requests
import pandas as pd
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
    return "Stopped Spark session"

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
        StructField("artist(s)_name", StringType(), True),
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
        StructField("danceability_%", IntegerType(), True),
        StructField("valence_%", IntegerType(), True),
        StructField("energy_%", IntegerType(), True),
        StructField("acousticness_%", IntegerType(), True),
        StructField("instrumentalness_%", IntegerType(), True),
        StructField("liveness_%", IntegerType(), True),
        StructField("speechiness_%", IntegerType(), True),
        StructField("cover_url", StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv(data)
    df = df.withColumnRenamed("artist(s)_name", "artist_name") \
           .withColumnRenamed("danceability_%", "danceability_percent") \
           .withColumnRenamed("valence_%", "valence_percent") \
           .withColumnRenamed("energy_%", "energy_percent") \
           .withColumnRenamed("acousticness_%", "acousticness_percent") \
           .withColumnRenamed("instrumentalness_%", "instrumentalness_percent") \
           .withColumnRenamed("liveness_%", "liveness_percent") \
           .withColumnRenamed("speechiness_%", "speechiness_percent")
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
