from pyspark.sql.functions import col, year, month, dayofmonth


def transform(data_path="data/Spotify_Most_Streamed_Songs.csv", spark=None):
    if spark is None:
        raise ValueError("A Spark session must be provided.")

    df = spark.read.csv(data_path, header=True, inferSchema=True)

    df = (
        df.withColumnRenamed("Track Name", "track_name")
        .withColumnRenamed("Artist Name", "artist_name")
        .withColumnRenamed("Released Date", "released_date")
    )

    if "released_date" in df.columns:
        df = (
            df.withColumn("released_year", year(col("released_date")))
            .withColumn("released_month", month(col("released_date")))
            .withColumn("released_day", dayofmonth(col("released_date")))
        )

    df = df.dropna().withColumn("streams_millions", col("streams") / 1_000_000)
    return df


def load(df, output_path="output/Spotify_Transformed.parquet", file_format="parquet"):
    if file_format == "parquet":
        df.write.mode("overwrite").parquet(output_path)
    elif file_format == "csv":
        df.write.mode("overwrite").option("header", True).csv(output_path)
    elif file_format == "json":
        df.write.mode("overwrite").json(output_path)
    else:
        raise ValueError("Unsupported file format. Choose 'parquet', 'csv', or 'json'.")

    return f"Data saved to {output_path} in {file_format} format."
