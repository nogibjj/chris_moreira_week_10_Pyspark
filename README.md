# PySpark Operations
Data Transformation with Pyspark
```python
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
```

Query Operation ran on Data
```sql
SELECT released_year, COUNT(DISTINCT track_name) AS 
unique_tracks FROM SpotifyData GROUP BY released_year 
ORDER BY released_year
```

# Project Goal
In this project, we set up a PySpark environment to process and analyze a large dataset containing information on Spotify's most-streamed songs. The primary goal is to leverage PySpark’s data processing capabilities to perform efficient transformations and analyses on the dataset, which includes fields such as track name, artist, release date, streams, and several musical attributes. The project includes loading the dataset, executing a Spark SQL query to extract insights (e.g., counting unique tracks by release year), and performing a data transformation that categorizes songs by popularity based on stream counts. Through these steps, we aim to demonstrate PySpark’s ability to handle large datasets, run SQL queries, and apply transformations that yield meaningful insights, all while storing processed outputs as CSV files for further use.

# Project Schema

# Results Snapshot