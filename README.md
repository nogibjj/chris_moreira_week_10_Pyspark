[![CI](https://github.com/nogibjj/chris_moreira_week_10_Pyspark/actions/workflows/cicd.yml/badge.svg)](https://github.com/nogibjj/chris_moreira_week_10_Pyspark/actions/workflows/cicd.yml)


# Project Goal
In this project, we set up a PySpark environment to process and analyze a large dataset containing information on Spotify's most-streamed songs. The primary goal is to leverage PySpark’s data processing capabilities to perform efficient transformations and analyses on the dataset, which includes fields such as track name, artist, release date, streams, and several musical attributes. The project includes loading the dataset, executing a Spark SQL query to extract insights (e.g., counting unique tracks by release year), and performing a data transformation that categorizes songs by popularity based on stream counts. Through these steps, we aim to demonstrate PySpark’s ability to handle large datasets, run SQL queries, and apply transformations that yield meaningful insights, all while storing processed outputs as CSV files for further use.

# Pyspark Functions running in the following order:
- start_spark: Initiates PySpark
- end_spark: Terminates PySpark
- extract: Retrieves dataset form Github and adds raw data into the data folder in this rpeository. 
- load_data: structures a data schema in PySpark
- query: runs a SQL query operation in the raw data(see below for operation details)
- describe: produces a data summary each attribute of the raw dataset.
- example_transform: generates column for Music Popularity to the data set based on stream count (see below for details)


# Summary of Pyspark Operations
Data Transformation with Pyspark
```PySpark
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
# Project Schema
![image](https://github.com/user-attachments/assets/65f0f13e-7555-4fb3-b20b-b17f8ef84e67)

# Results Snapshot
# Describe Result
![image](https://github.com/user-attachments/assets/e45dad13-3410-4fc6-b32b-1b32e94dccf1)

# Load Data Result
![image](https://github.com/user-attachments/assets/bdb6ed6e-17bd-4582-9820-84ab14ab80d2)

# Loaded Data Preview
![image](https://github.com/user-attachments/assets/bee05666-c3a1-46af-acec-73f34adc2972)

# Query Result
![image](https://github.com/user-attachments/assets/02c22086-5d42-47bc-8484-fe29697e9fad)

# Transform Operation Output
![image](https://github.com/user-attachments/assets/7259513f-5577-454e-9f41-cf62eea04969)
