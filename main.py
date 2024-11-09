from mylib.extract import extract
from mylib.transform_load import transform, load
from mylib.query import SpotifyDataFrameManager  # Import the class
from pyspark.sql import SparkSession


def main():
    # Start a Spark session
    spark = SparkSession.builder.appName("SpotifyETL").getOrCreate()

    # Step 1: Extract the CSV file
    extract_path = extract()

    # Step 2: Transform the data
    transformed_df = transform(extract_path, spark)

    # Step 3: Load the data into storage (e.g., Parquet)
    load_path = "output/Spotify_Transformed.parquet"
    load(transformed_df, output_path=load_path, file_format="parquet")

    # Step 4: Initialize the Spotify DataFrame Manager and execute query functions
    manager = SpotifyDataFrameManager(spark)

    # Insert record
    print(manager.query_create())
    # Read records
    print(manager.query_read())
    # Update record
    print(manager.query_update())
    # Delete record
    print(manager.query_delete())

    # Return main results for verification
    results = {
        "extract_to": extract_path,
        "transform_db": load_path,
        "create": manager.query_create(),
        "read": manager.query_read(),
        "update": manager.query_update(),
        "delete": manager.query_delete(),
    }

    # Stop the Spark session
    spark.stop()

    return results


# Run main function if this is the main script
if __name__ == "__main__":
    main()
