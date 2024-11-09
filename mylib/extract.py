from pyspark.sql import SparkSession
import requests
import os


def extract(
    url="https://raw.githubusercontent.com/RunCHIRON/dataset/refs/heads/main/Spotify_2023.csv",
    file_path="data/Spotify_Most_Streamed_Songs.csv",
    timeout=10,
):
    """
    Downloads a file from a specified URL and saves it to the given file path.

    Args:
        url (str): URL to download the file from.
        file_path (str): Local path where the file will be saved.
        timeout (int): Request timeout in seconds.

    Returns:
        str: Path to the downloaded file.
    """
    # Ensure the 'data' directory exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    # Download and save the file with a timeout
    with requests.get(url, timeout=timeout) as response:
        response.raise_for_status()  # Raise an error for bad status codes
        with open(file_path, "wb") as file:
            file.write(response.content)

    return file_path


def load_data(spark, file_path):
    """
    Loads data from a CSV file into a PySpark DataFrame.

    Args:
        spark (SparkSession): Active Spark session.
        file_path (str): Path to the CSV file.

    Returns:
        DataFrame: PySpark DataFrame containing the data.
    """
    return spark.read.csv(file_path, header=True, inferSchema=True)


if __name__ == "__main__":
    # Initialize a Spark session
    spark = SparkSession.builder.appName("ExtractSpotifyDataset").getOrCreate()

    # Extract the dataset
    file_path = extract()

    # Load it into a Spark DataFrame
    spotify_df = load_data(spark, file_path)

    # Show the DataFrame schema and first few rows for verification
    spotify_df.printSchema()
    spotify_df.show(5)

    # Stop the Spark session
    spark.stop()
