import os
import pytest
from mylib.lib import (
    start_spark,
    end_spark,
    extract,
    load_data,
    describe,
    query,
    example_transform
)

# Directory where output files will be saved
OUTPUT_DIR = "output"

@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def test_extract():
    url = (
        "https://raw.githubusercontent.com/nogibjj/"
        "chris_moreira_week5_python_sql_db_project/"
        "main/data/Spotify_Most_Streamed_Songs.csv"
    )
    file_path = "data/Spotify_Most_Streamed_Songs.csv"
    extract(url, file_path)
    assert os.path.exists(file_path) is True


def test_load_data(spark):
    df = load_data(spark)
    assert df is not None
    assert df.count() > 0
    assert os.path.exists(os.path.join(OUTPUT_DIR, "load_data_output.csv"))


def test_describe(spark):
    df = load_data(spark)
    describe(df)
    # Check that the describe output CSV file exists
    assert os.path.exists(os.path.join(OUTPUT_DIR, "describe_output.csv"))


def test_query(spark):
    df = load_data(spark)
    query_text = (
        "SELECT released_year, COUNT(DISTINCT track_name) AS unique_tracks "
        "FROM SpotifyData GROUP BY released_year ORDER BY released_year"
    )
    query(spark, df, query_text, "SpotifyData")
    # Check that the query output CSV file exists
    assert os.path.exists(os.path.join(OUTPUT_DIR, "query_output.csv"))


def test_example_transform(spark):
    df = load_data(spark)
    example_transform(df)
    # Check that the transform output CSV file exists
    assert os.path.exists(os.path.join(OUTPUT_DIR, "transform_output.csv"))


if __name__ == "__main__":
    # Run tests manually
    spark = start_spark("TestApp")
    test_extract()
    test_load_data(spark)
    test_describe(spark)
    test_query(spark)
    test_example_transform(spark)
    end_spark(spark)
