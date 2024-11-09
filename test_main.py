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


def test_describe(spark):
    df = load_data(spark)
    result = describe(df)
    assert result is None  # Assuming describe prints the result and returns None


def test_query(spark):
    df = load_data(spark)
    query_text = (
        "SELECT released_year, COUNT(DISTINCT track_name) AS unique_tracks "
        "FROM SpotifyData GROUP BY released_year ORDER BY released_year"
    )
    result = query(spark, df, query_text, "SpotifyData")
    assert result is None  # Assuming query prints the result and returns None


def test_example_transform(spark):
    df = load_data(spark)
    result = example_transform(df)
    assert result is None  # Assuming example_transform prints the result and returns None


if __name__ == "__main__":
    # Run tests manually
    spark = start_spark("TestApp")
    test_extract()
    test_load_data(spark)
    test_describe(spark)
    test_query(spark)
    test_example_transform(spark)
    end_spark(spark)
