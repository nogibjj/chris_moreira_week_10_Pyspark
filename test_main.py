"""
Tests for mylib functions
"""

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

def test_describe(spark):
    df = load_data(spark)
    result = describe(df)
    assert result is not None

def test_query(spark):
    df = load_data(spark)
    query_text = (
        "SELECT released_year, COUNT(DISTINCT track_name) AS "
        "unique_tracks FROM SpotifyData GROUP BY released_year "
        "ORDER BY released_year"
    )
    result = query(spark, df, query_text, "SpotifyData")
    assert result is not None

def test_example_transform(spark):
    df = load_data(spark)
    result = example_transform(df)
    assert result is not None

if __name__ == "__main__":
    # Manual execution of tests without redefining `spark`
    spark_session = start_spark("TestApp")
    test_extract()
    test_load_data(spark_session)
    test_describe(spark_session)
    test_query(spark_session)
    test_example_transform(spark_session)
    end_spark(spark_session)
