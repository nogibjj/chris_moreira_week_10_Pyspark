from mylib.lib import (
    start_spark, end_spark, extract, load_data,
    query, describe, example_transform
)

def main():
    """Main function for PySpark Operations"""
    spark = start_spark("SpotifyApp")

    # URL to the dataset
    url = (
        "https://raw.githubusercontent.com/nogibjj/"
        "chris_moreira_week5_python_sql_db_project/"
        "main/data/Spotify_Most_Streamed_Songs.csv"
    )
    file_path = "data/Spotify_Most_Streamed_Songs.csv"
    
    # Extract data
    extract(url, file_path)
    
    # Load data
    df = load_data(spark)
    
    # Query data
    query_text = (
        "SELECT released_year, COUNT(DISTINCT track_name) AS "
        "unique_tracks FROM SpotifyData GROUP BY released_year "
        "ORDER BY released_year"
    )
    query(spark, df, query_text)
    
    # Describe data
    describe(df)
    
    # Transform data
    example_transform(df)
    
    # End Spark session
    end_spark(spark)

if __name__ == "__main__":
    main()
