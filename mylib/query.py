# Import necessary libraries
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    FloatType,
)
from pyspark.sql.functions import when

# Define schema for the SpotifyDB table
schema = StructType(
    [
        StructField("music_id", IntegerType(), True),
        StructField("track_name", StringType(), True),
        StructField("artist_name", StringType(), True),
        StructField("artist_count", IntegerType(), True),
        StructField("released_year", IntegerType(), True),
        StructField("released_month", IntegerType(), True),
        StructField("released_day", IntegerType(), True),
        StructField("in_spotify_playlists", IntegerType(), True),
        StructField("in_spotify_charts", IntegerType(), True),
        StructField("streams", IntegerType(), True),
        StructField("in_apple_playlists", IntegerType(), True),
        StructField("in_apple_charts", IntegerType(), True),
        StructField("in_deezer_playlists", IntegerType(), True),
        StructField("in_deezer_charts", IntegerType(), True),
        StructField("in_shazam_charts", IntegerType(), True),
        StructField("bpm", IntegerType(), True),
        StructField("key", StringType(), True),
        StructField("mode", StringType(), True),
        StructField("danceability_percent", FloatType(), True),
        StructField("valence_percent", FloatType(), True),
        StructField("energy_percent", FloatType(), True),
        StructField("acousticness_percent", FloatType(), True),
        StructField("instrumentalness_percent", FloatType(), True),
        StructField("liveness_percent", FloatType(), True),
        StructField("speechiness_percent", FloatType(), True),
    ]
)

# Define a Spotify DataFrame Manager Class
class SpotifyDataFrameManager:
    def __init__(self, spark_session):
        # Store the Spark session as an instance attribute
        self.spark = spark_session
        # Initialize DataFrame placeholder
        self.spotify_df = self.spark.createDataFrame([], schema)

    def query_create(self):
        new_data = [
            (
                12345,
                "Sample Track",
                "Sample Artist",
                3,
                2023,
                8,
                15,
                100,
                50,
                50000000,
                200,
                180,
                150,
                100,
                50,
                120,
                "A",
                "Minor",
                80.5,
                70.4,
                90.2,
                12.3,
                0.0,
                15.6,
                5.8,
            )
        ]
        new_record_df = self.spark.createDataFrame(new_data, schema)
        self.spotify_df = self.spotify_df.union(new_record_df)
        return "Create Success"

    def query_read(self, limit=5):
        self.spotify_df.show(limit)
        return "Read Success"

    def query_update(self, record_id=12345, new_artist_name="Updated Artist"):
        self.spotify_df = self.spotify_df.withColumn(
            "artist_name",
            when(self.spotify_df.music_id == record_id, new_artist_name).otherwise(
                self.spotify_df.artist_name
            ),
        )
        return "Update Success"

    def query_delete(self, record_id=12345):
        self.spotify_df = self.spotify_df.filter(self.spotify_df.music_id != record_id)
        return "Delete Success"
