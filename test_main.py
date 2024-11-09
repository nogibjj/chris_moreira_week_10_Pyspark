"""
Test Main
"""

from main import main


def test_function():
    # Run the main function and capture results
    return main()


if __name__ == "__main__":
    # Run the test function and capture results
    results = test_function()

    # Assertions to verify each operation's output
    assert (
        results["extract_to"] == "data/Spotify_Most_Streamed_Songs.csv"
    ), "Extraction path mismatch"
    assert (
        results["transform_db"] == "output/Spotify_Transformed.parquet"
    ), "Transformation output path mismatch"
    assert results["create"] == "Create Success", "Create operation failed"
    assert results["read"] == "Read Success", "Read operation failed"
    assert results["update"] == "Update Success", "Update operation failed"
    assert results["delete"] == "Delete Success", "Delete operation failed"

    print("All tests passed successfully.")
