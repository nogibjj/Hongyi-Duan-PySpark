from library.lib import init_spark, read_csv, count_team_appearances, get_elo_prob1_statistics
from pyspark.sql import SparkSession
import pandas as pd

spark = init_spark("TeamStats")
file_path = "nba_elo_latest.csv"
data = read_csv(file_path, spark)

def test_init_spark():
    assert isinstance(spark, SparkSession), "Test failed."
    print("Spark initiation test passed successfully.")

def test_read_csv():
    assert data.count() > 0, "Test failed."
    print("CSV file reading test passed successfully.")
    
def test_count_team_appearances():
    team_count = count_team_appearances(data)
    first_row = team_count.first()
    first_team = first_row['team1']
    first_team_count = first_row['team1_count']
    assert first_team == 'GSW', "Test failed."
    assert first_team_count == 47, "Test failed."
    print("Count team appearances test passed successfully.")

def test_get_elo_prob1_statistics():
    elo_prob1_stats = get_elo_prob1_statistics(data)
    elo_prob1_mean = elo_prob1_stats.first()['mean']
    assert elo_prob1_mean > 0.62, "Test failed."
    assert elo_prob1_mean < 0.63, "Test failed."
    print("Get elo_prob1 statistics test passed successfully.")

if __name__ == "__main__":
    test_init_spark()
    test_read_csv()
    test_count_team_appearances()
    test_get_elo_prob1_statistics()