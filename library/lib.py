from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, mean, stddev, min, max

def init_spark(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()

def read_csv(file_path, spark_session):
    """
    Reads a CSV file into a Spark DataFrame.
    
    Args:
    file_path (str): The path to the CSV file.
    
    Returns:
    DataFrame: The Spark DataFrame containing the CSV data.
    """
    return spark_session.read.csv(file_path, header=True, inferSchema=True)

def count_team_appearances(data):
    """
    Counts the number of times each team appears in the `team1` column.
    
    Args:
    data (DataFrame): The Spark DataFrame containing the data.
    
    Returns:
    DataFrame: A DataFrame with each team and their count.
    """
    return data.groupBy("team1").agg(count("team1").alias("team1_count"))

def get_elo_prob1_statistics(data):
    """
    Calculates descriptive statistics for the `elo_prob1` column.
    
    Args:
    data (DataFrame): The Spark DataFrame containing the data.
    
    Returns:
    DataFrame: A DataFrame containing mean, stddev, min, and max of `elo_prob1`.
    """
    return data.select(
        mean("elo_prob1").alias("mean"),
        stddev("elo_prob1").alias("stddev"),
        min("elo_prob1").alias("min"),
        max("elo_prob1").alias("max")
    )