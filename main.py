from library.lib import init_spark, read_csv, count_team_appearances, get_elo_prob1_statistics

if __name__ == "__main__":
    spark = init_spark("TeamStats")
    
    file_path = "nba_elo_latest.csv"
    data = read_csv(file_path, spark)
    
    team_count = count_team_appearances(data)
    team_count.show()
    
    elo_prob1_stats = get_elo_prob1_statistics(data)
    elo_prob1_stats.show()
    
    spark.stop()