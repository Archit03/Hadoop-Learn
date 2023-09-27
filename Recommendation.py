

if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    # Load up our movie ID -> name dictionary
    movieNames = loadMovieNames()

    # Filter movies rated 10 or fewer times
    popularAveragesAndCounts = averagesAndCounts.filter("count > 10")

    # Pull the top 10 results
    topTen = popularAveragesAndCounts.orderBy("avg(rating)").take(10)

    # Print them out, converting movie ID's to names as we go.
    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    # Stop the session
    spark.stop()


