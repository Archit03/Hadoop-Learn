from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def loadMovieNames():
    movieNames = {}
    with open("u.data") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


def parseInput(line):
    fields = line.split()
    return Row(movieID=int(fields[1]), rating=float(fields[2]))


if __name__ == '__main__':
    spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

    movieNames = loadMovieNames()

    lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/ml-100k/u.data")

    movies = lines.map(parseInput())
    movieDataSet = spark.createDataFrame(movies)

    counts = movieDataSet.groupby("movieID").count()
    averageRatings = movieDataSet.groupby("movieID").avg("Rating")

    averageAndCounts = counts.join(averageRatings, "movieID")

    topTen = averageAndCounts.orderBy("avg(ratings").take(10)

    for movie in topTen:
        print(movieNames[movie[0]], movie[1], movie[2])

    spark.stop()

