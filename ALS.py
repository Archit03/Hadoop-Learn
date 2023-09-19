from pyexpat import model

import ratings
from pyspark.shell import spark
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.sql import Row
from pyspark.sql.functions import lit
import numpy as np


# Load up movie ID -> movie name dictionary
def loadMovieNames():
    movieNames = {}
    with open("u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames


# Convert u.data lines into (userID, movieID, rating) rows
def parseInput(line):
    fields = line.value.split()
    return Row(userID=int(fields[0]), movieID=int(fields[1]), rating=float
    (fields[2]))


if __name__ == "__main__":
    # Create a SparkSession (the config bit is only for Windows!)
    # Find movies rated more than 100 times
    ratingCounts = ratings.groupBy("movieID").count().filter("count > 100")
    # Construct a "test" dataframe for user 0 with every movie rated more than $
    popularMovies = ratingCounts.select("movieID").withColumn('userID', lit(0))

    # Run our model on that list of popular movies for user ID 0
    recommendations = model.transform(popularMovies)

    # Get the top 20 movies with the highest predicted rating for this user
    topRecommendations = recommendations.sort(recommendations.prediction.desc())

    for recommendation in topRecommendations:
        print(movieNames[recommendation['movieID']], recommendation['prediction'])

    spark.stop()
