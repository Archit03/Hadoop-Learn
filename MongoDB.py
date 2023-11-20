from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions


def parseInput(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])


if __name__ == "__main__":
    # create a spark session
    spark = SparkSession.builder.appName("MongoDBIntegration").getOrCreate()

    # Get the raw data
    lines = spark.sparkContext.textFile("hdff:///user/maria_dev/ml-100k/u.user")

    # Convert it to a RDD of Row objects with (userID, age, gender, occupation, zip)
    users = lines.map(parseInput)
    usersDataset = spark.createDataFrame(users)

    # write it into MongoDB
    usersDataset.write \
        .format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", "mongodb://127.0.0.1/movielens.users") \
        .mode('append') \
        .save()
    ReadUsers = spark.read \
        .format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://127.0.0.1/movielens.users")\
        .load()

    ReadUsers.createOrReplaceTempView("users")

    sqlDF = spark.sql("SELECT * FROM users where age < 20")
    sqlDF.show()

    spark.stop()


