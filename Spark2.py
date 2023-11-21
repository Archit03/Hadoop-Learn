from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Spark').getOrCreate()
# print(dataframe)

df = spark.read.csv('Book.csv', header=True, inferSchema=True)
df.show()

# print(type(dataframe))
df.printSchema()

df.head(3)

df.select(['Name', 'Phone']).show()

#print(df.column)

df.describe().show()

df.withColumn('Honey Singh', df['Name']).show()
