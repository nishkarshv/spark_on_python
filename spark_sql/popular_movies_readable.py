from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
import codecs

def loadMovieNames():
    movieNames = {}
    with codecs.open("data/ml-100k/u.ITEM", "r",encoding="ISO-8859-1", errors="ignore") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())
# create schema when reading u.data
schema = StructType([
    StructField("userID", IntegerType(), True), \
        StructField("movieID", IntegerType(), True), \
            StructField("rating", IntegerType(), True), \
                StructField("timestamp", LongType(), True)
])
# load up movie data as dataframe
moviesdf = spark.read.option("sep", "\t").schema(schema).csv("data/ml-100k/u.data")


moviecounts = moviesdf.groupBy("movieID").count()
# create udf to look up movie names from our broadcasted dictionary
def lookupName(movieID):
    return nameDict.value[movieID]
lookupNameUDF = func.udf(lookupName)
# add a movietitle column using our new udf
moviesWithNames = moviecounts.withColumn("movieTitle", lookupNameUDF(func.col("movieID")))
# Sort the results
sortedMovieswithNames = moviesWithNames.orderBy(func.desc("count"))
# Grab the top 10
sortedMovieswithNames.show(10, False)
spark.stop()