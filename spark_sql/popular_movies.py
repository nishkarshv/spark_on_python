from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, LongType


spark = SparkSession.builder.appName("PopularMovies").getOrCreate()
schema = StructType([
    StructField("userID", IntegerType(), True), \
        StructField("movieID", IntegerType(), True), \
            StructField("rating", IntegerType(), True), \
                StructField("timestamp", LongType(), True)
])

moviesdf = spark.read.option("sep", "\t").schema(schema).csv("data/ml-100k/u.data")

topmovieids = moviesdf.groupBy("movieID").count().orderBy(func.desc("count"))

topmovieids.show(10)
spark.stop()