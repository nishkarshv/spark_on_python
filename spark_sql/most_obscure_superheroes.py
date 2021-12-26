from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType,StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperHero").getOrCreate()
schema = StructType([
    StructField("id", IntegerType(), True),\
        StructField("name", StringType(), True)
])

names = spark.read.schema(schema).option("sep", " ").csv("data/marvel_names.txt")
lines = spark.read.text("data/marvel_graph.txt")
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))
    
minconnectioncount = connections.agg(func.min("connections")).first()[0]

minconnections = connections.filter(func.col("connections") == minconnectioncount)

minconnectionswithnames = minconnections.join(names, "id")

print("Following characters have only "+ str(minconnectioncount)+ " connection(s):")

minconnectionswithnames.select("name").show()
spark.stop()