from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperature").getOrCreate()
schema = StructType([StructField("stationID", StringType(), True), StructField("date", IntegerType(), True), \
    StructField("measure_type", StringType(),True), StructField("temperature", FloatType(),True)])

#  Read file as dataframe
df = spark.read.schema(schema).csv("data/1800.csv")
df.printSchema()

# Filter out TMIN entries
mintemps = df.filter(df.measure_type == "TMIN")

stationtemps = mintemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
mintempbystation = stationtemps.groupBy("stationID").min("temperature")
mintempbystation.show()
# convert temperature to fahrenheit and sort the dataset
mintempbystationf = mintempbystation.withColumn("temperature", func.round(func.col("min(temperature)")*0.1*(9.0/5.0) +32.0,2)).select("stationID", "temperature").sort("temperature")

results = mintempbystationf.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
spark.stop()