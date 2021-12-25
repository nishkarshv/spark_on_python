from pyspark.sql import SparkSession
from pyspark.sql import functions as func 
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType

spark = SparkSession.builder.appName("totalSpend").getOrCreate()
schema = StructType(\
    [StructField("cust_id", IntegerType(), True), 
     StructField("item_id", IntegerType(), True), 
     StructField("total_spend", FloatType(), True)
    ])

df = spark.read.schema(schema).csv("data/customer-orders.csv")

totalbycustomer = df.groupBy("cust_id").agg(func.round(func.sum("total_spend"), 2).alias("total_spent"))

# df.printSchema()
totalbycustomerSorted = totalbycustomer.sort("total_spent")
totalbycustomerSorted.show(totalbycustomerSorted.count())
spark.stop()