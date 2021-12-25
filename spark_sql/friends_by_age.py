from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends-header.csv")

friends_by_age = people.select("age", "friends")
# from friends by age group by age and then compute average
friends_by_age.groupBy("age").avg("friends").show()
# sorted
friends_by_age.groupBy("age").avg("friends").sort("age").show()

# formatted more nicely
friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# with a custom column name
friends_by_age.groupBy("age").agg(func.round(func.avg("friends"), 2).alias("friends_avg")).sort("age").show()
spark.stop()