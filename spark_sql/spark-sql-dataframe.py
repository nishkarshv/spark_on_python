from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
people = spark.read.option("header", "true").option("inferSchema", "true").csv("data/fakefriends-header.csv")

print("Inferred schema: ")
people.printSchema()

people.select("name").show()

people.filter(people.age < 21).show()

print("Group by Age")
people.groupBy("age").count().show()

print("Make everyone 10years older")
people.select(people.name, people.age + 10).show()
spark.stop()