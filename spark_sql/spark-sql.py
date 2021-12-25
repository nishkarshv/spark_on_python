from pyspark.sql import SparkSession, Row

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))
# Create a Sparksession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()
lines = spark.sparkContext.textFile("data/fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and refister the dataframe as a table
schemapeople = spark.createDataFrame(people).cache()
schemapeople.createOrReplaceTempView("people")

# SQL can be run on dataframes that have been registered as a table.
teenagers = spark.sql("Select * from people where age>=13 and age<=19")

# results of sql queries are RDDs and support all RDD operations.
for teen in teenagers.collect():
    print(teen)
    
# using functions instead of sql
schemapeople.groupby("age").count().orderBy("age").show()

spark.stop()