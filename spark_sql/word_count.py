from pyspark.sql import functions as func 
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()
# read each line of book into dataframe
inputdf = spark.read.text("data/book.txt")

# split using a regex that extracts words

words = inputdf.select(func.explode(func.split(inputdf.value, "\\W+")).alias("word"))
words.filter(words.word != "")

# Normalize everything to lowercase
lowercasewords = words.select(func.lower(words.word).alias("word"))

# Count up the occurences of each word
wordcounts = lowercasewords.groupBy("word").count()

# sort by counts
wordcountsSorted = wordcounts.sort("count")

# show results
wordcountsSorted.show(wordcountsSorted.count())
spark.stop()