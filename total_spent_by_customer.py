from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(",")
    ID = int(fields[0])
    amount = float(fields[2])
    return (ID, amount)
    
conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


fields = sc.textFile("customer-orders.csv")
rdd = fields.map(parseLine)

amountspend = rdd.reduceByKey(lambda x,y: x+y)
flipped = amountspend.map(lambda x : (x[1], x[0]))
amoutspendsorted = flipped.sortByKey()
results = amoutspendsorted.collect()
for result in results:
    print(result)