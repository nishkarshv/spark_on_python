from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temp = float(fields[3])*0.1*(9.0/5.0) + 32.0
    return (stationID, entryType, temp)

lines = sc.textFile("1800.csv")

parsedlines = lines.map(parseLine)
minTemp = parsedlines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemp.map(lambda x: (x[0],x[2]))
mintemps = stationTemps.reduceByKey(lambda x,y :min(x,y))


results = mintemps.collect()
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))