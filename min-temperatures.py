from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    date = fields[1]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, date, entryType, temperature)

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
minTemps = parsedLines.filter(lambda x: "TMIN" in x[2])
stationTemps = minTemps.map(lambda x: (x[0], (x[3], x[1])))  # (stationID, (temperature, date))
minTemps = stationTemps.reduceByKey(lambda x, y: x if x[0] < y[0] else y)  # keep min temp and its date
results = minTemps.collect()

for result in results:
    station = result[0]
    temp = result[1][0]
    date = result[1][1]
    print(f"{station}\t{date}\t{temp:.2f}F")
