import re
from pyspark import SparkConf, SparkContext


def parseLine(line):
    fields = line.split(',')
    customerID = fields[0]
    orderID = fields[1]
    amount = fields[2]
    return (customerID, orderID, amount)

conf = SparkConf().setMaster("local").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

input = sc.textFile("customer-orders.csv")
# Map each line to (customerID, amount) pairs using the parseLine function
parsedLines = input.map(parseLine)

mappedInput = parsedLines.map(lambda x: (int(x[0]), float(x[2])))
# Reduce by customerID to sum up the amounts
totalsByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

totalsByCustomer = totalsByCustomer.map(lambda x: (int(x[0]), round(float(x[1]), 2)))
# Sort by amount spent in descending order
totalsByCustomer = totalsByCustomer.map(lambda x: (x[0], x[1])).sortBy(lambda x: -x[1])

results = totalsByCustomer.collect()
# results_sorted = sorted(results, key=lambda x: x[0])
for result in results:
    print(result)