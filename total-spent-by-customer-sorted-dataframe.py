from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

schema = StructType([
    StructField("customerId", StringType(), True),
    StructField("orderId", StringType(), True),
    StructField("amount", FloatType(), True)
])

spark = SparkSession.builder.appName("TotalSpentByCustomer").getOrCreate()

df= spark.read.schema(schema).csv("customer-orders.csv")

filteredDf=df.select("customerId", "amount")
filteredDf.printSchema()

totalSpent = filteredDf.groupBy("customerId").agg(func.round(func.sum("amount"), 2).alias("totalSpent"))

totalSpent.show()

sortedTotalSpent = totalSpent.sort("totalSpent")

sortedTotalSpent.show(sortedTotalSpent.count())

spark.stop()