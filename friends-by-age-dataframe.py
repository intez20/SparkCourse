from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")


friend = lines.select("age", "friends")
print("Here is our inferred schema:")
friend.printSchema()

friend.groupBy("age").agg({"friends": "avg"}).sort("age").show()

friend.groupBy("age").avg("friends").orderBy("age").sort("age").show()

friend.groupBy("age").agg (func.round(func.avg("friends"), 2)).sort("age").show()
friend.groupBy("age").agg (func.round(
    func.avg("friends"), 2).alias("friends_avg")).sort("age").show()

spark.stop()
