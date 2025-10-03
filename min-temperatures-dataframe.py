from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([
    StructField("stationID", StringType(), True),
    StructField("date", IntegerType(), True),
    StructField("measure_type", StringType(), True),
    StructField("temperature", FloatType(), True)
])

inputDf = spark.read.schema(schema).csv("1800.csv")

inputDf.printSchema()
minTemps = inputDf.filter(inputDf.measure_type == "TMIN").select("stationID", "temperature").groupBy("stationID").agg(func.min("temperature").alias("min_temperature"))

minTemps.show()

minTempbyStation = minTemps.withColumn("min_temperature", func.round(func.col("min_temperature") * 0.1 *(9.0 /5.0) + 32.0, 2)).select("stationID", "min_temperature").sort("min_temperature")

result=minTempbyStation.collect()

for r in result:
    print("StationID: %s has minimum temperature: %s" % (r['stationID'], r['min_temperature']))
# minTemps.show()