from pyspark.sql import SparkSession
#Must imports for not to get warnings
import os
os.environ['PYARROW_IGNORE_TIMEZONE'] = '1'
import numpy as np
import pandas as pd
import pyspark.pandas as ps #Alias for pandas API on Spark

spark = SparkSession.builder.appName("PandasConversion")\
    .config("spark.sql.ansi.enabled", "false")\
    .config("PYARROW_IGNORE_TIMEZONE", "1")\
    .getOrCreate()

# Create a sample Pandas DataFrame
ps_df = pd.DataFrame({
    "Id": ["C1", "C2", "C1", "C3"],
    "name": ["Messi", "Ronaldo", "Neymar", "Bale"],
    "age": [34.0, 36.0, 29.0, 32.0]
})
print("Pandas DataFrame:")
print(ps_df)

# Convert Pandas DataFrame to Spark DataFrame
spark_df = spark.createDataFrame(ps_df)

print("Converted Spark DataFrame Schema:")
spark_df.printSchema()

print("Converted Spark DataFrame Content:")
spark_df.show()

filtered_spark_df = spark_df.filter(spark_df.age > 32)

print("Filtered Spark DataFrame Content:(age>32)")
filtered_spark_df.show()

# Convert Spark DataFrame back to Pandas DataFrame
converted_ps_df = filtered_spark_df.toPandas()

print("Converted to Pandas DataFrame:")
print(converted_ps_df)


pandas_df=ps.DataFrame(ps_df)

print("Pandas DataFrame from Spark DataFrame:")
print(pandas_df)

pandas_df['age']=pandas_df['age']+4
print("Pandas DataFrame after adding 4 to age:")
print(pandas_df)

spark_df1=pandas_df.to_spark()

print("Spark DataFrame from modified Pandas DataFrame:")
spark_df1.show()

spark.stop()