from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCountBetterSortedDataFrame").getOrCreate()

inputDf= spark.read.text("book.txt")

words = inputDf.select(func.explode(func.split(inputDf.value, "\\W+")).alias("word"))
wordswithoutemptystrings = words.filter(words.word != "")

lowercasewords = wordswithoutemptystrings.select(func.lower(wordswithoutemptystrings.word).alias("word"))

wordCounts = lowercasewords.groupBy("word").count()

sortedWordCounts = wordCounts.orderBy("count")

sortedWordCounts.show(sortedWordCounts.count())