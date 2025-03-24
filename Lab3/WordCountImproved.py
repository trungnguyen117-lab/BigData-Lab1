from pyspark.sql import SparkSession
import sys
import re

# Initialize Spark session
spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read input and output paths from arguments
input_path, output_path = sys.argv[1], sys.argv[2]

# Regex to split words by punctuation and spaces
word_sep = re.compile(r"[^\w]+")

# Process word count
spark.read.text(input_path).rdd.flatMap(lambda line: word_sep.split(line[0].lower())) \
    .filter(lambda word: word) \
    .map(lambda word: (word, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .saveAsTextFile(output_path)

# Stop Spark session
spark.stop()
