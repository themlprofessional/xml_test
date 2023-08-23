# Import SparkSession
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Spark XML Example").getOrCreate() 

# Read the input CSV 
df = spark.read.csv("data.csv")

# Convert to XML and directly save as compressed XML to GCS
df.write.format("xml") \
  .option("codec", "org.apache.hadoop.io.compress.GzipCodec") \
  .save("gs://my-bucket/output.xml.gz")

# Stop the SparkSession
spark.stop()
