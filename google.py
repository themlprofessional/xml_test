# Import SparkSession
from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder.appName("Spark XML Example").getOrCreate()

# Read the input data
df = spark.read.csv("input.csv")

# Convert the Spark dataframe to an XML file
df_write = df.write.format("xml").save("output.xml")

# Convert the XML file to a compressed XML file
df_write = df_write.option("codec", "org.apache.hadoop.io.compress.GzipCodec")

# Upload the compressed XML file to a GCS bucket
df_write.save("gs://my-bucket/my-file.xml.gz")
