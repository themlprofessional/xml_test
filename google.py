# Create a SparkSession
spark = SparkSession.builder.appName("WriteDataFrameToGCS").getOrCreate()

# Read the input data
df = spark.read.csv("input_data.csv")

# Convert the Spark DataFrame to an XML file
df_write = df.toDF("name", "age")
df_write = df_write.option("codec", "org.apache.hadoop.io.compress.GzipCodec")
df_write.write.format("xml").save("gs://bucket_name/output_data.xml.gz")
