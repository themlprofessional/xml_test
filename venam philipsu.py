from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Spark XML Example").getOrCreate()

df = spark.read.csv("dbfs:/FileStore/annual.csv")

# repartitioning the dataFrame into a single partition and save as a single xml file with gzip compression
df.repartition(1).write.format("xml") \
  .option("compression", "gzip") \
  .mode("overwrite") \
  .save("gs://bkt-d-use1-lcef-target/xml_output_hello.xml.gz")

spark.stop()
