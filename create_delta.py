from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Test').enableHiveSupport().getOrCreate()
df = spark.read.csv("/Users/danielbeach/Downloads/202208-divvy-tripdata.csv", header='true')
df.write.format("delta").save("/Users/danielbeach/code/rust_delta/trips")
