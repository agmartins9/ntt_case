from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg, sum

spark = SparkSession.builder.appName("SensorData").getOrCreate()

# Load the sensor data into a dataframe
df = spark.read.format("parquet").load("/path/to/sensor/data")

# Total number of rows
total_rows = df.count()

# Number of distinct sensors present on the database
num_sensors = df.select("name").distinct().count()

# Number of rows for the sensor PPL340
num_rows_PPL340 = df.filter(df.name == "PPL340").count()

# The number of rows by year for the sensor PPL340
num_rows_by_year_PPL340 = df.filter(df.name == "PPL340").groupBy("year").agg(count("*").alias("count")).orderBy("year")

# Average number of readings by year for the sensor PPL340
avg_readings_by_year_PPL340 = df.filter(df.name == "PPL340").groupBy("year").agg(avg("value").alias("avg_value"))

# For PPL340, Identify the years in which the number of readings is less than the average
less_than_avg_PPL340 = num_rows_by_year_PPL340.join(avg_readings_by_year_PPL340, "year").filter(num_rows_by_year_PPL340.count < avg_readings_by_year_PPL340.avg_value).select("year")

