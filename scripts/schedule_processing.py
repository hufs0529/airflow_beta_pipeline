from os.path import expanduser, join, abspath

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from datetime import datetime

warehouse_location = abspath('spark-warehouse')
date = datetime.today().strftime("%Y{}%m{}%d").format('_','_') 

# Initialize Spark Session
spark = SparkSession \
    .builder \
    .appName("Schedule processing") \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .enableHiveSupport() \
    .getOrCreate()

# Read the file forex_rates.json from the HDFS
df = spark.read.csv(f'hdfs://namenode:9000/csv파일/{date} + ".csv"')

# Drop the duplicated rows based on the base and last_update columns
uncorrect_review = df.select('맛', '양', '배달', '리뷰')

# Export the dataframe into the Hive table forex_rates
uncorrect_review.write.mode("append").insertInto("uncorrect_review")