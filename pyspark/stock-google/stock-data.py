from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("stock data") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("./data/GOOG.csv")

# in case we need to convert the Date column to timestamp explicitly     
# df = df.withColumn("Date", F.unix_timestamp("Date", "yyyy-MM-dd").cast("timestamp"))

df = df.withColumn("Id", F.lit("GOOG"))
df.printSchema()
tmp_df1 = df.where("Date < '2018-08-01'")

df.createOrReplaceTempView("df")

# get previous row (lag) and next row (lead)
win_spec = Window.partitionBy(F.col("Id")). orderBy(F.col("Date").asc())
tmp_df = df.withColumn("close_lag_1", F.lag(F.col("Close"), 1).over(win_spec)) \
    .withColumn("close_lead_1", F.lead(F.col("Close"), 1).over(win_spec))
tmp_df.show()

# get rolling average of last 7 days with PySpark
# a lambda function to convert days into seconds
days = lambda d: d * 24 * 60 * 60

win_spec = Window.partitionBy(F.col("Id")) \
    .orderBy(F.col("Date").cast("long")) \
    .rangeBetween(-days(7), 0)
col_mean_7d = F.avg("Close").over(win_spec)
tmp_df = df.withColumn("mean_7d", col_mean_7d)
tmp_df.show()

# get rolling average of last 7 days with Spark SQL
stmt = """
    SELECT *, mean(Close) OVER(
        ORDER BY Date ASC NULLS LAST
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
        ) AS mean_7d
    FROM df"""
tmp_df = spark.sql(stmt)
tmp_df.show()


df.where("Date >= '2018-07-20' and Date <= '2018-07-28'") \
    .select(F.avg("Close")) \
    .show()

    