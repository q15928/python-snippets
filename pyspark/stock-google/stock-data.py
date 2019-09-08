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

df = df.withColumn("Symbol", F.lit("GOOG"))
df.printSchema()
tmp_df1 = df.where("Date < '2018-08-01'")

df.createOrReplaceTempView("df")

# get previous row (lag) and next row (lead)
win_spec = Window.partitionBy(F.col("Symbol")). orderBy(F.col("Date").asc())
tmp_df = df.withColumn("close_lag_1", F.lag(F.col("Close"), 1).over(win_spec)) \
    .withColumn("close_lead_1", F.lead(F.col("Close"), 1).over(win_spec))
tmp_df.show()

# get rolling average of last 7 days with PySpark
# ref https://stackoverflow.com/questions/33207164/spark-window-functions-rangebetween-dates
# ref https://stackoverflow.com/questions/45806194/pyspark-rolling-average-using-timeseries-data
# a lambda function to convert days into seconds
days = lambda d: d * 24 * 60 * 60

win_spec = Window.partitionBy(F.col("Symbol")) \
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



df = spark.createDataFrame([(17, "2017-03-10T15:27:18+00:00"),
                        (13, "2017-03-15T12:27:18+00:00"),
                        (19, "2017-03-15T02:27:18+00:00"),
                        (25, "2017-03-18T11:27:18+00:00")],
                        ["Close", "timestampGMT"])
df = df.withColumn('Symbol', F.lit('GOOG'))
df = df.withColumn('timestampGMT', df.timestampGMT.cast('timestamp'))
df = df.withColumn('Date', F.to_date('timestampGMT').cast('timestamp'))


days = lambda d: d * 24 * 60 * 60

win_spec = Window.partitionBy(F.col("Symbol")) \
    .orderBy(F.col("Date").cast("long")) \
    .rangeBetween(-days(7), 0)
col_mean_7d = F.avg("Close").over(win_spec)
tmp_df = df.withColumn("mean_7d", col_mean_7d)
tmp_df.show()