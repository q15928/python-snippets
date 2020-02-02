from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window

spark = SparkSession \
    .builder \
    .master('local[*]') \
    .getOrCreate()
spark.conf.set('spark.sql.shuffle.partitions', '5')

df = spark.createDataFrame([
    (18, "2017-03-09T10:27:18+00:00", "GOOG"),
    (17, "2017-03-10T15:27:18+00:00", "GOOG"),
    (22, "2017-03-13T12:27:18+00:00", "GOOG"),
    (13, "2017-03-15T12:27:18+00:00", "GOOG"),
    (19, "2017-03-15T02:27:18+00:00", "GOOG"),
    (25, "2017-03-18T11:27:18+00:00", "GOOG")],
    ["Close", "timestampGMT", "Symbol"])
df.printSchema()

df = df.withColumn('unix_time', F.unix_timestamp(F.col('timestampGMT'), "yyyy-MM-dd'T'HH:mm:ssX"))
df = df.withColumn('local_date', F.date_trunc('day', 'timestampGMT'))
df_by_date = df.groupBy('Symbol', 'local_date').agg(F.avg('Close').alias('avg_close'))
df_by_date.show(truncate=False)

# rolling mean for 7 days
days = lambda d: d * 24 * 60 * 60

win_spec = Window.partitionBy('Symbol') \
    .orderBy('unix_time') \
    .rangeBetween(-days(7), 0)
col_mean_7d = F.avg('Close').over(win_spec)
df_mean = df.withColumn('mean_7d', col_mean_7d)
df_mean.show(truncate=False)

# rolling mean for past 5 rows
win_spec = Window.partitionBy('Symbol') \
    .orderBy('unix_time') \
    .rowsBetween(-4, 0)
col_mean_5r = F.avg('Close').over(win_spec)
df_mean = df.withColumn('mean_5r', col_mean_5r)
df_mean.show(truncate=False)

# rolling max for past 3 rows
win_spec = Window.partitionBy('Symbol') \
    .orderBy('unix_time') \
    .rowsBetween(-2, 0)
col_max_3r = F.max('Close').over(win_spec)
df_max = df.withColumn('max_3r', col_max_3r)
df_max.show(truncate=False)