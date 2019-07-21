#######################
# pyspark snippets extracted from <Spark - The Definitive Guide>
#######################

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("stock data") \
    .config("spark.sql.shuffle.partitions", "5") \
    .getOrCreate()

# Chapter 5
df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")

# Load data with schema
from pyspark.sql.types import StructField, StructType, StringType, LongType
myManualSchema = StructType([
    StructField("DEST_COUNTRY_NAME", StringType(), True),
    StructField("ORIGIN_COUNTRY_NAME", StringType(), True),
    StructField("count", LongType(), False, metadata={"hello":"world"})
])
df = spark.read.format("json").schema(myManualSchema)\
    .load("/data/flight-data/json/2015-summary.json")

# in Python
from pyspark.sql import Row
myRow = Row("Hello", None, 1, False)

# in Python
df.select("DEST_COUNTRY_NAME", "ORIGIN_COUNTRY_NAME").show(2)

# select columns in different ways
from pyspark.sql.functions import expr, col, column
df.select(
    expr("DEST_COUNTRY_NAME"),
    col("DEST_COUNTRY_NAME"),
    column("DEST_COUNTRY_NAME"))\
  .show(2)

df.select(expr("DEST_COUNTRY_NAME AS destination")).show(2)
df.select(expr("DEST_COUNTRY_NAME as destination").alias("DEST_COUNTRY_NAME"))

df.selectExpr(
    "*", # all original columns
    "(DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry")\
.show(2)

df.selectExpr("avg(count)", "count(distinct(DEST_COUNTRY_NAME))").show(2)

# we need to pass explicit values into Spark that are just a value (rather than a new
# column).
from pyspark.sql.functions import lit
df.select(expr("*"), lit(1).alias("One")).show(2)

# add a column
df.withColumn("numberOne", lit(1)).show(2)
df.withColumn("withinCountry", expr("ORIGIN_COUNTRY_NAME == DEST_COUNTRY_NAME"))\
  .show(2)

# rename column
df.withColumnRenamed("DEST_COUNTRY_NAME", "dest").columns

# remove column
df.drop("ORIGIN_COUNTRY_NAME").columns

# change a column's type
df.withColumn("count2", col("count").cast("long"))

# filter rows
df.where(col("count") < 2).where(col("ORIGIN_COUNTRY_NAME") != "Croatia")\
  .show(2)

# get unique rows
df.select("ORIGIN_COUNTRY_NAME", "DEST_COUNTRY_NAME").distinct().count()
# -- in SQL
# SELECT COUNT(DISTINCT(ORIGIN_COUNTRY_NAME, DEST_COUNTRY_NAME)) FROM dfTable

# random sample
seed = 5
withReplacement = False
fraction = 0.5
df.sample(withReplacement, fraction, seed).count()

# random split
dataFrames = df.randomSplit([0.25, 0.75], seed)
dataFrames[0].count() > dataFrames[1].count() # False

# union two dataframes
# you must be sure that they have the same schema and number of columns;
# otherwise, the union will fail
from pyspark.sql import Row
schema = df.schema
newRows = [
    Row("New Country", "Other Country", 5L),
    Row("New Country 2", "Other Country 3", 1L)
]
parallelizedRows = spark.sparkContext.parallelize(newRows)
newDF = spark.createDataFrame(parallelizedRows, schema)
# in Python
df.union(newDF)\
  .where("count = 1")\
  .where(col("ORIGIN_COUNTRY_NAME") != "United States")\
  .show()

# sort rows
# in Python
from pyspark.sql.functions import desc, asc
df.orderBy(expr("count desc")).show(2)
df.orderBy(col("count").desc(), col("DEST_COUNTRY_NAME").asc()).show(2)

# repartition and coalesce
df.rdd.getNumPartitions() # 1
# in Python
df.repartition(5)

# If you know that you’re going to be filtering by a certain column often, it can be worth repartitioning
# based on that column:
df.repartition(5, col("DEST_COUNTRY_NAME"))

# Coalesce, on the other hand, will not incur a full shuffle and will try to combine partitions
df.repartition(5, col("DEST_COUNTRY_NAME")).coalesce(2)

# Chapter 6
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/by-day/2010-12-01.csv")
df.printSchema()
df.createOrReplaceTempView("dfTable")

# check substring
from pyspark.sql.functions import instr
priceFilter = col("UnitPrice") > 600
descripFilter = instr(df.Description, "POSTAGE") >= 1
df.where(df.StockCode.isin("DOT")).where(priceFilter | descripFilter).show()
# -- in SQL
# SELECT * FROM dfTable WHERE StockCode in ("DOT") AND(UnitPrice > 600 OR
# instr(Description, "POSTAGE") >= 1)

# boolean column
from pyspark.sql.functions import instr
DOTCodeFilter = col("StockCode") == "DOT"
priceFilter = col("UnitPrice") > 600
descripFilter = instr(col("Description"), "POSTAGE") >= 1
df.withColumn("isExpensive", DOTCodeFilter & (priceFilter | descripFilter))\
  .where("isExpensive")\
  .select("unitPrice", "isExpensive").show(5)
# -- in SQL
# SELECT UnitPrice, (StockCode = 'DOT' AND
# (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1)) as isExpensive
# FROM dfTable
# WHERE (StockCode = 'DOT' AND
# (UnitPrice > 600 OR instr(Description, "POSTAGE") >= 1))

df.where(col("Description").eqNullSafe("hello")).show()

# in Python
df.selectExpr(
  "CustomerId",
  "(POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity").show(2)
# -- in SQL
# SELECT customerId, (POWER((Quantity * UnitPrice), 2.0) + 5) as realQuantity
# FROM dfTable

# compute correlation of two columns
from pyspark.sql.functions import corr
df.stat.corr("Quantity", "UnitPrice")
df.select(corr("Quantity", "UnitPrice")).show()
# -- in SQL
# SELECT corr(Quantity, UnitPrice) FROM dfTable

# summary statistics
df.describe().show()

# in Python
df.stat.crosstab("StockCode", "Quantity").show()
df.stat.freqItems(["StockCode", "Quantity"]).show()

# in Python
from pyspark.sql.functions import monotonically_increasing_id
df.select(monotonically_increasing_id()).show(2)

# capitalize every word in a given string when that word is separated from another by a space
from pyspark.sql.functions import initcap
df.select(initcap(col("Description"))).show()
# -- in SQL
# SELECT initcap(Description) FROM dfTable

# in Python
from pyspark.sql.functions import lit, ltrim, rtrim, rpad, lpad, trim
df.select(
  ltrim(lit(" HELLO ")).alias("ltrim"),
  rtrim(lit(" HELLO ")).alias("rtrim"),
  trim(lit(" HELLO ")).alias("trim"),
  lpad(lit("HELLO"), 3, " ").alias("lp"),
  rpad(lit("HELLO"), 10, " ").alias("rp")).show(2)
# -- in SQL
# SELECT
# ltrim(' HELLLOOOO '),
# rtrim(' HELLLOOOO '),
# trim(' HELLLOOOO '),
# lpad('HELLOOOO ', 3, ' '),
# rpad('HELLOOOO ', 10, ' ')
# FROM dfTable

# regexp
from pyspark.sql.functions import regexp_replace
regex_string = "BLACK|WHITE|RED|GREEN|BLUE"
df.select(
  regexp_replace(col("Description"), regex_string, "COLOR").alias("color_clean"),
  col("Description")).show(2)
# -- in SQL
# SELECT
#   regexp_replace(Description, 'BLACK|WHITE|RED|GREEN|BLUE', 'COLOR') as
#   color_clean, Description
# FROM dfTable

# replace strings
from pyspark.sql.functions import translate
df.select(translate(col("Description"), "LEET", "1337"),col("Description"))\
  .show(2)
# -- in SQL
# SELECT translate(Description, 'LEET', '1337'), Description FROM dfTable

# regexp_extract
from pyspark.sql.functions import regexp_extract
extract_str = "(BLACK|WHITE|RED|GREEN|BLUE)"
df.select(
  regexp_extract(col("Description"), extract_str, 1).alias("color_clean"),
  col("Description")).show(2)
# -- in SQL
# SELECT regexp_extract(Description, '(BLACK|WHITE|RED|GREEN|BLUE)', 1),
#   Description
# FROM dfTable

# in Python
from pyspark.sql.functions import expr, locate
simpleColors = ["black", "white", "red", "green", "blue"]
def color_locator(column, color_string):
    return locate(color_string.upper(), column)\
        .cast("boolean")\
        .alias("is_" + c)

selectedColumns = [color_locator(df.Description, c) for c in simpleColors]
selectedColumns.append(expr("*")) # has to a be Column type
df.select(*selectedColumns).where(expr("is_white OR is_red"))\
  .select("Description").show(3, False)

# in Python
from pyspark.sql.functions import current_date, current_timestamp
dateDF = spark.range(10)\
  .withColumn("today", current_date())\
  .withColumn("now", current_timestamp())
dateDF.createOrReplaceTempView("dateTable")

from pyspark.sql.functions import date_add, date_sub
dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(1)
# -- in SQL
# SELECT date_sub(today, 5), date_add(today, 5) FROM dateTable

from pyspark.sql.functions import datediff, months_between, to_date
dateDF.withColumn("week_ago", date_sub(col("today"), 7))\
  .select(datediff(col("week_ago"), col("today"))).show(1)
dateDF.select(
    to_date(lit("2016-01-01")).alias("start"),
    to_date(lit("2017-05-22")).alias("end"))\
  .select(months_between(col("start"), col("end"))).show(1)
# -- in SQL
# SELECT to_date('2016-01-01'), months_between('2016-01-01', '2017-01-01'),
# datediff('2016-01-01', '2017-01-01')
# FROM dateTable

from pyspark.sql.functions import to_date
dateFormat = "yyyy-dd-MM"
cleanDateDF = spark.range(1).select(
    to_date(lit("2017-12-11"), dateFormat).alias("date"),
    to_date(lit("2017-20-12"), dateFormat).alias("date2"))
cleanDateDF.createOrReplaceTempView("dateTable2")
# -- in SQL
# SELECT to_date(date, 'yyyy-dd-MM'), to_date(date2, 'yyyy-dd-MM'), to_date(date)
# FROM dateTable2

# in Python
from pyspark.sql.functions import to_timestamp
cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()
# -- in SQL
# SELECT to_timestamp(date, 'yyyy-dd-MM'), to_timestamp(date2, 'yyyy-dd-MM')
# FROM dateTable2

# SELECT cast(to_date("2017-01-01", "yyyy-dd-MM") as timestamp)

# Spark includes a function to allow you to select the first non-null value from a set of columns by using
# the coalesce function. 
from pyspark.sql.functions import coalesce
df.select(coalesce(col("Description"), col("CustomerId"))).show()

# deal with null
# -- in SQL
# SELECT
# ifnull(null, 'return_value'),
# nullif('value', 'value'),
# nvl(null, 'return_value'),
# nvl2('not_null', 'return_value', "else_value")
# FROM dfTable LIMIT 1

df.na.drop("all", subset=["StockCode", "InvoiceNo"])
fill_cols_vals = {"StockCode": 5, "Description" : "No Value"}
df.na.fill(fill_cols_vals)
df.na.replace([""], ["UNKNOWN"], "Description")

# struct type
from pyspark.sql.functions import struct
complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
complexDF.createOrReplaceTempView("complexDF")
complexDF.select("complex.Description")
complexDF.select(col("complex").getField("Description"))

complexDF.select("complex.*")
# -- in SQL
# SELECT complex.* FROM complexDF

# array type
from pyspark.sql.functions import split
df.select(split(col("Description"), " ")).show(2)
# -- in SQL
# SELECT split(Description, ' ') FROM dfTable

df.select(split(col("Description"), " ").alias("array_col"))\
  .selectExpr("array_col[0]").show(2)
# -- in SQL
# SELECT split(Description, ' ')[0] FROM dfTable

from pyspark.sql.functions import size
df.select(size(split(col("Description"), " "))).show(2) # shows 5 and 3

from pyspark.sql.functions import array_contains
df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
# -- in SQL
# SELECT array_contains(split(Description, ' '), 'WHITE') FROM dfTable

from pyspark.sql.functions import split, explode
df.withColumn("splitted", split(col("Description"), " "))\
  .withColumn("exploded", explode(col("splitted")))\
  .select("Description", "InvoiceNo", "exploded").show(2)
# -- in SQL
# SELECT Description, InvoiceNo, exploded
# FROM (SELECT *, split(Description, " ") as splitted FROM dfTable)
# LATERAL VIEW explode(splitted) as exploded

# work with JSON
from pyspark.sql.functions import from_json
from pyspark.sql.types import *
parseSchema = StructType((
StructField("InvoiceNo",StringType(),True),
StructField("Description",StringType(),True)))
df.selectExpr("(InvoiceNo, Description) as myStruct")\
  .select(to_json(col("myStruct")).alias("newJSON"))\
  .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)

## Chapter 7
df = spark.read.format("csv")\
  .option("header", "true")\
  .option("inferSchema", "true")\
  .load("/data/retail-data/all/*.csv")\
  .coalesce(5)
df.cache()
df.createOrReplaceTempView("dfTable")

from pyspark.sql.functions import count
df.select(count("StockCode")).show() # 541909

from pyspark.sql.functions import countDistinct
df.select(countDistinct("StockCode")).show() # 4070
# -- in SQL
# SELECT COUNT(DISTINCT *) FROM DFTABLE

from pyspark.sql.functions import approx_count_distinct
df.select(approx_count_distinct("StockCode", 0.1)).show() # 3364
# -- in SQL
# SELECT approx_count_distinct(StockCode, 0.1) FROM DFTABLE

from pyspark.sql.functions import first, last
df.select(first("StockCode"), last("StockCode")).show()
# -- in SQL
# SELECT first(StockCode), last(StockCode) FROM dfTable

from pyspark.sql.functions import min, max
df.select(min("Quantity"), max("Quantity")).show()
# -- in SQL
# SELECT min(Quantity), max(Quantity) FROM dfTable

from pyspark.sql.functions import sum
df.select(sum("Quantity")).show() # 5176450
# -- in SQL
# SELECT sum(Quantity) FROM dfTable

from pyspark.sql.functions import sum, count, avg, expr
df.select(
    count("Quantity").alias("total_transactions"),
    sum("Quantity").alias("total_purchases"),
    avg("Quantity").alias("avg_purchases"),
    expr("mean(Quantity)").alias("mean_purchases"))\
  .selectExpr(
    "total_purchases/total_transactions",
    "avg_purchases",
    "mean_purchases").show()

from pyspark.sql.functions import var_pop, stddev_pop
from pyspark.sql.functions import var_samp, stddev_samp
df.select(var_pop("Quantity"), var_samp("Quantity"),
    stddev_pop("Quantity"), stddev_samp("Quantity")).show()
# -- in SQL
# SELECT var_pop(Quantity), var_samp(Quantity),
# stddev_pop(Quantity), stddev_samp(Quantity)
# FROM dfTable

# Skewness and kurtosis are both measurements of extreme points in your data. Skewness measures the
# asymmetry of the values in your data around the mean, whereas kurtosis is a measure of the tail of
# data. These are both relevant specifically when modeling your data as a probability distribution of a
# random variable.
from pyspark.sql.functions import skewness, kurtosis
df.select(skewness("Quantity"), kurtosis("Quantity")).show()
# -- in SQL
# SELECT skewness(Quantity), kurtosis(Quantity) FROM dfTable

from pyspark.sql.functions import corr, covar_pop, covar_samp
df.select(corr("InvoiceNo", "Quantity"), covar_samp("InvoiceNo", "Quantity"),
covar_pop("InvoiceNo", "Quantity")).show()
# -- in SQL
# SELECT corr(InvoiceNo, Quantity), covar_samp(InvoiceNo, Quantity),
# covar_pop(InvoiceNo, Quantity)
# FROM dfTable

from pyspark.sql.functions import collect_set, collect_list
df.agg(collect_set("Country"), collect_list("Country")).show()
# -- in SQL
# SELECT collect_set(Country), collect_set(Country) FROM dfTable

from pyspark.sql.functions import count
df.groupBy("InvoiceNo").agg(
    count("Quantity").alias("quan"),
    expr("count(Quantity)")).show()

df.groupBy("InvoiceNo").agg(expr("avg(Quantity)"),expr("stddev_pop(Quantity)"))\
.show()
# -- in SQL
# SELECT avg(Quantity), stddev_pop(Quantity), InvoiceNo FROM dfTable
# GROUP BY InvoiceNo

from pyspark.sql.functions import col, to_date
dfWithDate = df.withColumn("date", to_date(col("InvoiceDate"), "MM/d/yyyy H:mm"))
dfWithDate.createOrReplaceTempView("dfWithDate")

from pyspark.sql.window import Window
from pyspark.sql.functions import desc
windowSpec = Window\
    .partitionBy("CustomerId", "date")\
    .orderBy(desc("Quantity"))\
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

from pyspark.sql.functions import max
maxPurchaseQuantity = max(col("Quantity")).over(windowSpec)

from pyspark.sql.functions import dense_rank, rank
purchaseDenseRank = dense_rank().over(windowSpec)
purchaseRank = rank().over(windowSpec)

from pyspark.sql.functions import col
(df_with_date.where("CustomerID IS NOT NULL").orderBy("CustomerID")
  .select(
     col("CustomerID"),
     col("date"),
     col("InvoiceDate"),
     col("Quantity"),
     row_number().over(windowSpec).alias("quantityRowNum"),
     purchaseRank.alias("quantityRank"),
     purchaseDenseRank.alias("quantityDenseRank"),
     maxPurchaseQuantity.alias("maxPurchaseQuantity")).show())
# -- in SQL
# SELECT CustomerId, date, Quantity,
#   rank(Quantity) OVER (PARTITION BY CustomerId, date
#                        ORDER BY Quantity DESC NULLS LAST
#                        ROWS BETWEEN
#                        UNBOUNDED PRECEDING AND
#                        CURRENT ROW) as rank,
#   dense_rank(Quantity) OVER (PARTITION BY CustomerId, date
#                              ORDER BY Quantity DESC NULLS LAST
#                              ROWS BETWEEN
#                              UNBOUNDED PRECEDING AND
#                              CURRENT ROW) as dRank,
#   max(Quantity) OVER (PARTITION BY CustomerId, date
#                       ORDER BY Quantity DESC NULLS LAST
#                       ROWS BETWEEN
#                       UNBOUNDED PRECEDING AND
#                       CURRENT ROW) as maxPurchase
# FROM dfWithDate WHERE CustomerId IS NOT NULL ORDER BY CustomerId

# get rolling mean for last 7 days
stmt = """
    SELECT *, mean(Close) OVER(
        ORDER BY Date ASC NULLS LAST
        RANGE BETWEEN INTERVAL 7 DAYS PRECEDING AND CURRENT ROW
        ) AS mean_7d
    FROM df"""
tmp_df = spark.sql(stmt)
tmp_df.show()


dfNoNull = dfWithDate.drop()
dfNoNull.createOrReplaceTempView("dfNoNull")

rolledUpDF = dfNoNull.rollup("Date", "Country").agg(sum("Quantity"))\
    .selectExpr("Date", "Country", "`sum(Quantity)` as total_quantity")\
    .orderBy("Date")
rolledUpDF.show()

pivoted = dfWithDate.groupBy("date").pivot("Country").sum()


## Chapter 8
person = spark.createDataFrame([
    (0, "Bill Chambers", 0, [100]),
    (1, "Matei Zaharia", 1, [500, 250, 100]),
    (2, "Michael Armbrust", 1, [250, 100])])\
  .toDF("id", "name", "graduate_program", "spark_status")
graduateProgram = spark.createDataFrame([
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley")])\
  .toDF("id", "degree", "department", "school")
sparkStatus = spark.createDataFrame([
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor")])\
  .toDF("id", "status")

person.createOrReplaceTempView("person")
graduateProgram.createOrReplaceTempView("graduateProgram")
sparkStatus.createOrReplaceTempView("sparkStatus")  

joinExpression = person["graduate_program"] == graduateProgram['id']
person.join(graduateProgram, joinExpression).show()
# -- in SQL
# SELECT * FROM person JOIN graduateProgram
#   ON person.graduate_program = graduateProgram.id

joinType = "outer"
person.join(graduateProgram, joinExpression, joinType).show()
# -- in SQL
# SELECT * FROM person FULL OUTER JOIN graduateProgram
# ON graduate_program = graduateProgram.id

# select name, graduate_program, spark_grade
# from person
# lateral view explode(spark_status) as spark_grade

joinType = "left_outer"
graduateProgram.join(person, joinExpression, joinType).show()
# -- in SQL
# SELECT * FROM graduateProgram LEFT OUTER JOIN person
# ON person.graduate_program = graduateProgram.id

## Chapter 9
# in Python
csvFile = spark.read.format("csv")\
  .option("header", "true")\
  .option("mode", "FAILFAST")\
  .option("inferSchema", "true")\
  .load("/data/flight-data/csv/2010-summary.csv")

csvFile.write.format("csv").mode("overwrite").option("sep", "\t")\
  .save("/tmp/my-tsv-file.tsv")

driver = "org.sqlite.JDBC"
path = "/data/flight-data/jdbc/my-sqlite.db"
url = "jdbc:sqlite:" + path
tablename = "flight_info"

dbDataFrame = spark.read.format("jdbc").option("url", url)\
  .option("dbtable", tablename).option("driver", driver).load()

# connect to PostgreSQL
pgDF = spark.read.format("jdbc")\
  .option("driver", "org.postgresql.Driver")\
  .option("url", "jdbc:postgresql://database_server")\
  .option("dbtable", "schema.tablename")\
  .option("user", "username").option("password", "my-secret-password").load()

