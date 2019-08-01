"""
Parse and extract data from XML files with PySpark

This script is a PySpark job to parse xml files into DataFrame, then save as csv files.
    1. read each xml file into an RDD
    2. parse the xml tree to get the records which are flattened as RDDs
    3. convert the RDDs to DataFrame
    4. save the DataFrame to csv files
"""

from datetime import datetime
import xml.etree.ElementTree as ET

from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType, StructField, StringType, 
    FloatType, DateType)

# columns for the DataFrame
COL_NAMES = ['book_id', 'author', 'title', 'genre', 'price', 'publish_date',
            'description']
ELEMENTS_TO_EXTRAT = [c for c in COL_NAMES if c != 'book_id']

def set_schema():
    """
    Define the schema for the DataFrame
    """
    schema_list = []
    for c in COL_NAMES:
        if c == 'price':
            schema_list.append(StructField(c, FloatType(), True))
        elif c == 'publish_date':
            schema_list.append(StructField(c, DateType(), True))
        else:
            schema_list.append(StructField(c, StringType(), True))
    
    return StructType(schema_list)

def parse_xml(rdd):
    """
    Read the xml string from rdd, parse and extract the elements,
    then return a list of list.
    """
    results = []
    root = ET.fromstring(rdd[0])
    # tree = ET.parse("./data/books3.xml")
    # root = tree.getroot()

    for b in root.findall('book'):
        rec = []
        rec.append(b.attrib['id'])
        for e in ELEMENTS_TO_EXTRAT:
            value = None
            if b.find(e) is None:
                rec.append(value)
                continue
            value = b.find(e).text
            if e == 'price':
                value = float(value)
            elif e == 'publish_date':
                value = datetime.strptime(value, '%Y-%m-%d')
            rec.append(value)
        results.append(rec)

    return results

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .getOrCreate()

    # define the schema
    my_schema = set_schema()
    # read each xml file as one row, then convert to RDD
    file_rdd = spark.read.text("./data/*.xml", wholetext=True).rdd
    # parse xml tree, extract the records and transform to new RDD
    records_rdd = file_rdd.flatMap(parse_xml)
    # convert RDDs to DataFrame with the pre-defined schema
    book_df = records_rdd.toDF(my_schema)

    # write to csv
    book_df.write.format("csv").mode("overwrite")\
        .save("./output")


