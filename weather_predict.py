from pyspark.sql import SparkSession, functions, types
import sys
from pprint import pprint
import pandas as pd
import numpy as np
import glob

spark = SparkSession.builder.appName('test').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5) # make sure we have Python 3.5+
assert spark.version >= '2.3' # make sure we have Spark 2.3+

weather_schema = types.StructType([
    types.StructField('STATION', types.StringType()),
    types.StructField('NAME', types.StringType()),
    types.StructField('LATITUDE', types.DoubleType()),
    types.StructField('LONGITUDE', types.DoubleType()),
    types.StructField('ELEVATION', types.DoubleType()),
    types.StructField('DATE', types.StringType()),
    types.StructField('DP01', types.IntegerType()),
    types.StructField('DSND', types.IntegerType()),
    types.StructField('DSNW', types.IntegerType()),
    types.StructField('DT00', types.IntegerType()),
    types.StructField('DT32', types.IntegerType()),
    types.StructField('DX70', types.IntegerType()),
    types.StructField('DX90', types.IntegerType()),
    types.StructField('DYFG', types.IntegerType()),
    types.StructField('DYTS', types.IntegerType()),
    types.StructField('EMNT', types.DoubleType()),
    types.StructField('EMSD', types.DoubleType()),
    types.StructField('EMSN', types.DoubleType()),
    types.StructField('EMXP', types.DoubleType()),
    types.StructField('EMXT', types.DoubleType()),
    types.StructField('EVAP', types.DoubleType()),
    types.StructField('PRCP', types.DoubleType()),
    types.StructField('SNOW', types.IntegerType()),
    types.StructField('TAVG', types.DoubleType()),
    types.StructField('TMAX', types.DoubleType()),
    types.StructField('TMIN', types.DoubleType()),
])


def main(in_directory):
    att = spark.read.option("header", "true").csv(in_directory, schema=weather_schema)
    df = att.select(
        att['STATION'],
        att['NAME'].alias("STATION_NAME"),
        att['LATITUDE'],
        att['LONGITUDE'],
        att['ELEVATION'],
        att['DATE'],
        att['DP01'],
        att['DSND'],
        att['DSNW'],
        att['DT00'],
        att['DT32'],
        att['DX70'],
        att['DX90'],
        att['DYFG'],
        att['DYTS'],
        att['EMNT'],
        att['EMSD'],
        att['EMSN'],
        att['EMXP'],
        att['EMXT'],
        att['EVAP'],
        att['PRCP'],
        att['SNOW'],
        att['TAVG'],
        att['TMAX'],
        att['TMIN'],
    )

    #TODO: Drop rows with null values
    # df = df.filter(df.DP01.startswith('null'))
    df.show(20);return
    # cachedDF = df.cache()

    # #Setting Latitude
    df = df.filter(df.LATITUDE >= 48.9)
    df = df.filter(df.LATITUDE <= 49.43)
    #
    # #Setting Longitude
    df = df.filter(df.LONGITUDE >= -123.3)
    df = df.filter(df.LONGITUDE <= -121.81)



    # df.show(); return

if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
