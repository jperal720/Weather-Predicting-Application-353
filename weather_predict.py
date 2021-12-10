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


def main(in_directory):
    att = spark.read.option("header", "true").csv(in_directory)
    df = att.select(
        att['STATION'],
        att['NAME'].alias("STATION_NAME"),
        att['LATITUDE'],
        att['LONGITUDE'],
        att['ELEVATION'],
        att['DATE'],
        att['CLDD'],
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
        att['HTDD'],
        att['HN01'],
        att['HX01'],
        att['LN01'],
        att['LX01'],
        att['MN01'],
        att['MX01'],
        att['PRCP'],
        att['SNOW'],
        att['TAVG'],
        att['TMAX'],
        att['TMIN'],
    )
    # df = df.cache()

    # #Setting Latitde
    df = df.filter(df.LATITUDE >= 48.9)
    df = df.filter(df.LATITUDE <= 49.43)
    #
    # #Setting Longitude
    df = df.filter(df.LONGITUDE >= -123.3)
    # df = df.filter(df.LONGITUDE <= -121.81)

    #Dropping columns with null values
    df = df.dropna()
    df.show(2000)

    # df.show(); return

if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
