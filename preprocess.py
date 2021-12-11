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
        att['LATITUDE'].cast(types.DoubleType()),
        att['LONGITUDE'].cast(types.DoubleType()),
        att['ELEVATION'].cast(types.DoubleType()),
        att['DATE'],
        att['CLDD'].cast(types.IntegerType()),
        att['DP01'].cast(types.IntegerType()),
        att['DSNW'].cast(types.IntegerType()),
        att['DT00'].cast(types.IntegerType()),
        att['DT32'].cast(types.IntegerType()),
        att['DX70'].cast(types.IntegerType()),
        att['DX90'].cast(types.IntegerType()),
        att['EMNT'].cast(types.DoubleType()),
        att['EMSN'].cast(types.DoubleType()),
        att['EMXP'].cast(types.DoubleType()),
        att['EMXT'].cast(types.DoubleType()),
        att['HTDD'].cast(types.DoubleType()),
        att['PRCP'].cast(types.DoubleType()),
        att['SNOW'].cast(types.DoubleType()),
        att['TAVG'].cast(types.DoubleType()),
        att['TMAX'].cast(types.DoubleType()),
        att['TMIN'].cast(types.DoubleType()),
    )


    df = df.filter(df['STATION'].isNotNull() &
        df["STATION_NAME"].isNotNull() &
        df['LATITUDE'].isNotNull() &
        df['LONGITUDE'].isNotNull() &
        df['ELEVATION'].isNotNull() &
        df['DATE'].isNotNull() &
        df['CLDD'].isNotNull() &
        df['DP01'].isNotNull() &
        df['DSNW'].isNotNull() &
        df['DT00'].isNotNull() &
        df['DT32'].isNotNull() &
        df['DX70'].isNotNull() &
        df['DX90'].isNotNull() &
        df['EMNT'].isNotNull() &
        df['EMSN'].isNotNull() &
        df['EMXP'].isNotNull() &
        df['EMXT'].isNotNull() &
        df['HTDD'].isNotNull() &
        df['PRCP'].isNotNull() &
        df['SNOW'].isNotNull() &
        df['TAVG'].isNotNull() &
        df['TMAX'].isNotNull() &
        df['TMIN'].isNotNull())

    df_cache = df.cache()

    #Convert units (inch -> mm, °F -> °C)
    df_cache = df_cache.withColumn("EMNT_CONVERTED", udf_f_to_c(df_cache['EMNT']))
    df_cache = df_cache.withColumn("EMSN_CONVERTED", udf_in_to_mm(df_cache['EMSN']))
    df_cache = df_cache.withColumn("EMXP_CONVERTED", udf_in_to_mm(df_cache['EMXP']))
    df_cache = df_cache.withColumn("EMXT_CONVERTED", udf_f_to_c(df_cache['EMXT']))
    df_cache = df_cache.withColumn("PRCP_CONVERTED", udf_in_to_mm(df_cache['PRCP']))
    df_cache = df_cache.withColumn("SNOW_CONVERTED", udf_in_to_mm(df_cache['SNOW']))
    df_cache = df_cache.withColumn("TAVG_CONVERTED", udf_f_to_c(df_cache['TAVG']))
    df_cache = df_cache.withColumn("TMAX_CONVERTED", udf_f_to_c(df_cache['TMAX']))
    df_cache = df_cache.withColumn("TMIN_CONVERTED", udf_f_to_c(df_cache['TMIN']))
    df_cache = df_cache.withColumn("HTDD_CONVERTED", udf_f_to_c(df_cache['HTDD']))


    df_converted = df_cache.select(
        df_cache['STATION'],
        df_cache['STATION_NAME'],
        df_cache['LATITUDE'],
        df_cache['LONGITUDE'],
        df_cache['ELEVATION'],
        df_cache['DATE'],
        df_cache['DP01'],
        df_cache['DSNW'],
        df_cache['DT00'],
        df_cache['DT32'],
        df_cache['DX70'],
        df_cache['DX90'],
        df_cache['EMNT_CONVERTED'].alias('EMNT'),
        df_cache['EMSN_CONVERTED'].alias('EMSN'),
        df_cache['EMXP_CONVERTED'].alias('EMXP'),
        df_cache['EMXT_CONVERTED'].alias('EMXT'),
        df_cache['HTDD_CONVERTED'].alias('HTDD'),
        df_cache['PRCP_CONVERTED'].alias('PRCP'),
        df_cache['SNOW_CONVERTED'].alias('SNOW'),
        df_cache['TAVG_CONVERTED'].alias('TAVG'),
        df_cache['TMAX_CONVERTED'].alias('TMAX'),
        df_cache['TMIN_CONVERTED'].alias('TMIN'),
    )
    spark.catalog.clearCache()
    df_converted.show()


    # df_converted = df_converted.cache()
    # #Setting Latitude
    # df_converted = df_converted.filter(df.LATITUDE >= 48.9)
    # df_converted = df_converted.filter(df.LATITUDE <= 49.43)
    # #
    # # #Setting Longitude
    # df_converted = df_converted.filter(df.LONGITUDE >= -123.3)
    # df_converted = df_converted.filter(df.LONGITUDE <= -121.81)




    # spark.catalog.clearCache()
    df_converted.write.option("header", "true").csv('updatedWeather2.csv')
    # df_converted.write.save("/Users/jonathanperalgort/Documents/df_converted")


def f_to_c(input):
    if(input is None):
        return None
    else:
        return round((input - 32) * (5 / 9), 4)

def in_to_mm(input):
    if(input is None):
        return None
    else:
        return round(input * 25.4, 4)

udf_f_to_c = functions.udf(f_to_c, returnType=types.DoubleType())
udf_in_to_mm = functions.udf(in_to_mm, returnType=types.DoubleType())

if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
