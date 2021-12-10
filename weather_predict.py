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


# weather_schema = types.StructType([
#     types.StructField('STATION', types.StringType()),
#     types.StructField('DATE', types.StringType()),
#     types.StructField('LATITUDE', types.DoubleType()),
#     types.StructField('LONGITUDE', types.DoubleType()),
#     types.StructField('ELEVATION', types.DoubleType()),
#     types.StructField('NAME', types.StringType()),
#     types.StructField('CDSD', types.StringType()),
#     types.StructField('CDSD_ATTRIBUTES', types.StringType()),
#     types.StructField('CLDD', types.DoubleType()),
#     types.StructField('CLDD_ATTRIBUTES', types.StringType()),
#     types.StructField('DP01', types.DoubleType()),
#     types.StructField('DP01_ATTRIBUTES', types.StringType()),
#     types.StructField('DP10', types.LongType()),
#     types.StructField('DP10_ATTRIBUTES', types.StringType()),
#     types.StructField('DP1X', types.LongType()),
#     types.StructField('DP1X_ATTRIBUTES', types.StringType()),
#     types.StructField('DSND', types.LongType()),
#     types.StructField('DSND_ATTRIBUTES', types.StringType()),
#     types.StructField('DSNW', types.LongType()),
#     types.StructField('DSNW_ATTRIBUTES', types.StringType()),
#     types.StructField('DT00', types.LongType()),
#     types.StructField('DT00_ATTRIBUTES ', types.StringType()),
#     types.StructField('DT32', types.LongType()),
#     types.StructField('DT32_ATTRIBUTES', types.StringType()),
#     types.StructField('DX32', types.LongType()),
#     types.StructField('DX32_ATTRIBUTES', types.StringType()),
#     types.StructField('DX70', types.LongType()),
#     types.StructField('DX70_ATTRIBUTES', types.StringType()),
#     types.StructField('DX90', types.LongType()),
#     types.StructField('DX90_ATTRIBUTES', types.StringType()),
#     types.StructField('DYFG', types.LongType()),
#     types.StructField('DYHF', types.LongType()),
#     types.StructField('DYTS', types.LongType()),
#     types.StructField('EMNT', types.DoubleType()),
#     types.StructField('EMNT_ATTRIBUTES', types.StringType()),
#     types.StructField('EMSD', types.LongType()),
#     types.StructField('EMSD_ATTRIBUTES', types.StringType()),
#     types.StructField('EMSN', types.LongType()),
#     types.StructField('EMSN_ATTRIBUTES', types.StringType()),
#     types.StructField('EMXP', types.LongType()),
#     types.StructField('EMXP_ATTRIBUTES', types.StringType()),
#     types.StructField('EMXT', types.DoubleType()),
#     types.StructField('EMXT_ATTRIBUTES', types.StringType()),
#     types.StructField('EVAP', types.DoubleType()),
#     types.StructField('EVAP_ATTRIBUTES', types.StringType()),
#     types.StructField('HDSD', types.DoubleType()),
#     types.StructField('HDSD_ATTRIBUTES', types.StringType()),
#     types.StructField('HNN01', types.DoubleType()),
#     types.StructField('HN01_ATTRIBUTES', types.StringType()),
#     types.StructField('HNN02', types.DoubleType()),
#     types.StructField('HN02_ATTRIBUTES', types.StringType()),
#     types.StructField('HNN03', types.DoubleType()),
#     types.StructField('HN03_ATTRIBUTES', types.StringType()),
#     types.StructField('HNN04', types.DoubleType()),
#     types.StructField('HN04_ATTRIBUTES', types.StringType()),
#     types.StructField('HTDD', types.DoubleType()),
#     types.StructField('HTDD_ATTRIBUTES', types.StringType()),
#     types.StructField('HX01', types.DoubleType()),
#     types.StructField('HX01_ATTRIBUTES', types.StringType()),
#     types.StructField('HX02', types.DoubleType()),
#     types.StructField('HX02_ATTRIBUTES', types.StringType()),
#     types.StructField('HX03', types.DoubleType()),
#     types.StructField('HX03_ATTRIBUTES', types.StringType()),
#     types.StructField('HX04', types.DoubleType()),
#     types.StructField('HX04_ATTRIBUTES', types.StringType()),
#     types.StructField('HX05', types.DoubleType()),
#     types.StructField('HX05_ATTRIBUTES', types.StringType()),
#     types.StructField('LNN01', types.DoubleType()),
#     types.StructField('LNN01_ATTRIBUTES', types.StringType()),
#     types.StructField('LNN02', types.DoubleType()),
#     types.StructField('LNN02_ATTRIBUTES', types.StringType()),
#     types.StructField('LNN03', types.DoubleType()),
#     types.StructField('LNN03_ATTRIBUTES', types.StringType()),
#     types.StructField('LNN04', types.DoubleType()),
#     types.StructField('LNN04_ATTRIBUTES', types.StringType()),
#     types.StructField('LX01', types.DoubleType()),
#     types.StructField('LX01_ATTRIBUTES', types.StringType()),
#     types.StructField('LX02', types.DoubleType()),
#     types.StructField('LX02_ATTRIBUTES', types.StringType()),
#     types.StructField('LX03', types.DoubleType()),
#     types.StructField('LX03_ATTRIBUTES', types.StringType()),
#     types.StructField('LX04', types.DoubleType()),
#     types.StructField('LX04_ATTRIBUTES', types.StringType()),
#     types.StructField('LX05', types.DoubleType()),
#     types.StructField('LX05_ATTRIBUTES', types.StringType()),
#     types.StructField('MN01', types.DoubleType()),
#     types.StructField('MN01_ATTRIBUTES', types.StringType()),
#     types.StructField('MN02', types.DoubleType()),
#     types.StructField('MN02_ATTRIBUTES', types.StringType()),
#     types.StructField('MN03', types.DoubleType()),
#     types.StructField('MN03_ATTRIBUTES', types.StringType()),
#     types.StructField('MN04', types.DoubleType()),
#     types.StructField('MN04_ATTRIBUTES', types.StringType()),
#     types.StructField('MX01', types.DoubleType()),
#     types.StructField('MX01_ATTRIBUTES', types.StringType()),
#     types.StructField('MX02', types.DoubleType()),
#     types.StructField('MX02_ATTRIBUTES', types.StringType()),
#     types.StructField('MX03', types.DoubleType()),
#     types.StructField('MX03_ATTRIBUTES', types.StringType()),
#     types.StructField('MX04', types.DoubleType()),
#     types.StructField('MX04_ATTRIBUTES', types.StringType()),
#     types.StructField('MX05', types.DoubleType()),
#     types.StructField('MX05_ATTRIBUTES', types.StringType()),
#     types.StructField('MXPN', types.DoubleType()),
#     types.StructField('MXPN_ATTRIBUTES', types.StringType()),
#     types.StructField('PRCP', types.DoubleType()),
#     types.StructField('PRCP_ATTRIBUTES', types.StringType()),
#     types.StructField('SNOW', types.DoubleType()),
#     types.StructField('SNOW_ATTRIBUTES', types.StringType()),
#     types.StructField('TAVG', types.DoubleType()),
#     types.StructField('TAVG_ATTRIBUTES', types.StringType()),
#     types.StructField('TMAX', types.DoubleType()),
#     types.StructField('TMAX_ATTRIBUTES', types.StringType()),
#     types.StructField('TMIN', types.DoubleType()),
#     types.StructField('TMIN_ATTRIBUTES', types.StringType()),
#     types.StructField('WDMV', types.DoubleType()),
#     types.StructField('WDMV_ATTRIBUTES', types.StringType()),
    
# ])


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
    df_cache = df.cache()

    #Convert units (inch -> mm, °F -> °C)
    df_cache = df_cache.withColumn("EMNT_CONVERTED", udf_f_to_c(df_cache['EMNT']))
    df_cache = df_cache.withColumn("EMSD_CONVERTED", udf_in_to_mm(df_cache['EMSD']))
    df_cache = df_cache.withColumn("EMSN_CONVERTED", udf_in_to_mm(df_cache['EMSN']))
    df_cache = df_cache.withColumn("EMXP_CONVERTED", udf_in_to_mm(df_cache['EMXP']))
    df_cache = df_cache.withColumn("EMXT_CONVERTED", udf_f_to_c(df_cache['EMXT']))
    df_cache = df_cache.withColumn("EVAP_CONVERTED", udf_in_to_mm(df_cache['EVAP']))
    df_cache = df_cache.withColumn("PRCP_CONVERTED", udf_in_to_mm(df_cache['PRCP']))
    df_cache = df_cache.withColumn("SNOW_CONVERTED", udf_in_to_mm(df_cache['SNOW']))
    df_cache = df_cache.withColumn("TAVG_CONVERTED", udf_f_to_c(df_cache['TAVG']))
    df_cache = df_cache.withColumn("TMAX_CONVERTED", udf_f_to_c(df_cache['TMAX']))
    df_cache = df_cache.withColumn("TMIN_CONVERTED", udf_f_to_c(df_cache['TMIN']))


    df_converted = df_cache.select(
        df_cache['STATION'],
        df_cache['STATION_NAME'],
        df_cache['LATITUDE'],
        df_cache['LONGITUDE'],
        df_cache['ELEVATION'],
        df_cache['DATE'],
        df_cache['DP01'],
        df_cache['DSND'],
        df_cache['DSNW'],
        df_cache['DT00'],
        df_cache['DT32'],
        df_cache['DX70'],
        df_cache['DX90'],
        df_cache['DYFG'],
        df_cache['DYTS'],
        df_cache['EMNT_CONVERTED'].alias('EMNT'),
        df_cache['EMSD_CONVERTED'].alias('EMSD'),
        df_cache['EMSN_CONVERTED'].alias('EMSN'),
        df_cache['EMXP_CONVERTED'].alias('EMXP'),
        df_cache['EMXT_CONVERTED'].alias('EMXT'),
        df_cache['EVAP_CONVERTED'].alias('EVAP'),
        df_cache['PRCP_CONVERTED'].alias('PRCP'),
        df_cache['SNOW_CONVERTED'].alias('SNOW'),
        df_cache['TAVG_CONVERTED'].alias('TAVG'),
        df_cache['TMAX_CONVERTED'].alias('TMAX'),
        df_cache['TMIN_CONVERTED'].alias('TMIN'),
    )
    spark.catalog.clearCache()
    # df_converted.show()

    # TODO: Drop rows with null values

    df_converted = df_converted.cache()
    # #Setting Latitude
    df_converted = df_converted.filter(df.LATITUDE >= 48.9)
    df_converted = df_converted.filter(df.LATITUDE <= 49.43)
    #
    # #Setting Longitude
    df_converted = df_converted.filter(df.LONGITUDE >= -123.3)
    df_converted = df_converted.filter(df.LONGITUDE <= -121.81)

    spark.catalog.clearCache()
    df_converted.show()
    # df_converted.write.save("/Users/jonathanperalgort/Documents/df_converted")


def f_to_c(input):
    if(input is None):
        return None
    else:
        return round((input - 32) * 5 / 9, 4)

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
