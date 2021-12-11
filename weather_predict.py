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
    types.StructField('STATION_NAME', types.StringType()),
    types.StructField('LATITUDE', types.DoubleType()),
    types.StructField('LONGITUDE', types.DoubleType()),
    types.StructField('ELEVATION', types.DoubleType()),
    types.StructField('DATE', types.StringType()),
    types.StructField('DP01', types.IntegerType()),
    types.StructField('DSNW', types.IntegerType()),
    types.StructField('DT00', types.IntegerType()),
    types.StructField('DT32', types.IntegerType()),
    types.StructField('DX70', types.IntegerType()),
    types.StructField('DX90', types.IntegerType()),
    types.StructField('EMNT', types.DoubleType()),
    types.StructField('EMSN', types.DoubleType()),
    types.StructField('EMXP', types.DoubleType()),
    types.StructField('EMXT', types.DoubleType()),
    types.StructField('HTDD', types.DoubleType()),
    types.StructField('PRCP', types.DoubleType()),
    types.StructField('SNOW', types.DoubleType()),
    types.StructField('TAVG', types.DoubleType()),
    types.StructField('TMAX', types.DoubleType()),
    types.StructField('TMIN', types.DoubleType()),
])


def main(in_directory):
    weatherData = spark.read.option("header", "true").csv(in_directory, schema=weather_schema)
    weatherData = weatherData.cache()

    #Thats when data is complete, has entries in every month for each year
    WHEATLAND  = weatherData.filter(weatherData.STATION_NAME.startswith('WH')).filter(weatherData.DATE > '1953')
    CORVALLIS = weatherData.filter(weatherData.STATION_NAME.startswith('COR')).filter(weatherData.DATE > '1953')
    
    WHEATLAND = WHEATLAND.withColumn('DATE', udf_convertDate("DATE"))
    CORVALLIS = CORVALLIS.withColumn('DATE', udf_convertDate("DATE"))

    WHEATLAND.show()
    CORVALLIS.show()

    # print(weatherData.dropDuplicates(['STATION_NAME']).select(functions.collect_list('STATION_NAME')).first()[0])
    


def convertDate(input):
    if(input is None):
        return None
    else:
        return input[0:4]

udf_convertDate = functions.udf(convertDate, returnType=types.StringType())




if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
