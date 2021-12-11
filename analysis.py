from pyspark.sql import SparkSession, functions, types
import sys
from pprint import pprint
import pandas as pd
import numpy as np
import glob
from statsmodels.nonparametric.smoothers_lowess import lowess
import matplotlib.pyplot as plt
from scipy import stats
import seaborn as sns
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
    sns.set()
    weatherData = spark.read.option("header", "true").csv(in_directory, schema=weather_schema)
    weatherData = weatherData.cache()

    #Thats when data is complete, has entries in every month for each year
    WHEATLAND  = weatherData.filter(weatherData.STATION_NAME.startswith('WH')).filter(weatherData.DATE > '1953').sort('DATE')
    WHEATLAND = WHEATLAND.cache()
    CORVALLIS = weatherData.filter(weatherData.STATION_NAME.startswith('COR')).filter(weatherData.DATE > '1953').sort('DATE')
    CORVALLIS = CORVALLIS.cache()

  


    WHEATLAND = WHEATLAND.withColumn('DATE', udf_convertDate("DATE"))
    CORVALLIS = CORVALLIS.withColumn('DATE', udf_convertDate("DATE"))

    groupsWHEATLAND = WHEATLAND.groupBy('DATE').agg(
        functions.mean('PRCP').alias('YEARLY_AVG_PRCP'),
        functions.mean('SNOW').alias('YEARLY_AVG_SNOW'),
        functions.mean('TAVG').alias('YEARLY_AVG_TAVG'),
        functions.mean('TMAX').alias('YEARLY_AVG_TMAX'),
        functions.mean('TMIN').alias('YEARLY_AVG_TMIN'),
    ).sort('date').toPandas()

    
    #plotting results
    # plt.plot(groupsWHEATLAND['DATE'], groupsWHEATLAND['YEARLY_AVG_TAVG'],'b-',alpha=0.5 )

    #PLOTTTING TEMPERATURES
    filteredTAVG = lowess(groupsWHEATLAND['YEARLY_AVG_TAVG'], groupsWHEATLAND['DATE'] , frac=0.2)
    filteredAVGTMAX = lowess(groupsWHEATLAND['YEARLY_AVG_TMAX'], groupsWHEATLAND['DATE'] , frac=0.2)
    filteredAVGTMIN = lowess(groupsWHEATLAND['YEARLY_AVG_TMIN'], groupsWHEATLAND['DATE'] , frac=0.2)
    #plotting results
    plt.figure(0)
    plt.plot(filteredTAVG[:, 0], filteredTAVG[:, 1],'b-',linewidth = 2 )
    plt.plot(filteredAVGTMAX[:, 0], filteredAVGTMAX[:, 1],'r-',linewidth = 2 )
    plt.plot(filteredAVGTMIN[:, 0], filteredAVGTMIN[:, 1],'g-',linewidth = 2 )
    plt.legend(['Yearly Average Temperature','Yearly Average Max Temperature Of Months', 'Yearly Average Min Temperature Of Months'],loc='upper right')
    plt.xlabel('Year')
    plt.xticks(fontsize=6)
    plt.ylabel('Temperature °C')
    plt.show()

    # print(weatherData.dropDuplicates(['STATION_NAME']).select(functions.collect_list('STATION_NAME')).first()[0])
    
    filteredPRCP = lowess(groupsWHEATLAND['YEARLY_AVG_PRCP'], groupsWHEATLAND['DATE'] , frac=0.2)
    filteredSNOW = lowess(groupsWHEATLAND['YEARLY_AVG_SNOW'], groupsWHEATLAND['DATE'] , frac=0.2)

    #SNOW FALL, PRECIPITATION FALL
    plt.figure(1)
    plt.plot(filteredPRCP[:, 0], filteredPRCP[:, 1] ,'b-',linewidth = 2 )
    plt.plot(filteredSNOW[:, 0], filteredSNOW[:, 1] ,'y-',linewidth = 2 )
    plt.legend(['Yearly Average Precipitation','Yearly Average Snow Fall '],loc='upper right')
    plt.xlabel('Year')
    plt.xticks(fontsize=6)
    plt.ylabel('milliliters (ml)')
    plt.show()



    groupsCORVALLIS = CORVALLIS.groupBy('DATE').agg(
        functions.mean('PRCP').alias('YEARLY_AVG_PRCP'),
        functions.mean('SNOW').alias('YEARLY_AVG_SNOW'),
        functions.mean('TAVG').alias('YEARLY_AVG_TAVG'),
        functions.mean('TMAX').alias('YEARLY_AVG_TMAX'),
        functions.mean('TMIN').alias('YEARLY_AVG_TMIN'),
    ).sort('date').toPandas()

    
    #plotting results
    # plt.plot(groupsWHEATLAND['DATE'], groupsWHEATLAND['YEARLY_AVG_TAVG'],'b-',alpha=0.5 )


    #PLOTTTING TEMPERATURES
    filteredTAVG = lowess(groupsCORVALLIS['YEARLY_AVG_TAVG'], groupsCORVALLIS['DATE'] , frac=0.2)
    filteredAVGTMAX = lowess(groupsCORVALLIS['YEARLY_AVG_TMAX'], groupsCORVALLIS['DATE'] , frac=0.2)
    filteredAVGTMIN = lowess(groupsCORVALLIS['YEARLY_AVG_TMIN'], groupsCORVALLIS['DATE'] , frac=0.2)
    #plotting results
    plt.figure(2)
    plt.plot(filteredTAVG[:, 0], filteredTAVG[:, 1],'b-',linewidth = 2 )
    plt.plot(filteredAVGTMAX[:, 0], filteredAVGTMAX[:, 1],'r-',linewidth = 2 )
    plt.plot(filteredAVGTMIN[:, 0], filteredAVGTMIN[:, 1],'g-',linewidth = 2 )
    plt.legend(['Yearly Average Temperature','Yearly Average Max Temperature Of Months', 'Yearly Average Min Temperature Of Months'],loc='upper right')
    plt.xlabel('Year')
    plt.xticks(fontsize=6)
    plt.ylabel('Temperature °C')
    plt.show()
    
    # print(weatherData.dropDuplicates(['STATION_NAME']).select(functions.collect_list('STATION_NAME')).first()[0])
    
    filteredPRCP = lowess(groupsCORVALLIS['YEARLY_AVG_PRCP'], groupsCORVALLIS['DATE'] , frac=0.2)
    filteredSNOW = lowess(groupsCORVALLIS['YEARLY_AVG_SNOW'], groupsCORVALLIS['DATE'] , frac=0.2)

    #SNOW FALL, PRECIPITATION FALL
    plt.figure(3)
    plt.plot(filteredPRCP[:, 0], filteredPRCP[:, 1] ,'b-',linewidth = 2 )
    plt.plot(filteredSNOW[:, 0], filteredSNOW[:, 1] ,'y-',linewidth = 2 )
    plt.legend(['Yearly Average Precipitation','Yearly Average Snow Fall '],loc='upper right')
    plt.xlabel('Year')
    plt.xticks(fontsize=6)
    plt.ylabel('milliliters (ml)')
    plt.show()
    


    wPast50 = groupsWHEATLAND[groupsWHEATLAND.DATE > '1953']
    wPast50 = wPast50[wPast50.DATE <= '1999']['YEARLY_AVG_TAVG']
    w21Century = groupsWHEATLAND[ groupsWHEATLAND.DATE > '1999']['YEARLY_AVG_TAVG']



    cPast50 = groupsCORVALLIS[groupsCORVALLIS.DATE > '1953' ]
    cPast50 = cPast50[cPast50.DATE <= '1999']['YEARLY_AVG_TAVG']
    c21Century = groupsCORVALLIS[ groupsCORVALLIS.DATE > '1999']['YEARLY_AVG_TAVG']

    ttest = stats.ttest_ind(cPast50, c21Century)
    print(ttest,'\n')
    print(stats.normaltest(cPast50).pvalue,'\n')
    print(stats.normaltest(c21Century).pvalue)
    print(stats.levene(cPast50,c21Century).pvalue)
    #do not have same equal variances
    #so do man whitney

    print(stats.mannwhitneyu(cPast50,c21Century).pvalue)


    ttest = stats.ttest_ind(wPast50, w21Century)
    print(ttest,'\n')
    print(stats.normaltest(wPast50).pvalue,'\n')
    print(stats.normaltest(w21Century).pvalue)
    print(stats.levene(wPast50, w21Century).pvalue)
    #do not have same equal variances
    #so do man whitney

    print(stats.mannwhitneyu(wPast50, w21Century).pvalue)

def convertDate(input):
    if(input is None):
        return None
    else:
        return input[0:4]

udf_convertDate = functions.udf(convertDate, returnType=types.StringType())




if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
