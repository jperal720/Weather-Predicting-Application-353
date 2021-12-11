from pyspark.sql import SparkSession, functions, types
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import RandomForestRegressor
from datetime import datetime
import sys
from pprint import pprint
import pandas as pd
import numpy as np
import glob
pd.set_option('display.max_columns', 500)

spark = SparkSession.builder.appName('test').getOrCreate()
spark.sparkContext.setLogLevel('WARN')

assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert spark.version >= '2.3'  # make sure we have Spark 2.3+

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

    # Thats when data is complete, has entries in every month for each year
    WHEATLAND = weatherData.filter(weatherData.STATION_NAME.startswith('WH')).filter(weatherData.DATE > '1953')
    CORVALLIS = weatherData.filter(weatherData.STATION_NAME.startswith('COR')).filter(weatherData.DATE > '1953')

    #Converting from spark to pandas dataframe
    pd_WHEATLAND = WHEATLAND.toPandas()
    pd_CORVALLIS = CORVALLIS.toPandas()

    #Creating Prediction dataframes and returning unlabelled dataframe and its results
    print("Creating model for WHEATLAND:\n")
    pd_df_prediction_WHEATLAND, U_WHEATLAND, U_WHEATLAND_Y = model_train_test(pd_WHEATLAND)
    print("Creating model for CORVALLIS:\n")
    pd_df_prediction_CORVALLIS, U_CORVALLIS, U_CORVALLIS_Y = model_train_test(pd_CORVALLIS)

    #Resetting indeces of validation dataframes
    U_WHEATLAND.reset_index(inplace=True, drop=True)
    U_WHEATLAND_Y.reset_index(inplace=True, drop=True)
    U_CORVALLIS.reset_index(inplace=True, drop=True)
    U_CORVALLIS_Y.reset_index(inplace=True, drop=True)

    #Appending actual values and predicted values to the unlabelled dataframe
    print("\nWHEATLAND Prediction Results (DATAFRAME):\n")
    SAMPLE_WHEATLAND = U_WHEATLAND
    SAMPLE_WHEATLAND['SNOW'] = U_WHEATLAND_Y['SNOW']
    SAMPLE_WHEATLAND['TAVG'] = U_WHEATLAND_Y['TAVG']
    SAMPLE_WHEATLAND['P_SNOW'] = pd_df_prediction_WHEATLAND['P_SNOW'].iloc[0:]
    SAMPLE_WHEATLAND['P_TAVG'] = pd_df_prediction_WHEATLAND['P_TAVG'].iloc[0:]

    print(SAMPLE_WHEATLAND)

    print("\nCORVALLIS Prediction Results (DATAFRAME):\n")
    SAMPLE_CORVALLIS = U_CORVALLIS
    SAMPLE_CORVALLIS['SNOW'] = U_CORVALLIS_Y['SNOW']
    SAMPLE_CORVALLIS['TAVG'] = U_CORVALLIS_Y['TAVG']
    SAMPLE_CORVALLIS['P_SNOW'] = pd_df_prediction_CORVALLIS['P_SNOW'].iloc[0:]
    SAMPLE_CORVALLIS['P_TAVG'] = pd_df_prediction_CORVALLIS['P_TAVG'].iloc[0:]

    print(SAMPLE_CORVALLIS)


def model_train_test(data_frame):
    X = pd.DataFrame()
    y = pd.DataFrame()

    # Selecting X Columns

    X['ELEVATION'] = data_frame['ELEVATION']
    X['DATE'] = data_frame['DATE']
    X['DP01'] = data_frame['DP01']
    X['DSNW'] = data_frame['DSNW']
    X['DT00'] = data_frame['DT00']
    X['DT32'] = data_frame['DT32']
    X['DX70'] = data_frame['DX70']
    X['DX90'] = data_frame['DX90']
    X['EMNT'] = data_frame['EMNT']
    X['EMSN'] = data_frame['EMSN']
    X['EMXP'] = data_frame['EMXP']
    X['EMXT'] = data_frame['EMXT']
    X['HTDD'] = data_frame['HTDD']
    X['PRCP'] = data_frame['PRCP']
    X['TMAX'] = data_frame['TMAX']
    X['TMIN'] = data_frame['TMIN']

    #Converting DATE(String) to datetime and dropping DATE
    X['DATE'] = X['DATE'].apply(lambda x: datetime.strptime(x, '%Y-%m'))
    X['MONTH'] = X['DATE'].dt.month
    X['YEAR'] = X['DATE'].dt.year
    X = X.drop(['DATE'], axis=1)

    #Reordering columns of X
    X = X[['YEAR', 'MONTH', 'DP01', 'DSNW', 'DT00', 'DT32',
           'DX70', 'DX90', 'EMNT', 'EMSN', 'EMXP', 'EMXT', 'HTDD', 'PRCP', 'TMAX', 'TMIN']]

    # Selecting y columns
    y['SNOW'] = data_frame['SNOW']
    y['TAVG'] = data_frame['TAVG']

    #Creating a data split
    X_train, X_valid, y_train, y_valid = train_test_split(X, y)

    #Creating a y_prediction dataframe
    df_prediction = fit_model(X_train, X_valid, y_train, y_valid)

    return df_prediction, X_valid, y_valid


def fit_model (X_train, X_valid, y_train, y_valid):
    rfc = RandomForestRegressor()
    rfc.fit(X_train, y_train)

    #Making predictions
    y_prediction = rfc.predict(X_valid)

    #Turning predictions into dataframe
    df_prediction = pd.DataFrame(y_prediction)
    df_prediction.columns = ['P_SNOW', 'P_TAVG']

    #Printing scores
    print("Train score:" , rfc.score(X_train, y_train))
    print("Validation Score:", rfc.score(X_valid, y_valid))

    return df_prediction

if __name__ == '__main__':
    in_directory = sys.argv[1]
    main(in_directory)
