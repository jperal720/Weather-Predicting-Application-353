# Weather-Predicting-Application-353
An application that predicts the weather

1. Download the dataset we are using for the project:
https://www.ncei.noaa.gov/data/global-summary-of-the-month/archive/

2. Unzip the data into a folder (gsom-latest).

3. Copy and paste the **directory** in which the dataset is stored.

4. In your terminal, input **"spark-submit preprocess.py <gsom-latest directory>"** to run the preprocessing file that manipulates the raw data into usable data.
  
5. Once the preprocess.py file is done running, a folder called "updatedWeather2.csv" will be created: It contains the data our model will use.
  
6. In your terminal, input **"spark-submit weather_predict.py updatedWeather2.csv"**.
  
7. You will be displayed with two dataframes: One includes data from a weather station in Wheatland, and the other from Corvallis. The last two columns will be the predictions made by the model -Snow precipitations, as P_SNOW, and Monthly average temperature, as P_TAVG. The differences between SNOW/TAVG (the actual values) and P_SNOW and P_TAVG can be easily observed.
