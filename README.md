# Weather-Predicting-Application-353
An application that predicts the weather

1. Download the dataset we are using for the project:
https://www.ncei.noaa.gov/data/global-summary-of-the-month/archive/

2. Unzip the data into a folder (gsom-latest).

3. Copy and paste the **directory** in which the dataset is stored.

4. In your terminal, input **"spark-submit preprocess.py <gsom-latest directory>"** to run the preprocessing file that manipulates the raw data into usable data.
  
5. Once the preprocess.py file is done running, a folder called "updatedWeather2.csv" will be created: It contains the data our model will use.
  
6. In your terminal, input **"spark-submit weather_predict.py updatedWeather2.csv"**.
  
7. You will be displayed with a dataframe that has the the prediction made by the model -Snow precipitations, as P_SNOW, and Monthly average temperature, as TAVG.
