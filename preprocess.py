import sys
import pandas as pd
import zipfile
from multiprocessing import Pool
pool = Pool(8) # number of cores you want to use


#adpated from https://stackoverflow.com/questions/56786321/read-multiple-csv-files-zipped-in-one-file
filename = sys.argv[1]
zf = zipfile.ZipFile(filename)

dfs = [pd.read_csv(zf.open(f)) for f in zf.namelist()[1:]]

df = pd.concat(dfs,ignore_index=True)
df = df.dropna()
df.to_csv('weatherTogether.csv')

df.to_csv('weatherTogether.csv')
#takes 2m08

