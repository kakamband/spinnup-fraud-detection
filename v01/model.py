import os
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

# drops low variance features based on a variance threshold
def drop_low_var(df, thres):
    columns_to_drop = []
    for column in df.select_dtypes(include=np.number).columns:
        if abs(df[column].var()) < thres:
            columns_to_drop.append(column)
    df = df.drop(columns=columns_to_drop)
    return df

# identifies and drops skewed features; creates new log-normalised features (optional)
def drop_skewed(df, log=True):
    skew = df.select_dtypes(include=np.number).skew(axis=0)
    skewed_cols = list(skew[(abs(skew)>3)].index)
    if log:
        for c in skewed_cols:
            df[c+'_log'] = np.log(df[c]+1)  
    return df.drop(columns=skewed_cols)

# computes the euclidean distance between two 1D vectors
def euclidean_distance(v1, v2):
    distance = 0.0
    for i in range(len(v1)-1):
        distance += (v1[i] - v2[i])**2
    return np.sqrt(distance)

date = os.environ.get('ds')

# read data from BigQuery
sql = 'select * from `umg-data-science.detect_fraud_spinnup.f1_tracks` where _partitiondate = "{}" '.format(date)
df = pd.read_gbq(sql,dialect='standard', project_id='umg-data-science')

# replace null values in dataset with 0s
df = df.fillna(0)

# drop columns with variance very close to 0
df = drop_low_var(df, 0.01)

# replace numeric skewed columns with log-normalised ones
df = drop_skewed(df)

# select numeric values
num_df = df.select_dtypes(include=np.number)

# select numeric column names
num_cols = num_df.columns.tolist()

# scale numeric data using standardization (z-score transformation)
s = StandardScaler().fit_transform(num_df)

# make dataframe with date, ids and standardized values
sdf = pd.DataFrame(s, columns=num_cols)
sdf = pd.concat([df[['report_date', 'isrc']], sdf], axis=1)
sdf = sdf.drop(columns=['pct_fraud_streams', 'streams_log', 'users_log'])

# define different track group centres (this is based on previously inspected KMeans clusters)
not_fraud = [0, 0, 0.7, 0.3, 200, 0, 0.5, 100, 0, 0.5, 1, 0, 0, 0]
fraud_1 = [0, 1, 1, 0, 180, 0.9, 0.5, 90, 0, 0, 0, 0.1, 0.9, 0]
fraud_2 = [0, 1, 1, 0, 90, 0.9, 1, 90, 0, 1, 0, 0.5, 0.5, 3]
fraud_3 = [1, 0, 1, 0, 90, 0.9, 1, 90, 0, 1, 0, 0.1, 0.9, 5]
fraud_4 = [1, 1, 1, 0, 70, 0.9, 0.5, 35, 1, 0, 0, 0.5, 0.5, 5]
fraud_5 = [0, 1, 0, 1, 180, 0.9, 0.25, 45, 1, 0, 0, 0.1, 0.9, 0]
fraud_6 = [0.5, 0.5, 0, 1, 180, 0, 1, 180, 0, 1, 0, 0.5, 0.5, 3]

# create dataframe with track group centres
data = [not_fraud, fraud_1, fraud_2, fraud_3, fraud_4, fraud_5, fraud_6]
cols = sdf.select_dtypes(include=np.number).columns.tolist()
df2 = pd.DataFrame(data, columns = cols)

# create standardized version of dataframe
sdf2 = pd.DataFrame()
for column in cols:
    sdf2[column] = (df2[column]-num_df[column].mean())/num_df[column].std()

# initialise progress bar
n_rows = sdf.shape[0]

# for every track compute distance to each one of the group centres
batch_size=100
data_batch = [(n, min(n + batch_size, n_rows)) for n in range(0, n_rows, batch_size)]
date = df.report_date.astype(str)[0]
values = []
for (start, stop) in data_batch:
    batch = sdf.iloc[start:stop,:]
    batch_num = batch.select_dtypes(include=np.number)
    for i in range(batch.shape[0]):
        row = []
        dist = [euclidean_distance(batch_num.iloc[i,:].values.tolist(), sdf2.iloc[j,:].values.tolist()) for j in range(sdf2.shape[0])]
        row.append(date)
        row.append(batch.iat[i,1])
        row.extend(dist)
        values.append(row)
            
scores = pd.DataFrame(values, columns=['report_date', 'isrc', 'nf', 'f1', 'f2', 'f3', 'f4', 'f5', 'f6'])

# filter the tracks where the minimum (at row level) is lower than 3 (this threshold can be chosen by looking at the distribution of all min values) 
scores_filtered = scores[scores.min(axis=1)<=3]

# classify tracks
classes = scores_filtered.select_dtypes(include=np.number).idxmin(axis=1)
scores_filtered['class'] = classes.values.tolist()

# make minimum distance the final score column
minimums = scores_filtered.select_dtypes(include=np.number).min(axis=1)
scores_filtered['score'] = minimums.values.tolist()

# save results to CSV
scores_filtered.to_csv('v01_results.csv', index=False, header=False)