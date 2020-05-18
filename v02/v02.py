import os
import pandas as pd
import numpy as np

def euclidean_distance(v1, v2):
    distance = 0.0
    for i in range(len(v1)):
        distance += (v1[i] - v2[i])**2
    return np.sqrt(distance)

date = os.environ.get('ds')

# read data from BigQuery
sql = 'select * from `umg-data-science.detect_fraud_spinnup.v02_data` where _partitiondate = "{}" '.format(date)
df_all = pd.read_gbq(sql,dialect='standard', project_id='umg-data-science')
df = df_all[['report_date', 'isrc', 'repeater_streams_pct', 'focused_streams_pct', 'device_mobile_pct', 'device_pc_pct', 'device_other_pct']]


# replace null values in dataset with 0s and drop duplicates
df = df.fillna(0)
df = df.drop_duplicates()

# select numeric values
num_df = df.select_dtypes(include=np.number)

# select numeric column names
num_cols = num_df.columns.tolist()

groups_dict = {'f1_1': [0.9, 0.0, 0.0, 0.0, 1.0],
 'f1_2': [0.0, 0.0, 0.7, 0.1, 0.0],
 'f1_3': [0.0, 0.1, 0.0, 0.0, 1.0],
 'f1_4': [0.0, 0.0, 0.0, 0.9, 0.0],
 'f1_5': [0.9, 0.0, 0.9, 0.0, 0.0],
 'f1_6': [0.0, 0.8, 0.0, 0.0, 1.0],
 'f1_7': [0.9, 0.0, 0.0, 0.9, 0.0],
 'f2_1': [0.0, 0.5, 0.0, 0.0, 1.0],
 'f2_2': [0.0, 0.0, 0.5, 0.2, 0.0],
 'f2_3': [0.9, 0.0, 0.1, 0.3, 0.6],
 'f2_4': [0.0, 0.0, 0.1, 0.2, 0.7],
 'f2_5': [0.1, 0.0, 0.1, 0.6, 0.1],
 'f2_6': [0.0, 0.0, 1.0, 0.0, 0.0],
 'f2_7': [0.7, 0.4, 0.0, 0.0, 1.0],
 'f2_8': [0.5, 0.0, 0.9, 0.0, 0.0],
 'f2_9': [0.9, 0.0, 0.6, 0.2, 0.1],
 'f2_10': [0.0, 0.7, 0.0, 0.3, 0.7],
 'f2_11': [0.9, 0.0, 0.1, 0.0, 0.0],
 'f2_12': [0.0, 0.6, 0.0, 1.0, 0.0],
 'f2_13': [0.9, 0.0, 0.1, 0.7, 0.1],
 'f2_14': [0.4, 0.0, 0.0, 0.0, 1.0],
 'f3_1': [0.1, 0.0, 0.5, 0.2, 0.1],
 'f3_2': [0.2, 0.3, 0.0, 0.0, 0.9],
 'f3_3': [0.9, 0.0, 0.2, 0.3, 0.3],
 'f4_1': [0.3, 0.0, 0.9, 0.0, 0.0],
 'f4_2': [0.0, 0.6, 0.0, 0.0, 1.0],
 'f4_3': [0.7, 0.2, 0.0, 0.1, 0.9],
 'f4_4': [0.9, 0.0, 0.3, 0.4, 0.2],
 'f4_5': [0.1, 0.0, 0.1, 0.1, 0.8],
 'f4_6': [0.1, 0.0, 0.2, 0.4, 0.1],
 'f5_3': [0.1, 0.1, 0.0, 0.4, 0.5],
 'f5_4': [0.1, 0.1, 0.2, 0.7, 0.0],
 'f5_5': [0.0, 1.0, 0.0, 0.0, 1.0],
 'f5_6': [0.9, 0.0, 0.0, 0.6, 0.4],
 'f5_7': [0.0, 0.0, 0.9, 0.0, 0.0],
 'f5_8': [0.9, 0.0, 0.5, 0.1, 0.1],
 'f5_9': [0.2, 0.0, 0.0, 0.0, 1.0],
 'f5_10': [0.8, 0.6, 0.0, 0.0, 1.0],
 'f5_11': [0.7, 0.0, 0.9, 0.1, 0.0],
 'f5_12': [0.2, 0.0, 0.1, 0.0, 0.0],
 'f5_13': [0.1, 0.0, 0.3, 0.1, 0.5],
 'f5_14': [0.9, 0.0, 0.1, 0.1, 0.7],
 'f5_15': [0.8, 0.0, 0.3, 0.6, 0.0],
 'f5_16': [0.0, 0.5, 0.0, 0.0, 0.8],
 'f6_1': [0.6, 0.1, 0.0, 0.1, 0.9],
 'f6_2': [0.3, 0.0, 0.4, 0.2, 0.1],
 'f6_3': [0.9, 0.0, 0.1, 0.6, 0.2],
 'f6_4': [0.1, 0.1, 0.1, 0.3, 0.5]}

groups_dict2 =  {'f1_2': 'non-fraud',
 'f2_2': 'non-fraud',
 'f2_6': 'non-fraud',
 'f5_7': 'non-fraud',
 'f1_4': 'other-fraud',
 'f2_4': 'other-fraud',
 'f1_1': 'repeater',
 'f1_5': 'repeater',
 'f1_7': 'repeater',
 'f2_3': 'repeater',
 'f2_5': 'repeater',
 'f2_8': 'repeater',
 'f2_9': 'repeater',
 'f2_11': 'repeater',
 'f2_13': 'repeater',
 'f2_14': 'repeater',
 'f3_1': 'repeater',
 'f3_3': 'repeater',
 'f4_1': 'repeater',
 'f4_4': 'repeater',
 'f4_5': 'repeater',
 'f4_6': 'repeater',
 'f5_6': 'repeater',
 'f5_8': 'repeater',
 'f5_9': 'repeater',
 'f5_11': 'repeater',
 'f5_12': 'repeater',
 'f5_13': 'repeater',
 'f5_14': 'repeater',
 'f5_15': 'repeater',
 'f6_2': 'repeater',
 'f6_3': 'repeater',
 'f1_3': 'focused',
 'f1_6': 'focused',
 'f2_1': 'focused',
 'f2_10': 'focused',
 'f2_12': 'focused',
 'f4_2': 'focused',
 'f5_5': 'focused',
 'f5_16': 'focused',
 'f2_7': 'repeater-focused',
 'f3_2': 'repeater-focused',
 'f4_3': 'repeater-focused',
 'f5_3': 'repeater-focused',
 'f5_4': 'repeater-focused',
 'f5_10': 'repeater-focused',
 'f6_1': 'repeater-focused',
 'f6_4': 'repeater-focused'}

data = df
X = list(groups_dict.values())

# number of tracks
n_rows = data.shape[0]

# for every track compute distance to each one of the group centres
batch_size=1000
data_batch = [(n, min(n + batch_size, n_rows)) for n in range(0, n_rows, batch_size)]
values = []
for (start, stop) in data_batch:
    batch = data.iloc[start:stop,:]
    batch_num = batch.select_dtypes(include=np.number)
    for i in range(batch.shape[0]):
        row = []
        dist = [euclidean_distance(batch_num.iloc[i,:].values.tolist(), X[j]) for j in range(len(X))]
        row.append(batch.iat[i,0])
        row.append(batch.iat[i,1])
        row.extend(dist)
        values.append(row)

c = ['report_date', 'isrc']
c.extend(list(groups_dict.keys()))
scores = pd.DataFrame(values, columns=c)

# classify tracks
classes = scores.select_dtypes(include=np.number).idxmin(axis=1)
scores['class'] = classes.values.tolist()

p_series = pd.Series(groups_dict2)
p_series.name = 'category'
results = pd.merge(scores[['report_date', 'isrc', 'class']], p_series, left_on='class', right_index=True, how='left', sort=False)

# save results to CSV
results.to_csv('v02_results.csv', index=False, header=False)