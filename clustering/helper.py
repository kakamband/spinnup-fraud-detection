import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn import metrics 
from scipy.spatial.distance import cdist 
import seaborn as sns
import matplotlib.pyplot as plt
from math import sqrt

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

# drops highly correlated features based on a correlation threshold
def drop_high_corr(df, thres, heatmap=True):
    corr = df.corr().abs()
    corr2 = corr.reset_index(drop=True)
    corr2 = corr2.rename(columns={x:y for x,y in zip(corr.columns,range(0,len(corr.columns)))})
    if heatmap:
        plt.figure(figsize=(7,5))
        g=sns.heatmap(corr2, annot=False, cmap="YlOrRd")
    upper = corr.where(np.triu(np.ones(corr.shape), k=1).astype(np.bool))
    to_drop = [column for column in upper.columns if any(upper[column] > thres)]
    return df.drop(to_drop, axis=1)

# computes the euclidean distance between two 1D vectors
def euclidean_distance(v1, v2):
    distance = 0.0
    for i in range(len(v1)-1):
        distance += (v1[i] - v2[i])**2
    return np.sqrt(distance)

# visualisation of scores for choosing the number of clusters using four different methods to choose from
def optimal_k(max_num_clusters, data, method=None):
    
    if method != None:
        scores = []
        K = range(2,max_num_clusters+1) 
        X = data
        for k in K: 
            #Building and fitting the model 
            kmeanModel = KMeans(
                                n_clusters=k, init='random',
                                n_init=25, max_iter=1000,
                                random_state=2
                                )
            kmeanModel.fit(X)
            if method == 'inertia':
                scores.append(kmeanModel.inertia_)
            elif method == 'distortion':
                scores.append(sum(np.min(cdist(X, kmeanModel.cluster_centers_, 'euclidean'),axis=1)) / X.shape[0])
            elif method == 'silhouette':
                scores.append(metrics.silhouette_score(X, kmeanModel.labels_, metric = 'euclidean'))
            elif method == 'ch':
                scores.append(metrics.calinski_harabasz_score(X, kmeanModel.labels_))
        plt.plot(K, scores, 'bx-') 
        plt.xlabel('Values of K') 
        plt.ylabel('Score') 
        plt.title('Method - {}'.format(method)) 
        plt.show() 
        
    elif method == None:
        print('Please choose the type of method for finding the optimal number of clusters\n- inertia\n- distortion\n- silhouette\n- ch')