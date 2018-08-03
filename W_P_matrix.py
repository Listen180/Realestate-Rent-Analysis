#!/opt/Anaconda3/bin/python
# -*- coding: UTF-8 -*-

# ********************************************************
# * Author        : LEI Sen
# * Email         : sen.lei@outlook.com
# * Create time   : 2018-07-31 12:00 
# * Last modified : 2018-08-03 15:34
# * Filename      : W_P_matrix.py
# * Description   : 
# *********************************************************

# =============================================================================
# ##### Configuration #####
# =============================================================================

import os
import time
#import re
#import psycopg2 as pg2
import pandas as pd
import numpy as np
#import difflib
#import random

#from sklearn import linear_model
import math
import matplotlib.mlab as mlab
import matplotlib.pyplot as plt
import scipy
import scipy.stats as stats

#import json
import pickle

# =============================================================================
# ##### Read data #####
# =============================================================================
t0 = time.time()
print('\n')
print('Reading data set ...')

data_raw = pd.read_csv('data/rent_loupan_merged.csv')
df_citycode = pd.read_csv('data/citycode.csv')

t1 = time.time()
t_readdata = round(t1 - t0, 4)
print('... Data set read complete. (Time consumed:', t_readdata,'s) \n')
del(t0,t1)


#list(data_raw)


# =============================================================================
# ##### Data type check #####
# =============================================================================
data = data_raw.drop(columns=["recordbatchno"])

data["id"] = data["id"].astype('int')
data["city_id"] = data["city_id"].astype('object')
data["district_id"] = data["district_id"].astype('object')

data.dtypes

# =============================================================================
# ##### Sub-setting #####
# =============================================================================
data["area_diff"] = data["total_area"]-data["inner_area"]

## 关于写字楼、商铺、办公
searchfor = ['写字楼', '商铺']
#data_sub1 = data[data.housename.str.contains('|'.join(searchfor))]
sub_index_out1 = pd.DataFrame(np.where([data.housename.str.contains('|'.join(searchfor))])[1])
sub_index_out1.columns = ["item_index"]

searchfor = ['可做商铺', '可用作商铺']
#data_sub2 = data_sub1[~data_sub1.housename.str.contains('|'.join(searchfor))]
sub_index_in1 = pd.DataFrame(np.where([~data.housename.str.contains('|'.join(searchfor))])[1])
sub_index_in1.columns = ["item_index"]

sub_index_inNout1 = pd.DataFrame(
        np.intersect1d(sub_index_out1.item_index, sub_index_in1.item_index, 
                       assume_unique=True))
sub_index_inNout1.columns = ["item_index"]


## 关于车位、车库
searchfor = ['车位', '车库']
#data_sub3 = data[data.housename.str.contains('|'.join(searchfor))]
sub_index_out2 = pd.DataFrame(np.where([data.housename.str.contains('|'.join(searchfor))])[1])
sub_index_out2.columns = ["item_index"]

searchfor = ['有车位', '有车库', '带车位','带车库']
#data_sub4 = data_sub3[~data_sub3.housename.str.contains('|'.join(searchfor))]
sub_index_in2 = pd.DataFrame(np.where([~data.housename.str.contains('|'.join(searchfor))])[1])
sub_index_in2.columns = ["item_index"]

sub_index_inNout2 = pd.DataFrame(
        np.intersect1d(sub_index_out2.item_index, sub_index_in2.item_index, 
                       assume_unique=True))
sub_index_inNout2.columns = ["item_index"]


## 关于别墅
searchfor = ['别墅']
data_sub_temp = data[data.housename.str.contains('|'.join(searchfor))]
sub_index_out3 = pd.DataFrame(np.where([data.housename.str.contains('|'.join(searchfor))])[1])
sub_index_out3.columns = ["item_index"]



## Final cut
sub_index_inNout_all = pd.concat([sub_index_inNout1,sub_index_inNout2, sub_index_out3], axis=0)
rule_out_index = sub_index_inNout_all.item_index.unique()
mask = np.setxor1d(data.index, rule_out_index)

data_sub = data.iloc[mask]


## 关于房屋面积和租金 re-sub-setting
data_sub_sub = data_sub[data_sub.monthly_rental > data_sub.inner_area]
data_sub_sub = data_sub_sub[data_sub_sub["monthly_rental"]<100000]

data_sub_sub = data_sub_sub[data_sub_sub["inner_area"]>1]
data_sub_sub = data_sub_sub[data_sub_sub["total_area"]>1]
data_sub_sub = data_sub_sub[data_sub_sub["total_area"]<400]
data_sub_sub = data_sub_sub[data_sub_sub["area_diff"]>=0]



## eliminate duplication
#data_new = data.drop_duplicates(subset=None, keep='first', inplace=False)
data_new = data_sub_sub.drop_duplicates(subset=["housecode", "housename"])


## eliminate some special case
#data_new = data_new[]
data_new["monthly_rental_per_m2"] = data_new["monthly_rental"] / data_new["inner_area"]



# =============================================================================
# ##### Save filtered data set #####
# =============================================================================
print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/rent_loupan_merged_filtered.csv'
data_new.to_csv(save_path, index=False)
print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)



# =============================================================================
# ##### Calculate P bar by district #####
# =============================================================================

data_new.city_id.describe()
data_new.city_id.unique()

P_bar = pd.DataFrame(data_new.groupby(["city_id", "district_id"]).mean().monthly_rental_per_m2)
P_bar = P_bar.reset_index()

city_code_sum = df_citycode.sort_values(by=["citycode"]).citycode
city_code_sum = city_code_sum.reset_index().citycode

##
W_P_dict = {}

for i,code in enumerate(city_code_sum):
    '''
    This is to generate W * P_bar matrix for each city
    '''
    ##
    p_i = P_bar[P_bar["city_id"]==code]
    dist_id_i = p_i.district_id.astype('int')
    dist_id_i = dist_id_i.reset_index()["district_id"]
    p_i = np.matrix(p_i["monthly_rental_per_m2"]).T
    ##
    read_path_i = 'data/IDW/' + 'IDW_city-' + str(code) + '.csv'
    IDW_i = pd.read_csv(read_path_i, index_col=["dist_code"])
    dist_id_i_all = IDW_i.index
    IDW_i.columns = dist_id_i_all
    w_i = IDW_i.loc[dist_id_i]
    w_i = w_i.T
    w_i = w_i.loc[dist_id_i]
    w_i = np.matrix(w_i)
    ##
    W_P_i = np.matmul(w_i, p_i)
    W_P_i_df = pd.DataFrame(W_P_i).T
    W_P_i_df.columns = dist_id_i
    #print('\nW * P matrix for city <', code, '> :')
    #print(W_P_i)
    ##
    #W_P_dict[code] = W_P_i
    W_P_dict[code] = W_P_i_df




# =============================================================================
# ##### Save file #####
# =============================================================================
print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/W_P_matrix.pickle'

with open(save_path, 'wb') as f_saver:
    pickle.dump(W_P_dict, f_saver, protocol=pickle.HIGHEST_PROTOCOL)

print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)






