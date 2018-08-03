#!/opt/Anaconda3/bin/python
# -*- coding: UTF-8 -*-

# ********************************************************
# * Author        : LEI Sen
# * Email         : sen.lei@outlook.com
# * Create time   : 2018-07-25 11:13
# * Last modified : 2018-08-03 15:30
# * Filename      : project1_read.py
# * Description   : This reads data from csv files
# *********************************************************

import os
import time
#import re
#import psycopg2 as pg2
import pandas as pd
import numpy as np
#import difflib
import random

# =============================================================================
# ##### Reading Date Set #####
# =============================================================================

t0 = time.time()
print('\n')
print('Reading all data sets ...')

#df_rent = pd.read_csv('data/rent_loupan_merged.csv') 
df_rent = pd.read_csv('data/rent_loupan_merged_filtered.csv') 
    # Be aware that this is a merged data set which contains basic rent info 
     # and property fee from loupan data set
df_cityaround = pd.read_csv('data/city_around.csv')
df_cityinfo = pd.read_csv('data/city_info.csv')
df_citypop = pd.read_csv('data/city_pop.csv')
df_cityaround = pd.read_csv('data/city_around.csv')
df_macro1 = pd.read_csv('data/shibor_mean.csv')
df_macro2 = pd.read_csv('data/macro_cpi_m12_pmi.csv')

WP = pd.read_pickle('data/W_P_matrix.pickle')
df_WP = pd.DataFrame()
for i,key in enumerate(WP):
    WP_i = WP[key]
    df_WP = pd.concat([df_WP, WP_i], axis=1)
df_WP = df_WP.T.reset_index()
df_WP.columns = ["district_id", "WP"]
del(i,key,WP_i)

df_houseprice = pd.read_pickle('data/cityhouse_history_price.pickle')


t1 = time.time()
t_readdata = round(t1 - t0, 4)
print('... All data set read complete. (Time consumed:', t_readdata,'s) \n')
del(t0,t1)


## Some basic modification on data sets
#df_rent["monthly_rental"] = df_rent["monthly_rental"] / df_rent["inner_area"]

df_macro1["date"] = pd.to_datetime(df_macro1["date"])
df_macro1['date'] = df_macro1['date'].apply(lambda x: x.strftime('%Y-%m')) 

df_macro2["date"] = pd.to_datetime(df_macro2["date"])
df_macro2['date'] = df_macro2['date'].apply(lambda x: x.strftime('%Y-%m'))

df_macro2 = df_macro2[['date',
 'M2_rate',
 'M1_rate',
 'city_cpi_rate',
 'gmi_manu_index',
 'gmi_non_manu_index']]

## Omit rows who have N/A "district_id"
sum(df_rent.district_id.isna())
df_rent = df_rent[~df_rent.district_id.isna()]

#print('\n')
#print('The type of [data] is:', type(data), '; Shape:', np.shape(data))
#print('\n')
#print(data.dtypes)
#list(data)




# =============================================================================
# ##### Perform merge in Python #####
# =============================================================================
## Merge 85 city info
t0 = time.time()
print('\n')
print('Merging 85 city info data sets ... [city - city]')
df_citymerge = df_cityinfo.merge(df_citypop.drop(columns=["city"]), 
                                    left_on=["citycode"], 
                                    right_on=["citycode"], 
                                    how='inner')
#del(df_cityinfo, df_citypop) ##
t1 = time.time()
t_merge1 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge1,'s) \n')
del(t0,t1)

## Merge macro data
t0 = time.time()
print('\n')
print('Merging macro data sets ... [macro - macro]')
df_macromerge = df_macro2.merge(df_macro1[["date","O/N"]], 
                                left_on=["date"], 
                                right_on=["date"], 
                                how='inner')
#del(df_macro1, df_macro2) ##
t1 = time.time()
t_merge2 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge2,'s) \n')
del(t0,t1)
df_macromerge.columns = ['date',
 'M2_rate',
 'M1_rate',
 'city_cpi_rate',
 'gmi_manu_index',
 'gmi_non_manu_index',
 'SHIBOR_O/N']



## Merge rent data with all (merged) city info ##
t0 = time.time()
print('\n')
print('Merging on to rent data set ... [city info --> rent]')
df_rent_city = df_rent.merge(df_citymerge.drop(columns=["city"]), 
                                    left_on=["city_id"], 
                                    right_on=["citycode"], 
                                    how='left').drop(columns=["citycode"])
#del(df_rent)
t1 = time.time()
t_merge3 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge3,'s) \n')
del(t0,t1)



## Merge WP info ##
t0 = time.time()
print('\n')
print('Merging W*P info ... [W*P --> rent & city info]')
#list(df_rent_city)
#list(df_WP)

df_rent_city_WP = df_rent_city.merge(df_WP, 
                    left_on=["district_id"], 
                    right_on=["district_id"], 
                    how='left') 
#del()
t1 = time.time()
t_merge4 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge4,'s) \n')
del(t0,t1)


## Merge grid around info ##
t0 = time.time()
print('\n')
print('Merging grid around data set ... [grid around info --> rent & city info & W*P]')
#list(df_rent_city)
#list(df_cityaround)

col_drop = ['provincename','cityname','citycode','districtname','districtcode']

df_rent_city_WP_around = df_rent_city_WP.merge(df_cityaround.drop(columns=col_drop), 
                    left_on=["geohashcode"], 
                    right_on=["geohashcode"], 
                    suffixes=('_city', '_grid'), 
                    how='inner') # should use 'left' instead, but use 'inner' to check
del(col_drop)
#del()
t1 = time.time()
t_merge4 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge4,'s) \n')
del(t0,t1)



## Merge complete macro data ##
df_rent_city_WP_around["pub_month"] = df_rent_city_WP_around["pub_date"].str[:7]

t0 = time.time()
print('\n')
print('Merging macro data sets ... [macro info --> rent & city info & W*P & grid around info]')
df_rent_city_WP_around_macro = df_rent_city_WP_around.merge(df_macromerge, 
                                left_on=["pub_month"], 
                                right_on=["date"], 
                                how='inner').drop(columns=["date"])
#del()
t1 = time.time()
t_merge5 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge5,'s) \n')
del(t0,t1)

#list(set(list(df_rent_city_WP_around_macro)) - set(list(df_rent_city_WP_around)))



## Merge house price lag data ##
df_rent_city_WP_around_macro["pub_date"] = pd.to_datetime(df_rent_city_WP_around_macro["pub_date"])

t0 = time.time()
print('\n')
print('Merging macro data sets ... [house price lag --> rent & city info & W*P & grid around info & macro info]')

lag_num = 3
for i in range(lag_num):
    lag = i+1
    lagcol = 'date_lag'+str(lag)
    newname = 'house_price_lag'+str(lag)
    df_rent_city_WP_around_macro[lagcol] = df_rent_city_WP_around_macro["pub_date"] - pd.DateOffset(months=lag)
    df_rent_city_WP_around_macro[lagcol] = df_rent_city_WP_around_macro[lagcol].dt.to_period('M')
    if lag == 1:
        df_rent_city_WP_around_macro_pricelag = df_rent_city_WP_around_macro.merge(df_houseprice[["house_price", "city_id", "date"]], 
                                left_on=[lagcol, "city_id"], 
                                right_on=["date", "city_id"], 
                                how='inner').drop(columns=["date"])
        df_rent_city_WP_around_macro_pricelag.rename(columns={'house_price':newname}, inplace=True)
    else:
        df_rent_city_WP_around_macro_pricelag = df_rent_city_WP_around_macro_pricelag.merge(df_houseprice[["house_price", "city_id", "date"]], 
                                left_on=[lagcol, "city_id"], 
                                right_on=["date", "city_id"], 
                                how='inner').drop(columns=["date"])
        df_rent_city_WP_around_macro_pricelag.rename(columns={'house_price':newname}, inplace=True)
del(i, lag,lagcol,newname)
#df_rent_city_WP_around_macro[["pub_month", "date_lag1", "date_lag2", "date_lag3"]].head()
#del()
t1 = time.time()
t_merge6 = round(t1 - t0, 4)
print('... Merge complete. (Time consumed:', t_merge6,'s) \n')
del(t0,t1)

list(set(list(df_rent_city_WP_around_macro_pricelag)) - set(list(df_rent_city_WP_around_macro)))



# =============================================================================
# ##### Save data #####
# =============================================================================
## Save rent-city merge merged data set 
data = df_rent_city

print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/rent_citymerge.pickle'    
data.to_pickle(save_path)
print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)


## Save rent-city-WP merged data set 
data = df_rent_city_WP

print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/rent_citymerge_WP.pickle'    
data.to_pickle(save_path)
print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)


## Save rent-citymerge_WP_city_around_pricelag merged data set 
data = df_rent_city_WP_around_macro_pricelag

print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/rent_allmerged.pickle'    
data.to_pickle(save_path)
print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)


