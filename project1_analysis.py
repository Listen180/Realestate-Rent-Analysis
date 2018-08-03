#!/opt/Anaconda3/bin/python
# -*- coding: UTF-8 -*-

# ********************************************************
# * Author        : LEI Sen
# * Email         : sen.lei@outlook.com
# * Create time   : 2018-07-31 12:00 
# * Last modified : 2018-08-03 15:32
# * Filename      : project1_analysis.py
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

#import math
#import matplotlib.mlab as mlab
#import matplotlib.pyplot as plt
#import scipy
#import scipy.stats as stats


# =============================================================================
# ##### Read data #####
# =============================================================================
t0 = time.time()
print('\n')
print('Reading data sets ...')

# data_raw = pd.read_csv('data/rent_merge_all.csv') # old version
# data_raw = pd.read_csv('data/rent_allmerged.csv')
read_path = 'data/rent_allmerged.pickle'    
data_raw = pd.read_pickle(read_path)

t1 = time.time()
t_readdata = round(t1 - t0, 4)
print('... Data set read complete. (Time consumed:', t_readdata,'s) \n')
del(t0,t1)


# =============================================================================
# ##### Prep #####
# =============================================================================
##
data = data_raw.dropna(axis=1, how='all', thresh=None, subset=None, inplace=False)

##
#col_list = list(data)

## Drop useless columns ##
drop_list = ['id',
 'pub_date',
 'geohashcode',
 'apartment_name',
 'monthly_rental',
 'inner_area',
 'total_area',
 'city_id',
 'districtname',
 'district_id',
 'gps_s',
 'province',
 'housecode',
 'housename',
 'projhighlight',
 'resblockid',
 'site_housecode',
 'lat_a',
 'lat_b',
 'lat_g',
 'lng_a',
 'lng_b',
 'lng_g',
 'urlpath',
 'area_diff',
 'longitude',
 'latitude'
]

data = data.drop(columns=drop_list)


## Specify the colnames that need to be converted to factors ##
factor_list = ['city',
 'district',
 'payment',
 'lease_type',
 'floor',
 'house_type',
 'house_status',
 'decoration_type',
 'room_orientation',
 'air_conditioner',
 'frige',
 'gas',
 'heat',
 'heat_supply',
 'internet',
 'microwave',
 'table_chair',
 'tv',
 'wardrobe',
 'washing_machine',
 'water_heater'
]


for i,item in enumerate(factor_list): # convert some variables to category type
    data[item] = data[item].fillna("暂无数据")
    data[item] = data[item].astype('category')
del(i,item)




## 
data['decoration_type'] = data['decoration_type'].apply(lambda x: '自如友家4.0' if x.find('4')>=0 else x)
data['decoration_type'] = data['decoration_type'].apply(lambda x: '自如友家3.0' if x.find('3')>=0 else x)
data['decoration_type'] = data['decoration_type'].apply(lambda x: '自如友家2.0' if x.find('2')>=0 else x)
data['decoration_type'] = data['decoration_type'].apply(lambda x: '自如友家1.0' if x.find('1')>=0 else x)
#data['decoration_type'].value_counts()

data['room_orientation'] = data['room_orientation'].apply(lambda x: '包含[南]' if x.find('南')>=0 else x)
data['room_orientation'] = data['room_orientation'].apply(lambda x: '不包含[南]' if x.find('东')>=0 else x)
data['room_orientation'] = data['room_orientation'].apply(lambda x: '不包含[南]' if x.find('西')>=0 else x)
data['room_orientation'] = data['room_orientation'].apply(lambda x: '不包含[南]' if x.find('北')>=0 else x)
#data['room_orientation'].value_counts()

data['house_type'] = data['house_type'].str.replace('房间','室0厅')
data['house_type'] = data['house_type'].str.replace('室','-')
data['house_type'] = data['house_type'].str.replace('厅','-')
data['house_type'] = data['house_type'].str.replace('卫','') 
#data['house_type'].value_counts()

house_type = data['house_type'].str.split('-',expand=True)
house_type = pd.DataFrame(house_type)
house_type.columns = ['bedroom_num','livingroom_num','bathroom_num']
data = pd.concat([data, house_type], axis=1)
del(house_type)
data = data.drop(columns=['house_type'])



## Variable Adjustment [on rent info]
rent_drop_list = ['air_conditioner',
 'frige',
 'gas',
 'heat',
# 'heat_supply',
 'internet',
 'microwave',
 'table_chair',
 'tv',
 'wardrobe',
 'washing_machine',
 'water_heater'
 ]
data = data.drop(columns=rent_drop_list)

## decoration type re-arrange
data['decoration_type'].replace('自如友家4.0', '精装修', inplace=True)
data['decoration_type'].replace('自如友家3.0', '精装修', inplace=True)
data['decoration_type'].replace('自如友家2.0', '精装修', inplace=True)
data['decoration_type'].replace('自如友家1.0', '精装修', inplace=True)
data['decoration_type'].replace('米苏', '精装修', inplace=True)
data['decoration_type'].replace('布丁', '精装修', inplace=True)
data['decoration_type'].replace('木棉', '精装修', inplace=True)
data['decoration_type'].replace('原味', '精装修', inplace=True)
data['decoration_type'].replace('业主直租原味', '精装修', inplace=True)
data['decoration_type'].replace('业主直租', '精装修', inplace=True)
#data['decoration_type'] = data['decoration_type'].astype('category')
data['decoration_type'].value_counts()

## payment type re-arrange
data['payment'].replace('双月付', '月付(或双月付)', inplace=True)
data['payment'].replace('月付', '月付(或双月付)', inplace=True)
data['payment'] = data['payment'].astype('category')
#data['payment'].value_counts()

## floor num re-arrange
data['floor_multi'] = data['floor']
data['floor_multi'].replace('高楼层', 5/6, inplace=True)
data['floor_multi'].replace('中楼层', 1/2, inplace=True)
data['floor_multi'].replace('低楼层', 1/6, inplace=True)
data['floor_multi'].replace('地下室', -1, inplace=True)
data['floor_multi'].replace('未知楼层', 0, inplace=True)

data['num_floors'] = data['num_floors'].astype('int')
data['floor_multi'] = data['floor_multi'].astype('float64')
data['floor_approx'] = data['num_floors'] * data['floor_multi']
data['floor_approx'] = data['floor_approx'].astype(int) # round down to int
data.floor_approx[data.floor_approx < 0] = -1

## sub-set if num_floors does not make sense
data = data[data.num_floors<=50]
data = data[data.num_floors<=50]
data = data.drop(columns=["floor", "num_floors","floor_multi"])


## Variable Adjustment [on city info]
cityinfo_drop_list = ['inner_living_area',
 'inner_population_density',
# 'inner_GDP_per_person',
 'inner_realty_invest',
 'inner_housebuilding_invest',
 'all_colleges_num',
 'inner_secondary_vocational_school_num',
 'inner_middleschool_num',
 'inner_primary_school_num',
# 'inner_hospital_num',
 'inner_large_gy_company_num',
 'drama_num_city',
 'scene_num_city',
 'bar_homeparty_related_num_city',
 'ktv_num_city',
 'hotel_num_city',
 'coffeeshopsrelated_num_city',
 'teahouse_related_num_city',
 'sweetsdrinks_related_num_city',
 'haircuts_related_num_city',
 'gaming_websurfing_related_num_city',
 'bookstores_num_city',
 'physiorelated_places_num_city',
 'beauty_relaxation_related_num_city',
 'chesscards_related_num_city',
 'all_sports_places_num_city',
 'park_num',
 'shop_num_city',
# 'inner_finish_green_rate',
 'busline_num_city',
 'metroline_num',
 'gas_station_num_city',
 'arrival_flight_num',
 'departure_flight_num',
 'train_station_num',
 'parking_num_city',
 'pharmacy_dianping_num_city',
 'banks_num_city',
 'express_num_city',
 'wc_num_city',
 'sweets_drinks_snacks_num_city',
 'guangzhou_cuisine_num_city',
 'westernstyle_food_num_city',
 'fastfood_num_city',
 'japanese_korean_styles_num_city',
 'jiangzhe_cuisine_num_city',
 'hunan_cuisine_num_city',
 'sichuan_cuisine_num_city',
 'bbq_seafood_hotpot_num_city',
# 'restuarant_sum',
 'housebuilding_rate',
# 'ration_of_0_14'
 ]

data = data.drop(columns=cityinfo_drop_list)


## Variable Adjustment [on city around]
restuarant_grid = data[['sweets_drinks_snacks_num_grid',
 'guangzhou_cuisine_num_grid',
 'westernstyle_food_num_grid',
 'fastfood_num_grid',
 'japanese_korean_styles_num_grid',
 'jiangzhe_cuisine_num_grid',
 'hunan_cuisine_num_grid',
 'sichuan_cuisine_num_grid',
 'bbq_seafood_hotpot_num_grid']
 ]
data['restuarant_sum_grid'] = restuarant_grid.apply(lambda x: x.sum(), axis=1)
del(restuarant_grid)

cityaround_drop_list = [#'cbd_distance',
 'airport_distance',
# 'train_station_distance',
# 'park_distance',
# 'shoppingcenter_distance',
# 'supermarket_distance',
 'xiaoqu_num',
# 'xiaoqu_totaldoors',
 'xiaoqu_avgprice',
# 'publicbus_station_num',
 'busline_num_grid',
# 'metro_station_num',
 'metro_lines_num',
# 'parking_num_grid',
# 'college_num',
# 'midschool_num',
# 'primschool_num',
# 'kindergarten_num',
# 'medical_service_num',
# 'sanjia_hospital_num',
# 'scene_num_grid',
 'wc_num_grid',
# 'drama_num_grid',
# 'library_num',
# 'government_num',
# 'shop_num_grid',
# 'bar_homeparty_related_num_grid',
# 'ktv_num_grid',
# 'movietheatre_num',
# 'hotel_num_grid',
# 'coffeeshopsrelated_num_grid',
 'teahouse_related_num_grid',
 'sweetsdrinks_related_num_grid',
 'flower_shops_num',
# 'haircuts_related_num_grid',
# 'gaming_websurfing_related_num_grid',
# 'bookstores_num_grid',
# 'lenses_related_num',
# 'clotheswash_places_num',
# 'house_keeping_num',
# 'physiorelated_places_num_grid',
# 'beauty_relaxation_related_num_grid',
# 'chesscards_related_num_grid',
# 'pharmacy_dianping_num_grid',
 'pharmacy_detail_num',
# 'cigeratte_tea_alcohol_num',
# 'express_num_grid',
# 'banks_num_grid',
# 'gas_station_num_grid',
# 'supermarket_num',
# 'fitness_room_num',
# 'outdoor_sports_places_num',
# 'all_sports_places_num_grid',
 'sweets_drinks_snacks_num_grid',
 'guangzhou_cuisine_num_grid',
 'westernstyle_food_num_grid',
 'fastfood_num_grid',
 'japanese_korean_styles_num_grid',
 'jiangzhe_cuisine_num_grid',
 'hunan_cuisine_num_grid',
 'sichuan_cuisine_num_grid',
 'bbq_seafood_hotpot_num_grid',
# 'company_num',
 'position_num',
 'avg_salary'
 ]

data = data.drop(columns=cityaround_drop_list)












## Check N/A existence and drop columns which have too many N/A
NA_num = []
for i,item in enumerate(list(data)): # convert some variables to category type
    na_num = sum(data[item].isnull())
    NA_num.append(na_num)
del(i,item, na_num)
NA_num = pd.DataFrame(NA_num, index=list(data), columns=["NA_num"])

drop_list_na = ["property_fee"]





## Check column types
data_types = pd.DataFrame(data.dtypes)
data_types.columns = ["data_types"]


# =============================================================================
# ## Save a summary file of current data set
# =============================================================================
data_summary = data.describe().T
summary_table = pd.concat([data_types, data_summary], axis=1)

save_path = 'rent_analysis_summary_talbe.csv'    
summary_table.to_csv(save_path, index=True)

# =============================================================================
# ##### Save data to csv for commenly use #####
# =============================================================================
print('\n')
print('! Saving File ... !')
cwd = os.getcwd()
save_path = 'data/rent_analysis.csv'
data.to_csv(save_path, index=False)
print('\n')
print('! ... File Saved Successful !')
print('  File path:', cwd + '/' + save_path)

