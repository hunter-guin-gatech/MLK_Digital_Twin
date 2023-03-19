import os
import glob
#import time
#import math
import pymysql
import sqlite3 as lite
import datetime
#import pandas as pd
import mysql.connector
#import warnings
import json
from time import time, sleep
from datetime import datetime, timedelta
from os.path import exists
from glob import glob
import pandas as pd
import pickle
import ast
import time

## Read file to csv 
path_to_raw_files = "\\\\ad.gatech.edu\\gtfs\\COE\\CEE\\Transpo\\smartdata\\DOE\\Oct62022_Model\\GrdsmartData_fmtd\\"
# intersection_list = ['Broad','Pine','Chestnut','Market','Magnolia','Georgia', 'Lindsay','Houston','Peeples','Central','Douglas']
intersection_list=['Chestnut', 'Pine']
approach_list=['Eastbound','Westbound', 'Northbound', 'Southbound']
for i in range(len(intersection_list)):
	## read csv file
	df_raw = pd.read_csv(path_to_raw_files+intersection_list[i]+'_approach_vols.csv')
	print (df_raw)

	## filter for date 
	df_six_oct_filtered = df_raw[df_raw['date'] == '2022-10-06']
	print (df_six_oct_filtered)

	## filter for timestamp
	df_six_oct_sorted=df_six_oct_filtered.sort_values(by=['timestamp'])

	for j in range(len(approach_list)):
		## filter for approach 
		df_app_filtered = df_six_oct_sorted[df_six_oct_sorted['approach']==approach_list[j]]

		## get column values for hh, mm, and ss
		date_list = list(df_app_filtered.date.values)
		hh_list =  list(df_app_filtered.hh.values)
		mm_list = list(df_app_filtered.mm.values)
		vol_total_list = list(df_app_filtered.vol_total.values)
		vol_total_list = list(df_app_filtered.vol_total.values)
		

		column_names = ['date','hh','mm']
		column_names.append(intersection_list[i]+"-"+approach_list[j])
		new_df =pd.DataFrame(columns=column_names)

		new_df[column_names[0]] =  date_list
		new_df[column_names[1]] =  hh_list
		new_df[column_names[2]] =  mm_list
		new_df[column_names[3]] =  vol_total_list

		print (new_df)
		csv_vol_vissim_format_name = intersection_list[i]+"-"+approach_list[j]+"vissim_fmt.csv"
		new_df.to_csv(csv_vol_vissim_format_name, index = False)





##





##



## 