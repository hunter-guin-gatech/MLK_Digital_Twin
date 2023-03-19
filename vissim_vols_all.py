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
# intersection_list = ['Broad']
intersection_list = ['Broad','Pine','Chestnut','Market','Magnolia','Georgia', 'Lindsay','Houston','Peeples','Central','Douglas']
approach_list=['Eastbound','Westbound', 'Northbound', 'Southbound']


for i in range(len(intersection_list)):
	## read csv file
	df_raw = pd.read_csv(path_to_raw_files+intersection_list[i]+'_approach_vols.csv')
	# print (df_raw)
	df_new = pd.DataFrame()

	## filter for date 
	df_six_oct_filtered = df_raw[df_raw['date'] == '2022-10-06']
	# print (df_)
	

	## filter for timestamp
	df_six_oct_sorted=df_six_oct_filtered.sort_values(by=['timestamp'])

	for j in range(len(approach_list)):
		## filter for approach 
		df_app_filtered = df_six_oct_sorted[df_six_oct_sorted['approach']==approach_list[j]]
		df_app_sorted=df_app_filtered.sort_values(by=['timestamp'])
		df_app_sorted = df_app_sorted.reset_index(drop=True)
		print (df_app_sorted)
		print (len(df_app_sorted))
		# df_hh_filtered = df_app_filtered[df_app_filtered['hh'].isin([14, 15, 16, 17, 18])]
		
		left_vol_list = list(df_app_sorted.vol_left.values)
		print (left_vol_list)
		# thru_vol_list = list(df_app_sorted.vol_thru.values)
		# right_vol_list = list(df_app_sorted.vol_right.values)
		# uturn_vol_list = list(df_app_sorted.vol_uturn.values)
		# total_vol_list = list(df_app_sorted.vol_total.values)

		
		
		N=10
		p = list(df_app_sorted["vol_left"].groupby(df_app_sorted.index // N).sum())
		print (p)
		print (len(p))
		

		p1 = list(df_app_sorted["vol_thru"].groupby(df_app_sorted.index // N).sum())
		print (len(p1))

		p2 = list(df_app_sorted["vol_right"].groupby(df_app_sorted.index // N).sum())
		print (len(p2))

		p3 = list(df_app_sorted["vol_uturn"].groupby(df_app_sorted.index // N).sum())
		print (len(p3))

		p4 = list(df_app_sorted["vol_total"].groupby(df_app_sorted.index // N).sum())
		print (len(p4))

		col_name = intersection_list[i]+"-"+approach_list[j]

		df_new[col_name+"_L"] = p
		df_new[col_name+"_T"] = p1
		df_new[col_name+"_R"] = p2
		df_new[col_name+"_U"] = p3
		df_new[col_name+"_Total"] = p4
	print (df_new)
	df_new.to_csv(intersection_list[i]+"_vols_for_turn.csv")


		# ##
		# df_turn_vols[col_name] = p


# print (df_turn_vols)
# df_turn_vols.to_csv("turns_10min_allday.csv", index=False)
##



## 