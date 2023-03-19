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
intersection_list=['Georgia', 'Pine', 'Central']

def percent_blanks_zonename(intrsc):
	df_formatted = pd.read_csv(path_to_raw_files+intrsc+'_reformatted.csv')
	print (df_formatted['zoneapproach'].value_counts(normalize=True) * 100)

	return (df_formatted.isnull().mean() * 100)

# for i in range(len(intersection_list)):
# 	print (intersection_list[i])
# 	print (percent_blanks_zonename(intersection_list[i]))

approach_list = ['Eastbound', 'Westbound', 'Northbound', 'Southbound']

with open('test-broad-dict-v2.txt') as f:
    data = f.read()

dict_rel_zone = json.loads(data)
# dict_int_name = "MLK_"+intersection_list[0]
# print (dict_rel_zone["Pine"])
for j in range(len(intersection_list)):
	df_formatted = pd.read_csv(path_to_raw_files+intersection_list[j]+'_reformatted.csv')
	new_df =pd.DataFrame(columns=['timestamp','date','time_central','hh','mm','ss','intersection','approach','vol_left','vol_thru','vol_right','vol_uturn','vol_total'])

	for i in range(len(approach_list)):
		dict_int_name = "MLK_"+intersection_list[j]
		zone_reqd_list = dict_rel_zone[dict_int_name][i][approach_list[i]] 
		df_form_app_filtered = df_formatted[(df_formatted['zoneapproach'] == approach_list[i]) | (df_formatted['zoneapproach'] == "Unassigned")] 

		tsts_unique = df_form_app_filtered.timestamp.unique()

		for k in range(len(tsts_unique)):
			df_tst_filtered = df_form_app_filtered[df_form_app_filtered['timestamp'] == tsts_unique[k]]

			## Getting times in chattanooga time zone (1 hour behind atlanta localtime) ##
			p = float(float(tsts_unique[k])/1000-(3600*1))
			# p = float(1665115250.621-(3600*1))
			print (p)
			my_time = datetime.fromtimestamp(p).strftime('%Y-%m-%d %H:%M:%S.%f')
			dat = my_time.split(' ')[0]
			tm = my_time.split(' ')[1]
			hh = tm.split(':')[0]
			mm = tm.split(':')[1]
			ss = tm.split(':')[2]

			Arr_1 = [tsts_unique[k], dat, tm, hh, mm, ss, intersection_list[j],approach_list[i]]
			print (Arr_1)
			vol_left= 0
			vol_thru=0
			vol_right = 0
			vol_uturn = 0
			vol_total = 0

			# for each row of df_tst_filtered
			for index, row in df_tst_filtered.iterrows():
				if row["zonename"] in zone_reqd_list:

					vol_left = vol_left + row["turnsleft"]
					vol_thru = vol_thru + row["turnsthrough"]
					# if intersection_list[j] == "MLK_Douglas":
					# 	vol_thru = vol_thru + row["TurnsThrough"]
					# else:
					# 	vol_thru = vol_thru + row["TurnsThru"]
					vol_right = vol_right + row["turnsright"]
					vol_uturn = vol_uturn + row["turnsuturn"]
					vol_total = vol_total + row["turnsleft"] + row["turnsthrough"] + row["turnsright"] + row["turnsuturn"]
					# if intersection_list[j] == "MLK_Douglas":
					# 	vol_total = vol_total + item["TurnsLeft"] + item["TurnsThrough"] + item["TurnsRight"] + item["TurnsUturn"]
					# else:
					# 	vol_total = vol_total + item["TurnsLeft"] + item["TurnsThru"] + item["TurnsRight"] + item["TurnsUturn"]

			Arr_1.append(vol_left)
			Arr_1.append(vol_thru)
			Arr_1.append(vol_right)
			Arr_1.append(vol_uturn)
			Arr_1.append(vol_total)
			new_df.loc[len(new_df)] = Arr_1

	csv_file_name = intersection_list[j]+"_approach_vols.csv"
	new_df.to_csv(csv_file_name, index=False)
	print (new_df)



		
