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

## Read file to csv 
path_to_raw_files = "\\\\ad.gatech.edu\\gtfs\\COE\\CEE\\Transpo\\smartdata\\DOE\\Oct62022_Model\\GridSmart_Oct6\\"
intersection_list = ['Chestnut','Pine']


# print (df_raw)

# def reformat(log):
# 	log1 = log[1:-1]
# 	s = log1.replace("=", ":")
# 	arr = []
# 	f = []
# 	key_value = s.split(", {_")

# 	for k in key_value: 
# 	    arr.append(k)

# 	# print (len(arr))

# 	for l in arr: 
# 	    # print (l)
# 	    k_value = l.split(", ")
# 	    dict = {}
# 	    for v in k_value:
# 	        print (v)
# 	        aux = v.split(":")
# 	        aux[1] = aux[1].replace("'", "")
# 	        dict[aux[0]] = aux[1].replace("'", "")
# 	    f.append(dict)
# 	# print (f)
# 	return f

def reformat(log):
    log1 = log[1:-1]
    s = log1.replace("=", ":")
    arr = []
    f = []
    key_value = s.split(", {_")

    for k in key_value:
        arr.append(k)


    for l in arr:

        k_value = l.split(", ")
        dict = {}
        for v in k_value:
            aux = v.split(":")  
            try:
                aux[1] = aux[1].replace("'", "")
                dict[aux[0]] = aux[1].replace("'", "")
            except:
                try:
                    value = dict[list(dict)[-1]]
                    dict[list(dict)[-1]] = value + ", " + v
                    # print(value)
                except:
                    continue
               
                pass
        f.append(dict)
    return f

for i in range(len(intersection_list)):
	df_raw = pd.read_csv(path_to_raw_files+intersection_list[i]+".csv")
	df_merged = pd.DataFrame([])
	for index, row in df_raw.iterrows():

		tst = row["timestamp"]
		int_name = row['intersection']

		p = row['data']
		# print (row['data'])
		# print (df_raw.loc[[index]])

		p_dict = reformat(p)
		print (len(p_dict))
	# approach_list = [d['zoneapproach'] for d in p_dict]
	# print (approach_list)
	# print (len(approach_list))

		df_reformatted = pd.DataFrame([])
			
		# tst = df_raw.at[i, 'timestamp']
		tst_list = [tst]*(len(p_dict))
		# print (tst_list)

		# int_name = df_raw.at[i, 'intersection']
		intrsc_name_list = [int_name]*(len(p_dict))
		# print (intrsc_name_list)

		approach_list = [d['zoneapproach'] for d in p_dict]

		# print (approach_list)
		zone_name_list = [d['zonename'] for d in p_dict]

		turn_left_list = [d['turnsleft'] for d in p_dict]
		turn_thru_list = [d['turnsthrough'] for d in p_dict]
		turn_right_list = [d['turnsright'] for d in p_dict]
		turn_uturn_list = [d['turnsuturn'][:-1] for d in p_dict]

		# print (turn_left_list)
		df_reformatted['timestamp'] = tst_list
		df_reformatted['intersection'] = intrsc_name_list
		df_reformatted['zoneapproach']= approach_list
		df_reformatted['zonename']=zone_name_list
		df_reformatted['turnsleft']= turn_left_list
		df_reformatted['turnsthrough']=turn_thru_list
		df_reformatted['turnsright']=turn_right_list
		df_reformatted['turnsuturn']=turn_uturn_list
		df_reformatted.loc[df_reformatted["turnsthrough"] == "null", "turnsthrough"] = 0

		print(df_reformatted.at[0,'timestamp'])
		df_reformatted['turnsleft'] = df_reformatted['turnsleft'].astype(int)
		df_reformatted['turnsthrough'] = df_reformatted['turnsthrough'].astype(int)
		df_reformatted['turnsright'] = df_reformatted['turnsright'].astype(int)
		df_reformatted['turnsuturn'] = df_reformatted['turnsuturn'].astype(int)


		# print (df_reformatted)
		# df_reformatted.loc[df_reformatted["turnsthrough"] == "null", "turnsthrough"] = 0
		# print (df_reformatted)

		df_reformatted['vol_total'] = df_reformatted['turnsleft']+df_reformatted['turnsthrough']+df_reformatted['turnsright']+df_reformatted['turnsuturn']
		# print (df_reformatted)
		df_merged = df_merged.append(df_reformatted, ignore_index=True)

	print (df_merged)
	csv_name = intersection_list[i]+"_reformatted.csv"
	df_merged.to_csv(csv_name, index=False)