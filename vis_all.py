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

path_to_raw_files = "\\\\ad.gatech.edu\\gtfs\\COE\\CEE\\Transpo\\smartdata\\DOE\\Oct62022_Model\\"
intersection_list = ['Broad','Chestnut','Market','Magnolia','Lindsay','Houston','Peeples','Central','Douglas']
# intersection_list=['Chestnut', 'Pine']
approach_list=['Eastbound','Westbound', 'Northbound', 'Southbound']

df_all_empty =pd.DataFrame([])
for i in range(len(intersection_list)):
	for j in range(len(approach_list)):
		
		df_raw = pd.read_csv(path_to_raw_files+intersection_list[i]+"-"+approach_list[j]+"vissim_fmt.csv")
		print (df_raw)
		col_name = intersection_list[i]+"-"+approach_list[j]
		vol_total_list = df_raw[col_name].values.tolist()
		
		df_all_empty[col_name] = vol_total_list
		print (df_all_empty)
		# df_new = pd.concat([df_new,df_all_empty])

print (df_all_empty)
df_all_csv = "all_vol_tots.csv"
df_all_empty.to_csv(df_all_csv, index =False)

