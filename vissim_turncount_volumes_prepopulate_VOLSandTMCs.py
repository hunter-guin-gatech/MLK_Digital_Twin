
from __future__ import print_function
import os
# COM-Server
import win32com.client as com
import pandas as pd
import numpy as np


## Connecting the COM Server => Open a new Vissim Window:
Vissim = com.gencache.EnsureDispatch("Vissim.Vissim") #
# Vissim = com.Dispatch("Vissim.Vissim") # once the cache has been generated, its faster to call Dispatch which also creates the connection to Vissim.
# If you have installed multiple Vissim Versions, you can open a specific Vissim version adding the version number
# Vissim = com.gencache.EnsureDispatch("Vissim.Vissim.10") # Vissim 10
# Vissim = com.gencache.EnsureDispatch("Vissim.Vissim.21") # Vissim 2021

# Path_of_COM_Basic_Commands_network = 'C:\\Users\\Public\\Documents\\PTV Vision\\PTV Vissim 2021\\Examples Training\\COM\\Basic Commands\\'

## Load a Vissim Network:
## Connecting the COM Server => Open a new Vissim Window:
# Vissim = com.gencache.EnsureDispatch("Vissim.Vissim") #

Path_of_COM_Basic_Commands_network = os.getcwd()
print (os.getcwd())

## Load a Network:
Filename = os.path.join(Path_of_COM_Basic_Commands_network, 'MLKCorridor_24hr_blank_v2.inpx')
flag_read_additionally  = False
Vissim.LoadNet(Filename,flag_read_additionally)

## Initialize Vissim Model ---- Saturday
## Start vissim simulation 
Vissim.Net.Scripts.SetAllAttValues('RunType', 1)
simRes = Vissim.Simulation.AttValue('SimRes')
# simtime = 86400
## 23 hours = 82800
simtime = 82800
Vissim.Simulation.SetAttValue('SimPeriod', simtime)
Vissim.Simulation.SetAttValue('UseMaxSimSpeed', True)

## Get list of vissim vehicle input itemkeys
# Get list of vehicle inputs objects (intersection, approach, itemkey)
volume_count_key_df = pd.read_excel ( r'vehicle_inputs_list_v3.xlsx')
print(volume_count_key_df)
## Update these volume arrays
volume_intersection_key_array = volume_count_key_df.loc[:,'intersection_name'].values
volume_approach_key_array = volume_count_key_df.loc[:,'approach'].values
volume_itemkey_array = volume_count_key_df.loc[:,'item_key'].values
print(volume_itemkey_array)

# ############### SET TIME INTERVAL COLLECTION FOR VOLUME INPUTS ##########################################
interval_length_volume = 60
# for timeInt in range(2, (int(simtime/interval_length_volume)+1)):
#     Vissim.Net.TimeIntervalSets.ItemByKey(1).TimeInts.AddTimeInterval(timeInt)
#     TimeIntNoNew1 = Vissim.Net.TimeIntervalSets.ItemByKey(1).TimeInts.ItemByKey(timeInt)
#     TimeIntNoNew1.SetAttValue('Start',interval_length_volume*(timeInt-1))
#     for t in range(len(volume_itemkey_array)):
#     # for t in range(1):
#         key_no = volume_itemkey_array[t]
#         print (key_no)
#         Vissim.Net.VehicleInputs.ItemByKey(key_no).SetAttValue('Cont('+str(timeInt)+')', False)

no_of_intervals = int(simtime/interval_length_volume)


##### READ VOLUMES IN DATAFRAME ##########################
entry_volumes_df = pd.read_excel (r'vol_all_day_oct6_script_v5.xlsx')
columns_array = entry_volumes_df.columns
print (columns_array)

# # ###### PLUG IN VOLUMES TO VEHICLE INPUT TIME INTERVALS ####################
for tt in range(len(volume_itemkey_array)):
#for tt in range(1):
	col_name = columns_array[tt]
# # col_name = 'lindsay_nb_entry'

# 	## Get list of volumes for this input point in an array 
	input_volume_value_arr = entry_volumes_df[col_name].to_numpy()
	print (col_name)

	#key_no = volume_itemkey_array[tt]
	for t in range(no_of_intervals):
		vol_int_number = t+1
		volume_interval = 'Volume('+str(vol_int_number)+')'
		input_volume_value = input_volume_value_arr[t]
		print (input_volume_value)
		Vissim.Net.VehicleInputs.ItemByKey(volume_itemkey_array[tt]).SetAttValue(volume_interval, int(int(input_volume_value)*60))
		#Vissim.Net.VehicleInputs.ItemByKey(14).SetAttValue(volume_interval, int(input_volume_value))


# ####### PLUG IN TURN COUNT RATIOS TO ROUTE DECISIONS ####################
interval_length_turns = 600
no_of_intervals_turn = int(simtime/interval_length_turns)
print(no_of_intervals_turn)
# for timeInt in range(2, (int(simtime/interval_length_turns)+1)):
#     print (timeInt)
#     Vissim.Net.TimeIntervalSets.ItemByKey(2).TimeInts.AddTimeInterval(timeInt)
#     TimeIntNoNew = Vissim.Net.TimeIntervalSets.ItemByKey(2).TimeInts.ItemByKey(timeInt)
#     TimeIntNoNew.SetAttValue('Start',interval_length_turns*(timeInt-1))

### Get list of static vehicle routing objects 
turn_count_key_df = pd.read_excel(r'Updated_Routing_oct622_v2.xlsx')
print (list(turn_count_key_df.columns.values))
turn_approach_array = np.array(turn_count_key_df[['approach']]).tolist()
app_item_array = np.array(turn_count_key_df[['app_item_key']]).tolist()
turn_item_key_array = np.array(turn_count_key_df[['turn_item_key']]).tolist()
#turn_position_key_array = np.array(turn_count_key_df[['array_turn_position']]).tolist()


########   
for y in range(len(app_item_array)):
#for y in range(1):
	for t in range(no_of_intervals_turn):
		turn_int_number = t+1
		print (turn_int_number)
		relative_flow_interval = 'RelFlow('+str(turn_int_number)+')'
		# print (turn_intersection_unique_nm_array[y][0])
	 #    print (turn_approach_array[y][0])
	 #    get_turn_array = getTurn(startepoch, simTime, turn_intersection_nm_array[y][0], turn_approach_array[y][0])
	 #    t_position = turn_position_key_array[y][0]
		int_col_name = (turn_int_number-1)*600
		turn_percentage = turn_count_key_df.iloc[y][int_col_name]
		print (turn_percentage)
		Vissim.Net.VehicleRoutingDecisionsStatic.ItemByKey(app_item_array[y][0]).VehRoutSta.ItemByKey(turn_item_key_array[y][0]).SetAttValue(relative_flow_interval, turn_percentage)


# ############## SAVING THE FILE ############################################
Filename = os.path.join(Path_of_COM_Basic_Commands_network, 'MLKCorridor_24hr_VOL_TURN_Oct6_v2.inpx')
Vissim.SaveNetAs(Filename)
Filename = os.path.join(Path_of_COM_Basic_Commands_network, 'MLKCorridor_24hr_VOL_TURN_Oct6_v2.layx')
Vissim.SaveLayout(Filename)
