import pandas as pd
import xlrd
import numpy as np
import json
import psycopg2
#from sqlalchemy import create_engine
#from sqlalchemy.dialects.postgresql import insert
import time
import math 
from geopy import distance
import datetime
import logging
import os
#import sqlalchemy as sa
import traceback

today = datetime.date.today()
today = today.strftime("%Y-%m-%d")
yesterday = str(datetime.date.today() - datetime.timedelta(days = 12))

logging.basicConfig(filename='/home/azureuser/python_files/webscraping/gps_data_cleaning_log.log',level=logging.INFO,format='%(asctime)s:%(funcName)s:%(levelname)s:%(message)s')

while(True):
	try:

		#try:
		conn1 = psycopg2.connect(dbname="shreeji",user="ezyloads", host="127.0.0.1", password="ezy@1234")
		cur1 = conn1.cursor()


		# def upsert(table, conn, keys, data_iter):
		# 	upsert_args = {"constraint": "unique_id"}
		# 	for data in data_iter:
		# 		data = {k: data[i] for i, k in enumerate(keys)}
		# 		upsert_args["set_"] = data
		# 		insert_stmt = insert(meta.tables[table.name]).values(**data)
		# 		upsert_stmt = insert_stmt.on_conflict_do_nothing()
		# 		conn.execute(upsert_stmt)

		def calculate_distance(lat1,long1,lat2,long2):
			earth_radius = 6371 # Radius of the earth
			lat_distance = math.radians(lat2 - lat1)
			long_distance = math.radians(long2 - long1)
			a = math.sin(lat_distance / 2) * math.sin(lat_distance / 2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(long_distance/ 2) * math.sin(long_distance / 2)
			c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
			distance = earth_radius * c * 1000; # convert to meters
			distance = math.pow(distance, 2)
			distance = math.sqrt(distance)
			return round(distance,2)

		#try:
		jsonFile = open("/home/azureuser/python_files/webscraping/gps_meta_data.json", "r") # Open the JSON file for reading
		data = json.load(jsonFile) # Read the JSON into the buffer
		jsonFile.close() # Close the JSON file
		#print(data)
		start_id = data["record"]["first_id"]
		end_id = data["record"]["last_id"]
		fetching_time = data["record"]["last_record_time"]
		# except:
		# 	fetching_time = str(datetime.datetime.now() - datetime.timedelta(minutes=5))
		#try:

		total_trips="""select * 
			FROM shreeji where
			 time::timestamp > '{}'
			-- time::timestamp between '2022-06-01 18:30:00' and '2022-06-02 18:30:00' 
		order by time desc
	""".format(fetching_time)
		cur1.execute(total_trips)
		
		result = cur1.fetchall()
		

		last_record = """select distinct on (deviceid) deviceid, time::timestamp, latitude,longitude,speed
		from shreeji_cleaned_test
		order by deviceid, time::timestamp desc;"""  #.format(yesterday)

		cur1.execute(last_record)
		#print(last_record)
		latest_result = cur1.fetchall()

		# except Exception as e:
		# 	logging.info("An exception was thrown to modified!", exc_info=True)
		# 	print(e)


		gps_data=pd.DataFrame(result,columns=["id","vendor_name","trip_id","vehicle_number","imei","speed","latitude","longitude","location","location_update_time","gps_accuracy","gsm_signal_strength","travel_time","stopped_time","distance","battery_percentage","ignition_status","angle","altitude","heading","fuel","status","status_id","time","odometer","in_hub","closest_hub","geolocation","deviceid","time_stamp"])
		latest_result=pd.DataFrame(latest_result,columns=["deviceid","time","latitude","longitude","speed"])
		print("len of result",len(gps_data))
		## Working with buffered content
		if len(gps_data) > 0:

			latest_result['time'] = pd.to_datetime(latest_result['time']).dt.strftime('%Y-%m-%dT%H:%M:%S')
			#latest_result = latest_result.sort_values(['vehicle_number','time']).drop_duplicates(subset = ['vehicle_number'],keep = 'last')

			tmp = data["record"] 
			data["record"]["first_id"] = str(end_id)
			last_id = len(gps_data)-1
			data["record"]["last_id"] = str(gps_data.loc[0,"id"])
			data["record"]["last_record_time"] = str(gps_data.loc[0,"time"])

			with open("/home/azureuser/python_files/webscraping/record.txt", 'a') as f:
				f.write(str(gps_data.loc[0,"time"]))
				f.write('\n')
			## Save our changes to JSON file
			jsonFile = open("/home/azureuser/python_files/webscraping/gps_meta_data.json", "w+")
			#print("data to be dumped:",data)
			jsonFile.write(json.dumps(data))
			jsonFile.close()#

		print(gps_data.deviceid)
		unique_vehicles = gps_data.deviceid.unique()
		print("unique_vehicles:",unique_vehicles)
		tmp_gps_data = gps_data.copy()

		for vehicle in unique_vehicles:
			tmp_gps_data = gps_data[gps_data['deviceid'] == vehicle]
			tmp_latest_data = latest_result[latest_result['deviceid']==vehicle]
			tmp_latest_data.reset_index(drop=True,inplace=True)
			

		#Rule 1--> to eliminate all the location points with less than 0
			tmp_gps_data['latitude'] = tmp_gps_data['latitude'].astype(float)
			tmp_gps_data['longitude'] = tmp_gps_data['longitude'].astype(float)
			tmp_gps_data = tmp_gps_data[tmp_gps_data['latitude'] > 0]
			tmp_gps_data = tmp_gps_data[tmp_gps_data['longitude'] > 0] 
			print("rule --> 1 ingestion data",len(tmp_gps_data))

		#Rule to calculate distance
			tmp_gps_data['time'] = pd.to_datetime(tmp_gps_data['time'])
			tmp_gps_data = tmp_gps_data.sort_values(by='time')
			first_record = 0
			index_list = []
			index_list = tmp_gps_data.index.values
			print("index_list:",len(index_list))
			tmp_gps_data["distance"] = tmp_gps_data["distance"].astype(float)

			for ind in range(0,len(index_list)):
				if first_record == 0:
					first_record = 1
					print("************************************************")
					try:
					#print("Record size from cleaned:",tmp_latest_data)
					#print("tmp_latest_data.loc[0,'latitude']:",type(tmp_latest_data["latitude"]))
					#print("tmp_gps_data.loc[index_list[ind],'longitude']",tmp_gps_data.loc[index_list[ind],"longitude"])
						tmp_gps_data.loc[index_list[ind],"distance"] = (distance.great_circle((round(float(tmp_latest_data.loc[0,"latitude"]),5),round(float(tmp_latest_data.loc[0,"longitude"]),5)),(round(float(tmp_gps_data.loc[index_list[ind],"latitude"]),5),round(float(tmp_gps_data.loc[index_list[ind],"longitude"]),5)))).meters
						
					except:
						tmp_gps_data.loc[index_list[ind],"distance"] = 0
				else:
					tmp_gps_data.loc[index_list[ind],"distance"] = (distance.great_circle((round(float(tmp_gps_data.loc[index_list[ind - 1],"latitude"]),5),round(float(tmp_gps_data.loc[index_list[ind - 1],"longitude"]),5)),(round(float(tmp_gps_data.loc[index_list[ind],"latitude"]),5),round(float(tmp_gps_data.loc[index_list[ind],"longitude"]),5)))).meters
					#tmp_gps_data.loc[index_list[ind],"distance"] =  round(calculate_distance(round(float(tmp_gps_data.loc[index_list[ind - 1],"latitude"]),5),round(float(tmp_gps_data.loc[index_list[ind - 1],"longitude"]),5),round(float(tmp_gps_data.loc[index_list[ind],"latitude"]),5),round(float(tmp_gps_data.loc[index_list[ind],"longitude"]),5)),2)
				
			#print("Distance: \n",tmp_gps_data[["latitude","longitude","time","distance"]])
			print("Record size after:",len(tmp_gps_data))
			#print(tmp_gps_data.distance.sum())
			#break
			#Rule 2--> find the speed from d/t
			if len(tmp_gps_data) == 1:
				try:
					tmp_gps_data.reset_index(drop=True,inplace=True)
					tmp_latest_data.reset_index(drop=True,inplace=True)

					tmp_gps_data['distance'] = tmp_gps_data['distance'].astype(float)
					tmp_latest_data['time'] = pd.to_datetime(tmp_latest_data['time'], format='%Y-%m-%d %H:%M:%S')
					tmp_gps_data['time'] = pd.to_datetime(tmp_gps_data['time'], format='%Y-%m-%d %H:%M:%S')

					tmp_gps_data.loc[0,'travel_time'] = (tmp_gps_data.loc[0,'time'] - tmp_latest_data.loc[0,'time']).total_seconds()
					tmp_gps_data['calc_speed'] =(tmp_gps_data.loc[0,'distance']/tmp_gps_data.loc[0,'travel_time'])
					tmp_gps_data = tmp_gps_data[tmp_gps_data['calc_speed'] <= 100]
					tmp_gps_data.drop(columns=['calc_speed'],inplace=True,axis=1)
				# if [tmp_gps_data['vendor_name'].unique()][0] == 'suveechi':
				# 	tmp_gps_data['speed'] =(tmp_gps_data.loc[0,'distance']/tmp_gps_data.loc[0,'travel_time'])
				except Exception as e:
					#print("when the record is 1:",e)
					#If we don't find previous record of the selected vehicle.
					tmp_gps_data.loc[0,'travel_time'] = 0
					tmp_gps_data.loc[0,'speed'] = 0
			else:
				tmp_gps_data['distance'] = tmp_gps_data['distance'].astype(float)
				#tmp_gps_data['delta_distance'] = tmp_gps_data.distance.diff().shift(0)
				tmp_gps_data['time'] = pd.to_datetime(tmp_gps_data['time'], format='%Y-%m-%d %H:%M:%S')
				tmp_gps_data['travel_time'] = tmp_gps_data['time'].squeeze().diff().dt.seconds.fillna(0)
				tmp_gps_data['calc_speed'] =(tmp_gps_data['distance']/tmp_gps_data['travel_time']).fillna(0)
				tmp_gps_data = tmp_gps_data[tmp_gps_data['calc_speed'] <= 100] 
				tmp_gps_data.drop(columns=['calc_speed'],inplace=True,axis=1)
				# if [tmp_gps_data['vendor_name'].unique()][0] == 'suveechi':
				# 	tmp_gps_data['speed'] =(tmp_gps_data['distance']/tmp_gps_data['travel_time']).fillna(0)
				#print("rule --> 2 ingestion data",tmp_gps_data)
			#Rule 3--> gps accuracy <= 30
			# tmp_gps_data = tmp_gps_data[tmp_gps_data['gps_accuracy'] <= 30]
			# print("rule --> 3 ingestion data",tmp_gps_data)

			#tmp_gps_data = tmp_gps_data.drop(columns=["delta_time"],axis = 1)
			#print("speed:",tmp_gps_data['speed'].head())
			#raise("stop")
			#tmp_gps_data.to_sql('shreeji_cleaned', engine,if_exists = 'append',method=upsert,index= False)
			#break
			print("duplicated:\n",tmp_gps_data[tmp_gps_data.duplicated(['deviceid','time'])])
			for i in tmp_gps_data.index:
				print(tmp_gps_data.loc[i,'deviceid'],tmp_gps_data.loc[i,'time'])
				query_to_insert = """insert into shreeji_cleaned_test(travel_time,speed,vendor_name,vehicle_number,imei,ignition_status,location,latitude,longitude,angle,altitude,distance,fuel,status_id,status,time,deviceId) values('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}') 
				 on conflict (deviceid,time) do nothing """.format(tmp_gps_data.loc[i, 'travel_time'],
																   tmp_gps_data.loc[i,'speed'],
																   tmp_gps_data.loc[i,'vendor_name'],
																   tmp_gps_data.loc[i,'vehicle_number'],
																   tmp_gps_data.loc[i,'imei'],
																   tmp_gps_data.loc[i,'ignition_status'],
																   tmp_gps_data.loc[i,'location'],
																   tmp_gps_data.loc[i,'latitude'],
																   tmp_gps_data.loc[i,'longitude'],
																   tmp_gps_data.loc[i,'angle'],
																   tmp_gps_data.loc[i,'altitude'],
																   tmp_gps_data.loc[i,'distance'],
																   tmp_gps_data.loc[i,'fuel'],
																   tmp_gps_data.loc[i,'status_id'],
																   tmp_gps_data.loc[i,'status'],
																   tmp_gps_data.loc[i,'time'],
																   tmp_gps_data.loc[i,'deviceid'])
				print(query_to_insert)
				cur1.execute(query_to_insert)
				conn1.commit()

			tmp_gps_data = tmp_gps_data[0:0]
			tmp_latest_data = tmp_latest_data[0:0]
			
			time.sleep(1)
	except Exception as e: 
		print("-------------------------------------exception--------------------------------------------------------------",e)
		traceback.print_exc()
		logging.info("An exception was thrown to modified!", exc_info=True)
		#raise("stop")
	finally:
		cur1.close()
		conn1.close()
	print("------------end---------------------------")
	time.sleep(300)