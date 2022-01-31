"""
Date: Remodified Q1 - 2022

About: Iteratively harvests GTFS-RT, converts to proper dataframe structure, and appends to a csv file. 

Requires:
	1) Google Transit installed -> pip install --upgrade gtfs-realtime-bindings

USER NOTES - MUST READ: 
	1) Check to make sure that the URL to the GTFS-RT downloads as a PB file. 
	2) Works best if GTFS-RT updates less than a minute - ideally less than or equal to 30 seconds.
	3) Ensure that the GTFS-RT has the following schema: vehicle_id, trip_id, lat, lon, and timestamp. 
	4) Ensure that the timestamp being collected is originally UTC zone - check line 64, under tzinfo parameter.
"""

from google.transit import gtfs_realtime_pb2
from datetime import datetime 
import requests
import time
import pytz
import os.path
from pandas import DataFrame, to_datetime
from tqdm import tqdm 


class ExtractGTFSRT:

	def __init__(self, url, city, hrs_collect, time_zone, throttle, output_directory):
		"""
		:params url: The url to download GTFS-RT pb file. 
		:params city: The name of the city you are extracting GTFS-RT from. 
		:params hrs_collect: The number of hours for the harvester to run throughout the day.
						Contingent on the frequency of the update (i.e., throttle).
		:params time_zone: The time zone of the study area for Pytz. Type pytz.all_timezones to find proper zone.
		:params throttle: Pause the function in seconds - this is contingent on how often the 
						  GTFS-RT updates (e.g., Calgary every 30 seconds; Boston every 5 seconds.)
		:params output_directory: The output directory to store the csv file that collects raw GTFS-RT from. 
		"""

		self._extracter(url=url, 
						city=city, 
						hrs_collect=hrs_collect, 
						time_zone=time_zone, 
						throttle=throttle, 
						output_directory=output_directory)


	def _time_converter(self, time_stamp, time_zone, time_format):
		"""
		Dependent function - only used in self._extracter 
		Converts utc_time to proper time format, and utc_time to local time in proper format.

		:params time_stamp: Time stamp acquired from feed entity. 
		:params time_zone: Defined time_zone of the study area. 
		:params time_format: Defined format for time - used to convert. 

		:returns: Tuple - utc_time and loc_time in string format. 
		"""

		utc_time = time_stamp.strftime(time_format)
		loc_time = (
					time_stamp
						.replace(tzinfo=pytz.utc)
						.astimezone(pytz.timezone(time_zone))
						.strftime(time_format)
		)

		return (utc_time, loc_time)


	def _extracter(self, url, city, hrs_collect, time_zone, throttle, output_directory):
		"""
		For what each parameter means, refer to def __init__. 
		This function extracts entities from the GTFS-RT feed - uses self._time_extractor function.
		
		:returns: Log file (listing errors happening, if applicable) and csv file (appends parsed GTFS-RT every iteration)
		"""

		feed 	 = gtfs_realtime_pb2.FeedMessage()
		log_file = f"{str(datetime.now()).split(' ')[0]}_errors.log" 
		logs 	 = open(f"{output_directory}/{log_file}", "a")

		# Calculate the iterator - sets as a runtime for the harvester. 
		# For example, a GTFS-RT update is every 30 seconds and you want to collect for 12 hrs. per day:
		#			  Iterator = (60 sec. / update frequency) * 60 (min/hr) * hrs to collect   		
		#			  iterator = (60 sec. / 30 sec. for calgary) * 60 (min/hr) * 12 to collect
		iterator = round((60 / throttle) * 60 * hrs_collect)


		# Collect over time based on defined timer.
		for i in tqdm(range(0, iterator, 1)):
			try: 
				response = requests.get(url)
				feed.ParseFromString(response.content)
				tmp_dict = {'ID':[], 'Trip_ID':[], 'Vehicle_ID':[], 
				 		    'Lat':[], 'Lon':[], 
				            'UTC_Time':[], 'Local_Time':[]}
				try: 
					# Parse out entities from the feed - timestamp, vehicle_id, trip_id.
					for count, value in enumerate(feed.entity):
						tmp_entity  = feed.entity[count]
						time_stamp  = datetime.utcfromtimestamp(feed.entity[count].vehicle.timestamp)
						time_format = '%Y-%m-%d %H:%M:%S'

						# Entities from transit agency (vehicle_id, trip_id)
						tmp_dict['ID'].append(tmp_entity.id)
						tmp_dict['Trip_ID'].append(tmp_entity.vehicle.trip.trip_id)
						tmp_dict['Vehicle_ID'].append(tmp_entity.vehicle.vehicle.id)

						# Keeping original timestamp and converting to proper timezone
						time_converters = self._time_converter(time_stamp=time_stamp, 
															   time_zone=time_zone, 
															   time_format=time_format)

						tmp_dict['UTC_Time'].append(time_converters[0])
						tmp_dict['Local_Time'].append(time_converters[1])						

						# Acquiring geographic location
						tmp_dict['Lat'].append(tmp_entity.vehicle.position.latitude)
						tmp_dict['Lon'].append(tmp_entity.vehicle.position.longitude)

					# Check to make sure the feed entity has length greater than zero & construct DataFrame 
					if len(feed.entity) > 0:
						calg_df = (
									DataFrame.from_dict(tmp_dict)
										.assign(Year   = lambda r: r['Local_Time'].pipe(to_datetime).dt.year, 
												Month  = lambda r: r['Local_Time'].pipe(to_datetime).dt.month, 
												Day    = lambda r: r['Local_Time'].pipe(to_datetime).dt.day, 
												Hour   = lambda r: r['Local_Time'].pipe(to_datetime).dt.hour, 
												Minute = lambda r: r['Local_Time'].pipe(to_datetime).dt.minute, 
												Second = lambda r: r['Local_Time'].pipe(to_datetime).dt.second)
						)

						timestamp_file = f"GTFSRT_{city}_{calg_df['Year'].iloc[0]}-{calg_df['Month'].iloc[0]}-{calg_df['Day'].iloc[0]}.csv"
						output_loc     = f"{output_directory}/{timestamp_file}"

						if os.path.isfile(output_loc):
							calg_df.to_csv(output_loc, mode='a', index=False, header=False)
						else:
							calg_df.to_csv(output_loc, mode='a', index=False)

					else:
						logs.write(f'No entities detected at iteration: {i}\n')
		
				except Exception as e:
					logs.write(f'Failed to process GTFS-RT at iteration: {i}\n')

			except Exception as e: 
				logs.write(f'Failed to retrieve GTFS-RT at iteration: {i}\n - check url.')

			time.sleep(throttle)
			i += 1
