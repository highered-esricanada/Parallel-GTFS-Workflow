"""
Date: Remodified Q1 - 2022

About: Iteratively harvests GTFS-RT, converts to proper dataframe structure, and appends to a csv file. 

This is a main function and where users input their own parameters. 

Dependencies:
	1) Google Transit installed -> pip install --upgrade gtfs-realtime-bindings

USER NOTES - MUST READ: 
	1) Check to make sure that the URL to the GTFS-RT downloads as a PB file. 
	2) Works best if GTFS-RT updates less than a minute - ideally less than or equal to 30 seconds.
	3) Ensure that the GTFS-RT has the following schema: vehicle_id, trip_id, lat, lon, and timestamp. 


Below are required parameters, for backend details refer to the directory: gtfs_harvester/gtfs_converter.py 

:params url: The url to download GTFS-RT pb file. 
:params city: The name of the city you are extracting GTFS-RT from. This is to name the CSV file.
:params hours: The number of hours for the harvester to run throughout the day.
			   Contingent on the frequency of the GTFS-RT update (i.e., throttle)
:params throttle: Pauses the harvester in x seconds - this is contingent on how often the 
				  GTFS-RT updates (e.g., Calgary updates every 30 seconds; Boston every 5 seconds.)
:params output_directory: The output directory to store the csv file that collects raw GTFS-RT from.
					      Highly recommended to keep it the way it is - the rest of the pipeline depends on it.
"""

from gtfs_harvester import ExtractGTFSRT


harvest = ExtractGTFSRT(url="https://data.calgary.ca/download/am7c-qe3u/application%2Foctet-stream", 
		     			city="Calgary", 
					    hours=14, 
					    time_zone="America/Edmonton",
					    throttle=30, 
					    output_directory="../data")