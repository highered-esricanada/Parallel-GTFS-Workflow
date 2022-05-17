"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada.
Date: Remodified Q1 - 2022

About: Identify which raw GTFS-RT csv files need to be processed based on it not being labelled complete.
	   Read static GTFS files relevant to the raw GTFS-RT csv file.
	   Create a dataframe filled with trip_ids that match with the undissolved, dissolved, and stop shapefiles.

Note: The Ingestion Class requires to be in a for loop.

"""

from ..util import discover_docs
from pandas import DataFrame, read_table, read_csv, to_datetime
from numpy import where


class NeedProcess:

	def __init__(self, main_folder):
		"""
		Identify which raw GTFS-RT csv file needs to be processed based on it not being labelled complete.
		Additionally, it will be used to identify which static GTFS files need to be read. 

		:params main_folder: The folder hosting raw and static GTFS files (e.g., '../data/0_external/GTFS')
		"""

		self.csv_inf = self._csv_need_process(main_folder=main_folder)


	def _csv_need_process(self, main_folder) -> DataFrame:
		"""
		:params main_folder: The main folder that hosts raw GTFS-RT csv files (i.e., ../data/0_external/GTFS)

		:returns: Dataframe with path, filename, and directory of the raw GTFS-RT csv files that need to be processd.
		"""

		csv_inf = (
			discover_docs(path=main_folder)
		    [['path', 'filename', 'directory']]
		    .assign(is_txt = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "txt").astype(int), 
		            is_csv = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "csv").astype(int), 
		            is_complete = lambda r: (r['filename'].str.contains('(?i).*complete.*')), 
		            folder_date = lambda r: (r['directory'].str.extract(r'([0-9]{4}\-[0-9]{1,2}\-[0-9]{1,2})', expand=False)).pipe(to_datetime), 
                    raw_date    = lambda r: (r['filename'].str.extract(r'([0-9]{4}\-[0-9]{1,2}\-[0-9]{1,2})',  expand=False)).pipe(to_datetime))
		    .assign(folder_date = lambda r: r['folder_date'].dt.strftime('%Y-%m-%d'),  
		    		raw_date    = lambda r: r['raw_date'].dt.strftime('%Y-%m-%d'))
		    .query('is_csv == 1 and is_complete == False')
		)

		return csv_inf


class Ingestion:

	def __init__(self, individual_csv_df):
		"""
		Identify which static GTFS files need to be read based on the same directory where the raw GTFS-RT csv file is located.
		Read GTFS-RT file that needs to be processed and appropriate static GTFS files.
		Create dataframe that matches each trip_id to the dissolved & undissolved routes and transit stops shapefiles.

		:params individual_csv_df: Individual row containing information about the GTFS-RT csv file that needs to be processed.
		"""

		### WARNING - DO NOT CHANGE THIS ORDER IN THE LIST ###	
		self.rel_files = ['trips.txt', 'shapes.txt', 'routes.txt', 'stops.txt', 'stop_times.txt']
		self.exp_df    = self._mainProcess(individual_csv_df=individual_csv_df)


	def _txt_need_read(self, gtfs_rt_folder) -> DataFrame:
		"""
		Identify which static GTFS files need to be read based on the same directory where the raw GTFS-RT csv file is located.

		:params gtfs_rt_folder: The directory folder of where the raw GTFS-RT csv file is located for processing.
		:params rel_files: The relevant static GTFS files. 

		:returns: Dataframe with path, filename, and directory of the static GTFS files that will be read. 
		"""

		txt_inf = (
			discover_docs(path=gtfs_rt_folder)
			[['path', 'filename', 'directory']]
			.assign(is_txt = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "txt").astype(int))
			.query('is_txt == 1 and filename in @self.rel_files')
		)

		return txt_inf
	

	def _link_rt_static(self, rt_csv, txt_inf:DataFrame, rel_files, folder_date):
		"""
		Read GTFS-RT file that needs to be processed and appropriate static GTFS files.
		Create dataframe that matches each trip_id to the dissolved & undissolved routes and transit stops shapefiles.

		:params rt_csv: The path to the raw GTFS-RT csv file to be 
		:params txt_inf: Based on the folder where the raw GTFS-RT csv file is hosted, get the path, filename, and directory of each static GTFS file.
		:params rel_files: A list of the relevant static GTFS file.
		:params folder_date: The date that the GTFS static file & RT is to be processed. 

		:returns: file_explorer -> Specific file to read that matches the specific route that matches the trip_id
				  rt_df         -> DataFrame of the raw GTFS-RT csv file. 
				  trips  	    -> DataFrame of trips.txt
				  shapes 	    -> DataFrame of shapes.txt
				  routes 	    -> DataFrame of routes.txt
				  stops  	    -> DataFrame of stops.txt
				  stop_times    -> DataFrame of stop_times.txt  
		"""

		# To reduce "verboseness" when searching for a specific text file.
		dict_file = {
			"trips" 	 : txt_inf.query('filename == @rel_files[0]').path.iloc[0],
			"shapes"	 : txt_inf.query('filename == @rel_files[1]').path.iloc[0],
			"routes"	 : txt_inf.query('filename == @rel_files[2]').path.iloc[0],
			"stops" 	 : txt_inf.query('filename == @rel_files[3]').path.iloc[0],
			"stop_times" : txt_inf.query('filename == @rel_files[4]').path.iloc[0]
		}

		print('Ingestion Process - Reading relevant static GTFS files and raw GTFS-RT.')

		# Read relevant static GTFS files
		trips 	   = read_table(dict_file['trips'], sep=",")
		shapes 	   = read_table(dict_file['shapes'], sep=",")
		routes 	   = read_table(dict_file['routes'], sep=",")
		stops 	   = read_table(dict_file['stops'], sep=",")
		stop_times = read_table(dict_file['stop_times'], sep=",")
		
		# Read raw GTFS-RT csv file
		rt_df 	   = (
			read_csv(rt_csv)
				.assign(Uniquer = lambda l: l['Trip_ID'].astype(str) + "-" + 
											l['Vehicle_ID'].astype(str) + "-" + 
											l['Lat'].astype(str) + ";" + 
											l['Lon'].astype(str) + "-" + 
											l['Local_Time'])
		)

		# Get unique tripids 
		unique_tripid = rt_df.Trip_ID.unique()

		trips_df = (
			trips
				.assign(Direction = lambda r: where(r['direction_id'] == 1, "Inbound", "Outbound"), 
						UniqueInf = lambda r: r['route_id'] + "-" + r['shape_id'].astype(str))
		)

		# Get the unique transit routes
		#unique_trips = trips_df.UniqueInf.unique()

		print(f"Match each unique trip id and get essential information (route_id, direction, shape_id, and route/data files).")

		rte_folder = f"../data/2_staging/{folder_date}/Routes/"
		stp_folder = f"../data/2_staging/{folder_date}/Stops/"

		# Specific file to read that matches the specific route that matches the trip_id
		file_explorer = (
			trips_df
				.query('trip_id in @unique_tripid')
				[['trip_id', 'route_id', 'direction_id', 'shape_id', 'UniqueInf']]
				.replace({'direction_id' : {0 : "Outbound", 1: "Inbound"}})
				.assign(Rte_ID 	   = lambda r: r['route_id'].str.split('-').str[0] + "-" + r['shape_id'].astype(str), 
						Undiss_Rte = lambda r: rte_folder + r['direction_id'] + "/" + r['Rte_ID'] + "_Routes.shp", 
						Diss_Rte   = lambda r: rte_folder + r['direction_id'] + "/" + r['Rte_ID'] + "_Routes_dissolved.shp", 
						Stop       = lambda r: stp_folder + r['direction_id'] + "/" + r['Rte_ID'] + "_Stops.shp", 
						UniqueRte  = lambda r: r['Rte_ID'] + "-" + r['direction_id'], 
						Alt_Undiss_Rte = lambda r: rte_folder + r['direction_id'] + "/" + r['route_id'] + "_Routes.shp", 
						Alt_Diss_Rte  = lambda r: rte_folder + r['direction_id'] + "/" + r['route_id'] + "_Routes_dissolved.shp",
						Alt_Stop      = lambda r: rte_folder + r['direction_id'] + "/" + r['route_id'] + "_Stops.shp") 
		)

		return (file_explorer, rt_df, trips, shapes, routes, stops, stop_times)


	def _mainProcess(self, individual_csv_df):
		"""
		Main process that executes two functions - _txt_need_read and _link_rt_static.

		:params individual_csv_df:
		"""

		txt_inf   = self._txt_need_read(gtfs_rt_folder=individual_csv_df.directory)
		use_files = self._link_rt_static(rt_csv=individual_csv_df.path, 
				 	   			   		 txt_inf=txt_inf, 
					   			   		 rel_files=self.rel_files, 
					   			   		 folder_date=individual_csv_df.folder_date)
		return use_files