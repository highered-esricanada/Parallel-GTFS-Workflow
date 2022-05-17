"""
Author: Anastassios Dardas, PhD - Higher Education Specialist from Education & Research at Esri Canada 
Date: (New) Q2 - 2022
About: Prior to aggregation processing, parallel process interpolated csv files 
	   to clean unwanted observations. 
"""

from ..util import discover_docs, ParallelPool
from pandas import read_csv, concat, DataFrame
from functools import partial 


class RefineInterp:


	def __init__(self, start_method, L, path, trips_txt: DataFrame): 
		"""
		Clean unwanted observations in all interpolated files in parallel prior
		to aggregation processing. 

		:start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn".
		:param L: The list that is part of the Manager in Multiprocessing 
					report file error that needs to be assessed. 
		:param path: The path containing the list of interpolated csv files, 
					 typically found in "../data/5_conformed/{date_folder}/{raw_date}".
		:param trips_txt: The trips.txt file to be merged with the interpolated. 
		"""
		
		self._mainprocess(start_method=start_method,
		                  L=L,
		                  path=path,
		                  trips_txt=trips_txt)


	def _filt_df(self, pipe_df: DataFrame) -> DataFrame:
		"""
		Filters out unwanted observations that may distort aggregation 
		calculations - specifically beginning and terminus stops that may be
		geographically incorrect. 

		:param pipe_df: The dataframe processed in memory passed via pipe function.

		:return: Concatenated dataframe that retains only correct observation. 
		"""

		stp_min = sorted(pipe_df['stp_min'].unique()) # index values - correct min
		stp_max = sorted(pipe_df['stp_max'].unique()) # index values - correct max
		return concat([pipe_df.iloc[i:ii+1,:] for i,ii in zip(stp_min, stp_max)])


	def _clean_df(self, csv, L, trips_txt: DataFrame):
		"""
		Cleaning process - remove unwanted observations including illogical 
		observations that have very high speed and estimated extreme arrival times 
		whether at least 20 min. early or late. 

		Additionally, retain values that are in order from stop sequence in 
		relation to the trending idx value. 

		| idx | stop_seque | Max Stop Seq. | Retain |
		| --- | ---------- | ------------- | ------ | 
		|  1  |      2     |      30       |   Yes  | 
		|  2  |      3     |      30       |   Yes  | 
		| ... | .......... | ............. | ...... | 
		| 18  |     30     |      30       |   Yes  | 
		| 19  |      2     |      30       |   No   | 

		You can see that idx (vehicle movement of the trip_id) 19 ends up stop 
		seque 2, which does not make sense. This likely indicates that the route
		is a loop and when it reaches at the terminus (e.g., stop sequence 30), 
		it may overlap with stop sequence 2. This needs to be removed because it 
		will give estimations that are not applicable anymore. 

		:param csv: The interpolated csv file in the 
					"../data/5_conformed/{date_folder}/{raw_date}". 
		:param L: The list that is part of the Manager in Multiprocessing 
					report file error that needs to be assessed. 
		:param trips_txt: The trips.txt file to be merged with the interpolated 

		:return: If successful, new csv file; otherwise, report error. 
		"""

		try:

			df = (
				read_csv(csv)
					# Keep observations that seem logical 
					.query('proj_speed < 110 and off_arrdif > -1200 \
							and off_arrdif < 1200')
					.merge(trips_txt, on=['trip_id'], how='left')
					.groupby(['route_id', 'trip_id'], as_index=False)
					# For stp_max = find the very last index value where the max stop sequence ends. 
					# For stp_min = find the very first index value where the min stop sequence begins.
					.apply(lambda e: e
						.assign(stp_max = e.drop_duplicates(['stop_seque'], keep="last")['stop_seque'].idxmax(), 
								stp_min = e.drop_duplicates(['stop_seque'], keep="first")['stop_seque'].idxmin()))
					.pipe(lambda d: self._filt_df(d))
					.drop(columns=['stp_min', 'stp_max'])
					.reset_index()
			)

			file_name = f"{csv[:-4]}_cleaned.csv"

			df.to_csv(file_name, index=False)

		except Exception as e:
			L.append(f"Error,{csv}")
			pass


	def _mainprocess(self, start_method, L, path, trips_txt: DataFrame):
		"""
		The main process to clean, filter, and concat final output per interpolated csv file. 
		This entire process is done in conventional parallel processing. 

		:start_method: The method to initiate - typically in Linux -> "fork"; Windows -> "spawn".
		:param L: The list that is part of the Manager in Multiprocessing 
					report file error that needs to be assessed. 
		:param path: The path containing the list of interpolated csv files, 
					 typically found in "../data/5_conformed/{date_folder}/{raw_date}".
		:param trips_txt: The trips.txt file to be merged with the interpolated.
		"""

		csv_files = (
			discover_docs(path=path)
			[['path', 'filename', 'directory']]
			.assign(is_csv=lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "csv").astype(int))
			.query('is_csv == 1')
		)

		trips_txt = (
			trips_txt
				.assign(prt_rte=lambda d: d['route_id'].astype(str).str.split("-").str[0],
			            new_rte=lambda d: d['prt_rte'] + "-" + d['shape_id'].astype(str))
				[['new_rte', 'trip_id', 'direction_id']]
				.rename(columns={'new_rte': 'route_id'})
		)

		partial_func = partial(self._clean_df, L=L, trips_txt=trips_txt)
		main_list    = csv_files['path'].tolist()

		ParallelPool(start_method=start_method,
					 partial_func=partial_func,
					 main_list=main_list)