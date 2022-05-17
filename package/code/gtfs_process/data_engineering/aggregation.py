"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at the Education & Research Group at Esri Canada.

Date: Remodified - Q2-2022.

About: Finalizes the interpolated (cleaned version) results and aggregates based on the following operations:
		1) Perform per route_id, trip_id, stop_seque, and sched_arr to get sum of observations per on-time performance
		   value (Late, Early, and On-Time). Found in perf_df = self._aggOnTime(tmp_df=tmp_df)

		Points 2 - 4 are the big aggregation operations.
		2) Use the interpolated (cleaned) csv file and the aggregation from point 1 to do a general aggregation
		   per route per trip_id per stop_seque & stop_id and sched_arr.

		3) Use the outcome from point 2 to aggregate into an hourly basis per route per stop_seque per hour.

		4) Use the outcome from point 3 to aggregate into a daily basis per route per stop_seque.

		Schema outcomes are provided in this script for all points.
"""

from pandas import read_csv, crosstab, Series, to_datetime, concat, DataFrame
from ..util import ParallelPool, discover_docs
from functools import partial
from arcgis.features import GeoAccessor


class AggResults:

	def __init__(self, start_method, path, shp_path, analyses_folder, requests_folder, L, L2, L3):
		"""
		:param start_method: The start method to instantiate parallel processing (Linux -> "fork" except using ArcGIS (use "spawn"); Windows -> "spawn").
		:param path: The path to the folder (5_conformed/date_folder/day_folder) containing the clean interpolated csv files.
		:param shp_path: The path to where the GTFS shapefiles have been created and stored. Used to merge & acquire spatial comp.
		:param analyses_folder: The main path where the first aggregation will be exported and stored somewhere in the 6_analyses folder.
		:param requests_folder: The main path where the final aggregations (hr & daily) will be exported and stored somewhere in the 7_requests folder.
		:param L: List manager to collect main_agg. Used to concat afterwards (when parallel is complete)
				  and export as csv and shapefile.
		:param L2: List manager to collect agg_rte_hrly. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		:param L3: List manager to collect agg_rte_daily. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		"""

		# Get list of cleaned interpolated csv files.
		csv_files = (
			discover_docs(path=path)
			[['path', 'filename', 'directory']]
			.assign(is_csv=lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "csv").astype(int),
			        is_clean=lambda r: r['filename'].str.contains('cleaned').astype(int),
                    rte_name=lambda r: r['filename'].str.split('_').str[1].str.split("-").str[0:2].str.join("-"))
			        #rte_name=lambda r: r['filename'].str.split("-").str[0:2].str.join('-'))
			.query('is_csv == 1 and is_clean == 1')
		)

		# Get list of GTFS shapefiles to merge downstream & acquire spatial component.
		shp_files = (
			discover_docs(path=shp_path)
			[['path', 'filename', 'directory']]
			.assign(is_shp = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "shp").astype(int),
			        rte_name = lambda r: r['filename'].str.split("_").str[0],
			        dissolved = lambda r: r['filename'].str.contains('dissolved').astype(int))
			.query('is_shp == 1 and dissolved == 1')
		)

		merge_files = (
			csv_files
				.merge(shp_files, on=['rte_name'], how='left', validate='1:1')
				.rename(columns={'path_x': 'csv_path',
			                     'filename_x': 'csv_file',
			                     'directory_x': 'csv_directory',
			                     'path_y': 'shp_path',
			                     'filename_y': 'shp_file',
			                     'directory_y': 'shp_directory'})
		)

		main_list    = merge_files['csv_path'].unique()
		partial_func = partial(self._main,
		                       L=L,
		                       L2=L2,
		                       L3=L3,
		                       file_df=merge_files)

		ParallelPool(start_method=start_method,
		             partial_func=partial_func,
		             main_list=main_list)

		# Export contents to geojson - (originally shapefile, but it is not friendly with column formatting & some values).
		analyses_name = f"{analyses_folder}/general_aggregation.geojson"
		hrly_agg      = f"{requests_folder}/hourly_aggregation.geojson"
		daily_agg     = f"{requests_folder}/daily_aggregation.geojson"

		fsetL  = concat(L).spatial.to_featureset()
		fsetL2 = concat(L2).spatial.to_featureset()
		fsetL3 = concat(L3).spatial.to_featureset()

		fset_list = [fsetL, fsetL2, fsetL3]
		fset_names = [analyses_name, hrly_agg, daily_agg]

		for f,ff in zip(fset_list, fset_names):
			with open(ff, "w", encoding='utf-8') as file:
				file.write(f.to_geojson)


	def _main(self, tmp_csv, L, L2, L3, file_df):
		"""

		:param tmp_csv: The individual csv file that is being read and use for aggregation.
		:param L: List manager to collect main_agg. Used to concat afterwards (when parallel is complete)
				  and export as csv and shapefile.
		:param L2: List manager to collect agg_rte_hrly. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		:param L3: List manager to collect agg_rte_daily. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		:param file_df:
		:return: Indirectly in the self._mainAgg via the list managers.
		"""

		# Read csv file & shapefile
		tmp_df  = read_csv(tmp_csv)
		get_shp = file_df.query('csv_path == @tmp_csv')['shp_path'].iloc[0]
		tmp_shp = GeoAccessor.from_featureclass(get_shp)

		# Perform first aggregation - per route_id, trip_id, stop_seque, and sched_arr for
		# on-time performance (Late, Early, On-Time).
		perf_df  = self._aggOnTime(tmp_df=tmp_df)

		# Mega aggregation function - performs three different aggregation operations.
		# Agg. Op. 1 => Appended to L, general aggregation per route per trip_id per stop_seque & stop_id & sched_arr.
		# Agg. Op. 2 => Appended to L2, uses agg. op 1. to aggregate into an hourly basis per route per stop_seque per hour.
		# Agg. Op. 3 => Appended to L3, uses agg. op 2. to aggregate into a daily basis per route per stop_seque.
		self._mainAgg(tmp_df=tmp_df,
		              perf_df=perf_df,
		              L=L, L2=L2, L3=L3,
		              tmp_shp=tmp_shp)


	def _aggOnTime(self, tmp_df):
		"""
		Aggregate dataframe per route_id, trip_id, stop_seque, and sched_arr for the number of observation
        points the vehicle en-transit to the stop is projected to be Early, Late, or On-Time. For example,
        if vehicle A en-route to stop seque #8 and has been recorded & interpolated 6 times that means the
        status could be changing 6 different times; thus, indicating the stability of on-time performance at
        the stop.

		:param tmp_df: The clean interpolated of the csv file.
		:return: Aggregated dataframe with the following schema:
			route_id   => The transit route
			trip_id    => The specific number of the vehicle belonging to the transit route with its own schedule.
			stop_seque => Order sequence number of the transit stop.
			stop_id    => ID number of the transit stop.
			sched_arr  => Expected schedule arrival at the transit stop (stop_id).
			Early      => Number of observations projected to be early.
			Late       => Number of observations projected to be late.
			On-Time    => Number of observations projected to be on-time.
		"""

		grp_cols = ['route_id', 'trip_id', 'idx', 'stop_seque', 'stop_id', 'sched_arr', 'perf_rate']
		perf_df  = (
			tmp_df
				.groupby(grp_cols, as_index=False)
				.agg('count')
				.pipe(lambda e: crosstab([e['route_id'], e['trip_id'], e['stop_seque'], e['stop_id'], e['sched_arr']],
			                             e['perf_rate']))
				.reset_index()
		)

		return perf_df


	def _mainAgg(self, tmp_df, perf_df, L, L2, L3, tmp_shp):
		"""
		Main aggregation function that'll do the following:
			1) main_agg => General aggregation per route per trip_id per stop_seque & stop_id & sched_arr. This will
						   then be used to aggregate per hour and throughout the day - by combining all aggregated
						   trip_ids. The schema is as followed:

						   route_id   => The transit route.
						   trip_id    => The ID related to teh transit route with its own schedule.
 						   stop_seque => Tied to stop_id, the sequence number of the transit stop of the transit route.
						   stop_id    => Identifier of the transit stop of the transit route provided by static GTFS files.
						   sched_arr  => The expected scheduled arrival of that transit stop of the route.
						   off_earr   => The last projected observation (before transitioning to next stop) of arrival
						                 time of the vehicle (trip_id).
						   Lprfrte    => The last projected on-time performance observation (before transitioning to next stop).
						   ref_hr     => Reference hour extracted from sched_arr.
						   AvgSpd     => The overall average projected speed (km/h).
						   Avg_ArrDif => The overall average arrival time difference (km/h).
						   idx        => The cumulative number of vehicle movements - there will be gaps (e.g., 1 -> 3 -> 7).
						                 The gaps indicate that there were multiple recorded (and interpolated)
						                 observations that happened in-between and were filtered out after aggregation.
						   TotalObs   => Not entirely related to idx, this provides the total observations that
						                 occurred for that trip_id at the stop_seque. For instance trip_id X at stop_seque
						                 2 had 4 occurrences. This factors in the calculations for the columns Late till
						                 PrcObsUns. Keep in mind this does not directly measure probability of on-time
						                 performance, but rather the stability in status updates.
						   Late       => The number of observations per route, trip_id, per stop_seque that have been
						                 projected to arrive Late.
						   On-Time    => The number of observations per route, trip_id, per stop_seque that have been
						                 projected to arrive On-Time.
						   Early      => The number of observations per route, trip_id, per stop_seque that have been
						                 projected to arrive Early.
						   Satis      => Short for Satisfactory -> Factoring only the total observations that fall in On-Time field.
						   Unsatis    => Short for Unsatisfactory -> Combining Late and Early total observations.
						   PrcObsSat  => Short for percentage of all observations projected to have an on-time
						                 performance of satisfactory. Signifies stability of status changes.
						                 Higher indicates more stable status change and higher probability to be on-time.
						   PrcObsUns  => Short for percentage of all observations projected to be late or early.
						                 Higher indicates less stable status change and higher probability to be late or early.
						   spdList    => Nest list of all the observations from first to last observation of that
						                 trip_id and stop_seque of projected speed. Often won't match with TotalObs
						                 due to duplicates.
						   arrdifList => Nest list of all the observations from first to last observation of that
						                 trip_id and stop_seque of arrival time difference.
						                 Often won't match with TotalObs due to duplicates.
						   x          => The last observed x (lon) coordinate.
						   y          => The last observed y (lat) coordinate.

			2) agg_rte_hrly => Takes main_agg and aggregates into an hourly basis per route per stop_seque per hour.
							   See self._aggRteHrly for schema info.

			3) agg_rte_daily => Takes agg_rte_hrly, and aggregates into overall daily basis per route and per stop_seque.
								See self._aggRteDaily for schema info.

		:param tmp_df: The individual csv file being read - typically originating from interpolated_cleaned version.
		:param perf_df: Aggregated dataframe per route, trip_id, stop_seque, and on-time performance
						observations (Late, Early, On-Time).
		:param L: List manager to collect main_agg. Used to concat afterwards (when parallel is complete)
				  and export as csv and shapefile.
		:param L2: List manager to collect agg_rte_hrly. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		:param L3: List manager to collect agg_rte_daily. Used to concat afterwards (when parallel is complete) and
				   export as csv and shapefile.
		:param tmp_shp: The shapefile of the route used to merge with the aggregations.

		:return: L, L2, L3 - as list containing all aggregated information.
		"""

		spec_grp = ['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr', 'idx']
		gen_grp  = spec_grp[0:-1]

		# Main aggregation per route, trip_id, stop_seque & stop_id, and sched_arr
		main_agg = (
			tmp_df
				# Removing unnecessary duplicate values to compute overall average speed,
				# arrival difference, and percentage change.
				.drop_duplicates(spec_grp, keep='last')
				.groupby(spec_grp, as_index=False)
				.agg({'proj_speed' : 'mean',
			          'off_arrdif' : 'mean',
			          'perc_chge'  : 'mean'})
				.reset_index()
				.round(2)
				.rename(columns = {'proj_speed' : 'AvgSpd',
			                       'off_arrdif' : 'Avg_ArrDif',
			                       'perc_chge'  : 'Avg_prcChg'})
				# Another round of grouping excluding idx. The idea is to get the absolute
				# average speed, arrival difference, and percentage change per route, trip_id, stop_seque, and stop_id.
				.groupby(gen_grp, as_index=False)
				.agg({'AvgSpd'     : 'mean',
			          'Avg_ArrDif' : 'mean',
			          'Avg_prcChg' : 'mean'})
				.round(2)
				# Merge with perf_df to get Early, Late, and On-Time information
				.merge(perf_df, on=['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr'], how='left')
				# Merge with the original csv file to get additional features
				.merge(tmp_df, on=['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr'], how='left')
				.drop_duplicates(spec_grp, keep='last')
				# Ensure that Early, Late, and On-Time fields exist - some trip_ids / routes may not have one or more.
				.pipe(lambda e: self._checkFields(e))
				# Extensive unique groupby to get a nested list of past projected speed and arrival time difference.
				# This is to show the changes over time in speed and arrival time difference.
				# Can be used to expand and visualize extensively.
				.groupby(['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr', 'AvgSpd', 'Avg_ArrDif',
			              'Late', 'On-Time', 'Early'])
				.apply(lambda x: [list(x['proj_speed']), list(x['off_arrdif'])])
				.apply(Series)
				.reset_index()
				.rename(columns = {0 : 'spdList',
			                       1 : 'arrdifList'})
				# Do another merge to get extensive features - off_earr, x, y
				# (last observations per stop - indicates most recent)
				.merge(tmp_df, on=['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr'], how='left')
				.drop_duplicates(['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr',
			                      'AvgSpd', 'Avg_ArrDif'], keep='last')
				.rename(columns={'perf_rate': 'Lprfrte'})
				# Create new variables
				.assign(ref_hr    = lambda d: d['sched_arr'].pipe(to_datetime).dt.hour,
			            Satis     = lambda d: d['On-Time'],
			            Unsatis   = lambda d: d['Early'] + d['Late'],
			            TotalObs  = lambda d: d['Satis'] + d['Unsatis'],
			            PrcObsSat = lambda d: round((d['Satis'] / d['TotalObs'])*100,2),
			            PrcObsUns = lambda d: round((d['Unsatis'] / d['TotalObs'])*100,2))
				.merge(tmp_shp, on=['stop_id', 'stop_seque'], how='left')
			[['route_id', 'trip_id', 'stop_seque', 'stop_id', 'sched_arr', 'off_earr', 'Lprfrte',
			  'ref_hr', 'AvgSpd', 'Avg_ArrDif', 'idx', 'TotalObs', 'Late', 'On-Time', 'Early',
			  'Satis', 'Unsatis', 'PrcObsSat', 'PrcObsUns', 'spdList', 'arrdifList', 'x', 'y', 'SHAPE']]
		)

		# Semi-aggregated dataframe - export to csv (for anyone to inspect)
		L.append(main_agg)

		# Aggregated per route, stop, and hour - export to csv / merge with shapefile.
		agg_rte_hrly = self._aggRteHrly(main_agg=main_agg, tmp_shp=tmp_shp)
		L2.append(agg_rte_hrly)

		# Aggregate per route and stop to get daily - export to csv / merge with shapefile
		agg_rte_daily = self._aggRteDaily(agg_rte_hr=agg_rte_hrly, tmp_shp=tmp_shp)
		L3.append(agg_rte_daily)


	def _aggRteDaily(self, agg_rte_hr: DataFrame, tmp_shp) -> DataFrame:
		"""
		Aggregate per route, stop_id & stop_seque to get mean values including on-time performance throughout the day.

		:param agg_rte_hr: Aggregated dataframe with average results per route, stop_seque, per hour.
		:param tmp_shp: The shapefile of the route used to merge with the aggregations.

		:return: Aggregated dataframe per route per stop on a daily basis based on the schema below:

			route_id    => The transit route.
			stop_id     => ID number of the transit stop.
			stop_seque  => Order sequence number of the transit stop.
			agglength   => The number of hours per route per stop have been observed throughout the day.
			list_refhr  => A nested list of reference (ref_hr) hours that have been observed throughout the day.
			cntTripIDs  => The number of trip_ids that were observed in near "real-time" throughout the day.
			AllObs 		=> Not the same as cntTripIDs, all observations recorded & interpolated for all trip_ids
						   throughout the day.
			AvgSpd		=> The unweighted average speed (km/h) based on cntTripIDs throughout the day.
			spd_w		=> The weighted average speed based on calculations from AllObs.
			Avg_ArrDif	=> The unweighted average arrival time difference (sec.) based on cntTripIDs.
			arrd_w		=> The weighted average arrival time difference (sec.) based on calculations from AllObs.
			PrcObsSat	=> The unweighted average percent of total observations (recorded + interpolated)
						   projected to be on-time.
			PrcObsUns	=> The unweighted average percent of total observations (recorded + interpolated)
						   projected to be early or late.
			prcwSat		=> The weighted average percent of the total observations (recorded + interpolated)
						   projected to be on-time.
			prcwUns		=> The weighted average percent of total observations (recorded + interpolated)
						   projected to be early or late.
			ActSatP		=> The actual percentage (based on last projected observation per trip_id) of being on-time.
			ActUnsP		=> The actual percentage (based on last projected observation per trip_id) of being early or late.
		"""

		grp_cols = ['route_id', 'stop_id', 'stop_seque']
		new_grp  = ['route_id', 'stop_id', 'stop_seque', 'agglength']

		agg_rte_dly = (
			agg_rte_hr
				.pipe(lambda e: e
			          .groupby(grp_cols)
			          .apply(lambda x: [list(x['ref_hr']), len(x)])
			          .apply(Series)
			          .reset_index()
			          .rename(columns={0: 'list_refhr',
			                           1: 'agglength'})
			          .merge(e, on=['route_id', 'stop_id', 'stop_seque'], how='left')
			          .pipe(lambda f: f
			                .groupby(new_grp, as_index=False)
			                .agg({'cntTripIDs': 'nunique',
			                      'AllObs': 'sum',
			                      'AvgSpd': 'mean',
			                      'spd_w': 'mean',
			                      'Avg_ArrDif': 'mean',
			                      'arrd_w': 'mean',
			                      'PrcObsSat': 'mean',
			                      'PrcObsUns': 'mean',
			                      'prcwSat': 'mean',
			                      'prcwUns': 'mean',
			                      'ActSatP': 'mean',
			                      'ActUnsP': 'mean'})
			                .merge(f, on=['route_id', 'stop_id', 'stop_seque', 'agglength'], how='left')))
				.drop_duplicates(['route_id', 'stop_id', 'stop_seque', 'agglength'], keep='last')
				.rename(columns={'cntTripIDs_x': 'cntTripIDs',
			                     'AllObs_x': 'AllObs',
			                     'AvgSpd_x': 'AvgSpd',
			                     'spd_w_x': 'spd_w',
			                     'Avg_ArrDif_x': 'Avg_ArrDif',
			                     'arrd_w_x': 'arrd_w',
			                     'PrcObsSat_x': 'PrcObsSat',
			                     'PrcObsUns_x': 'PrcObsUns',
			                     'prcwSat_x': 'prcwSat',
			                     'prcwUns_x': 'prcwUns',
			                     'ActSatP_x': 'ActSatP',
			                     'ActUnsP_x': 'ActUnsP'})
				[['route_id', 'stop_id', 'stop_seque', 'agglength', 'list_refhr',
			      'cntTripIDs', 'AllObs', 'AvgSpd', 'spd_w', 'Avg_ArrDif', 'arrd_w',
			      'PrcObsSat', 'PrcObsUns', 'prcwSat', 'prcwUns', 'ActSatP', 'ActUnsP']]
				.sort_values(['route_id', 'stop_seque'])
				.merge(tmp_shp, on=['stop_id', 'stop_seque'], how='left')
		)

		return agg_rte_dly


	def _aggRteHrly(self, main_agg: DataFrame, tmp_shp) -> DataFrame:
		"""
		Aggregate per route, stop_id & stop_seque, and hour to get mean values including on-time performance.

		:param main_agg: DataFrame
		:param tmp_shp: The shapefile of the route used to merge with the aggregations.

		:return: Aggregated dataframe per route per stop per hour based on the schema below:

			route_id    => The transit route.
			stop_id     => ID number of the transit stop.
			stop_seque  => Order sequence number of the transit stop.
			ref_hr      => Reference hour of the expected scheduled at the stop (e.g., 6:00 -> 6)
			cntTripIDs  => The number of trip_ids that were observed in near "real-time".
			AllObs 		=> Not the same as cntTripIDs, all observations recorded & interpolated for all trip_ids.
			AvgSpd		=> The unweighted average speed (km/h) based on cntTripIDs.
			spd_w		=> The weighted average speed based on calculations from AllObs.
			Avg_ArrDif	=> The unweighted average arrival time difference (sec.) based on cntTripIDs.
			arrd_w		=> The weighted average arrival time difference (sec.) based on calculations from AllObs.
			PrcObsSat	=> The unweighted average percent of total observations (recorded + interpolated)
						   projected to be on-time.
			PrcObsUns	=> The unweighted average percent of total observations (recorded + interpolated)
						   projected to be early or late.
			prcwSat		=> The weighted average percent of the total observations (recorded + interpolated)
						   projected to be on-time.
			prcwUns		=> The weighted average percent of total observations (recorded + interpolated)
						   projected to be early or late.
			ActSatP		=> The actual percentage (based on last projected observation per trip_id) of being on-time.
			ActUnsP		=> The actual percentage (based on last projected observation per trip_id) of being early or late.
		"""

		grp_spec = ['route_id', 'stop_id', 'stop_seque', 'ref_hr']

		agg_rte_hrly = (
			main_agg
				.groupby(grp_spec, as_index=False)
				.apply(lambda e: e.assign(AllObs  = lambda d: d['TotalObs'].sum(),
										  wght    = lambda d: d['TotalObs'] / d['AllObs'],
										  spd_w   = lambda d: d['AvgSpd'] * d['wght'],
										  arrd_w  = lambda d: d['Avg_ArrDif'] * d['wght'],
										  prcwSat = lambda d: d['PrcObsSat'] * d['wght'],
										  prcwUns = lambda d: d['PrcObsUns'] * d['wght']))
		)

		new_grp = ['route_id', 'stop_id', 'stop_seque', 'ref_hr', 'AllObs']

		# Get absolute weighted mean
		wght_df = (
			agg_rte_hrly
				.groupby(new_grp, as_index=False)
				.agg({'trip_id' : 'count',
					  'AvgSpd'  : 'mean',
					  'spd_w'   : 'mean',
					  'Avg_ArrDif' : 'mean',
					  'arrd_w'     : 'mean',
					  'PrcObsSat'  : 'mean',
					  'PrcObsUns'  : 'mean',
			          'prcwSat'    : 'mean',
			          'prcwUns'    : 'mean'})
		)

		# Get unweighted mean of on-time performance (final observed - last projected status)
		raw_df = (
			agg_rte_hrly
				.pipe(lambda e: crosstab([e['route_id'], e['trip_id'], e['stop_id'], e['stop_seque'],
										  e['sched_arr'], e['ref_hr']], e['Lprfrte']))
				.pipe(lambda e: self._checkFields(e))
				.reset_index()
				.groupby(grp_spec, as_index=False)
				.apply(lambda e: e.assign(ActSat  = lambda d: d['On-Time'],
										  ActUns  = lambda d: d['Late'] + d['Early'],
										  finObs  = lambda d: d['ActSat'] + d['ActUns'],
										  ActSatP = lambda d: round((d['ActSat'] / d['finObs'])*100,2),
										  ActUnsP = lambda d: round((d['ActUns'] / d['finObs'])*100,2)))
				.groupby(grp_spec, as_index=False)
				.agg({'ActSatP' : 'mean',
					  'ActUnsP' : 'mean'})
		)

		# Merge the two aggregated dataframes to finalize
		merge_fin = (
			wght_df
				.merge(raw_df, on=['route_id', 'stop_id', 'stop_seque', 'ref_hr'], how='left', validate='1:1')
				.rename(columns={'trip_id' : 'cntTripIDs'})
				[['route_id', 'stop_id', 'stop_seque', 'ref_hr', 'cntTripIDs', 'AllObs',
			      'AvgSpd', 'spd_w', 'Avg_ArrDif', 'arrd_w', 'PrcObsSat', 'PrcObsUns',
			      'prcwSat', 'prcwUns', 'ActSatP', 'ActUnsP']]
				.sort_values(['route_id', 'stop_seque', 'ref_hr'])
				.merge(tmp_shp, on=['stop_id', 'stop_seque'], how='left')
		)

		return merge_fin


	def _checkFields(self, df):
		"""
		If one of the on-time performance fields do not exist, then assign it with a value of 0 (indicating none exist).

		:param df: The dataframe to be updated during aggregation process.
		:return: Updated dataframe.
		"""

		for field in ['Early', 'Late', 'On-Time']:
			if not field in df.columns:
				df[field] = 0

		return df
