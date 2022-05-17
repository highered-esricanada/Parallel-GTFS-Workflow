"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at the Education & Research at Esri Canada.
Date: Re-modified Q1 - 2022

About:
"""

from pandas import DataFrame
from ..util import TimeDelta, SpatialDelta


class RteEnricher:

	def __init__(self, clean_df: DataFrame, undiss_df, stp_df: DataFrame, stop_times: DataFrame, folder_date, output_folder, raw_date, unique_val, L3):
		"""
		Enriches additional features for each trip per trip_id.

		:param clean_df: Dataframe of the cleaner version (from QA/QC) of the GTFS-RT per transit route.
		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
		:param stp_df: The stop csv file as a dataframe of the transit route.
		:param stop_times: The schedule (from GTFS static) per stop_id per trip_id (vehicle's id associated to transit rte).
		:param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-11-17).
		:param output_folder: Contents exported & stored in the output folder.
		:param raw_date: The date of the raw GTFS-RT.
		:param unique_val: The unique-rte currently inspecting.
		:param L3: The list that is part of the Manager in Multiprocessing.

		:return: An enriched dataframe filled with stop information, index information, point info., time info., and spatial info.
		"""

		self.enrich_df = self._main(df=clean_df,
		                            undiss_df=undiss_df,
		                            stp_df=stp_df,
		                            stop_times=stop_times,
		                            folder_date=folder_date,
		                            output_folder=output_folder,
		                            raw_date=raw_date,
		                            unique_val=unique_val,
		                            L3=L3)


	def _get_max_seq_idx(self, stp_df: DataFrame, undiss_df):
		"""
		Validate if the max stop sequence exists in undissolved - performs another layer of QA/QC with the static GTFS data.
		Sometimes the static GTFS files are not entirely accurate - for instance, the terminus is supposed to be the
		63rd stop of the route, but it may only have 62. This function will determine terminus status based on the data provided.

		Acquires max stop sequence number, max index value of the undissolved segment, and statement validation match (true or false).

		:param stp_df: The stop csv file as a dataframe of the transit route.
		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.

		:return: A tuple - max stop sequence number, max index value of the undissolved segment, and statement validation match.
		"""

		try:
			# Get true max stop sequence & index
			max_stp_seq  = max(stp_df['stop_seque'])
			max_idx_seg  = max(undiss_df.query('stop_seque == @max_stp_seq')['index'])
			true_max_stp = True

			return (max_stp_seq, max_idx_seg, true_max_stp)

		except Exception as e:
			# If not true, then find the next highest using the undissolved shapefile of the transit route
			max_stp_seq  = max(undiss_df['stop_seque'])
			max_idx_seg  = max(undiss_df.query('stop_seque == @max_stp_seq')['index'])
			true_max_stp = False

			return (max_stp_seq, max_idx_seg, true_max_stp)


	def _last_clean(self, df: DataFrame):
		"""
		Removing incorrect observations - unordered trends - final sweep.

		:param df: DataFrame by grouped trip_id.
		:return: Cleaner DataFrame.
		"""

		return (
			df
				.groupby(['trip_id', 'stop_seque'], as_index=False)
				.apply(lambda e: e.assign(Idx_Diff = lambda d: d['index'].diff(1)))
				.sort_values(['trip_id', 'barcode', 'Local_Time'])
				.query('Idx_Diff >= 0 or Idx_Diff.isnull()', engine='python')
		)


	def _set_mvmt(self, idx_diff, stp_diff, stp_seq, max_stp_seq):
		"""
		Identify the movement status of the vehicle by comparing to the next observation (consecutive).

		:param idx_diff: The value from the index difference (Idx_Diff) column.
		:param stp_diff: The value from the stop sequence difference (Stp_Diff) column.
		:param stp_seq: The value from the stop sequence (stop_seque) column.
		:param max_stp_seg: The max value from the max. stop sequence (MaxStpSeq) column.

		:return: Pre-determined (require distance delta to confirm) status of the vehicle's movement.
		"""

		# If index difference or stop sequence difference is zero, and current stop sequence is equal to max stop sequence
		# Set to Terminus
		if (idx_diff == 0 or stp_diff == 0) and (int(stp_seq) == max_stp_seq):
			return "Terminus"

		else:
			# If current stop sequence is equal to max stop sequence, and the index and stop sequence difference is greater than 0.
			# Set to Terminus
			if (int(stp_seq) == max_stp_seq) and (idx_diff > 0 or stp_diff > 0):
				return "Terminus"

			# Set to terminus if the current stop sequence is equal to the max stop sequence.
			elif int(stp_seq) == max_stp_seq:
				return "Terminus"

			# Otherwise set to stationary (idling/very slow movement) if the vehicle has zero index and stop sequence difference.
			elif idx_diff == 0 and stp_diff == 0:
				return "Stationary"

			# Otherwise set to movement if the vehicle is en-transit.
			else:
				return "Movement"


	def _eval_pnts(self, pnt, pnt_2):
		"""
		Preparation to estimate the distance travelled between two consecutive snapped points.
		Applies only to the consecutive pair that have been flagged as stationary-stationary,
		stationary-movement, and stationary-terminus.
		It validates whether the vehicle has truly been idled en-transit.

		:param pnt: Snapped point (str) of the vehicle's location.
		:param pnt_2: The second snapped point (consecutive - str) of the vehicle's location.

		:return: A tuple of extracted points (x, y) and wkid.
		"""

		eval_1 = eval(pnt)
		eval_2 = eval(pnt_2)
		wkid   = eval_1['spatialReference']['wkid']

		x1 = eval_1['x']
		y1 = eval_1['y']

		x2 = eval_2['x']
		y2 = eval_2['y']

		return (y1, x1, y2, x2, wkid)


	def _get_dist(self, stat, stat_2, pnt, pnt_2):
		"""
		Uses the self._eval_pnts function to extract snapped consecutive vehicle locations.
		Estimates the distance between the two snapped consecutive vehicle locations via the SpatialDelta class.
		Applies only to the consecutive movements for validation: stationary-stationary, stationary-movement, stationary-terminus.

		:param stat: Vehicle's location status.
		:param stat_2: The next vehicle's location status.
		:param pnt: Snapped point (str) vehicle's location.
		:param pnt_2: Snapped point (str) of the next vehicle's location status.

		:return: Distance in meters or None if not applicable.
		"""

		try:
			if (stat == 'Stationary' and stat_2 == 'Stationary') or \
					(stat == 'Stationary' and stat_2 == 'Terminus') or \
					(stat == 'Stationary' and stat_2 == 'Movement'):
				info  = self._eval_pnts(pnt, pnt_2)
				paths = [[[info[1], info[0]], [info[3], info[2]]]]

				dist  = SpatialDelta(paths=paths, wkid=info[4]).dist

				# If the distance is less than or equal 20 meters, then it is considered idle/ very slow transit.
				if dist <= 20:
					return dist

				else:
					return None

			else:
				return None

		except Exception as e:
			return None


	def _main(self, df: DataFrame, undiss_df, stp_df: DataFrame, stop_times: DataFrame, folder_date, output_folder, raw_date, unique_val, L3):
		"""
		The main function that enriches the vehicle's travel/recording.

		:param df: Dataframe of the cleaner version (from QA/QC) of the GTFS-RT per transit route.
		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
		:param stp_df: The stop csv file as a dataframe of the transit route.
		:param stop_times: The schedule per stop_id (transit stop) per trip_id (vehicle's id associated to transit rte).
		:param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-11-17).
		:param output_folder: Contents exported & stored in the output folder.
		:param raw_date: The date of the raw GTFS-RT.
		:param unique_val: The unique-rte currently inspecting.
		:param L3: The list that is part of the Manager in Multiprocessing.

		:return: DataFrame of the GTFS-RT per transit route with enriched data.
		"""

		get_max_info = self._get_max_seq_idx(stp_df=stp_df, undiss_df=undiss_df)

		suppl_df = (
			df
				.merge(stop_times, on=['trip_id', 'stop_id'], how='left', validate='m:m') # Merge with the scheduled GTFS file.
				.drop_duplicates(['Local_Time']) # Reduce unnecessary observations if duplicates arise.
				.drop(columns=['pickup_type', 'drop_off_type', 'shape_dist_traveled', 'timepoint']) # Remove unnecessary fields.
				.assign(MaxIndex     = get_max_info[1], # Get max index value of the transit route's undissolved segment.
			            MaxStpSeq    = get_max_info[0], # Get max stop sequence of the transit route.
			            true_max_stp = get_max_info[2]) # Indicate if the max stop is true - whether undissolved's stop sequence match with the stop sequence from the stop csv file - a warning of GTFS quality & determine terminus.
				.pipe(lambda d: self._last_clean(df=d)) # Another sweep of QA/QC
				.pipe(lambda d: self._last_clean(df=d)) # Another sweep of QA/QC
				.pipe(lambda d: self._last_clean(df=d)) # Final sweep of QA/QC
				.drop(columns = ['Idx_Diff'])
				.groupby(['trip_id'], as_index=False)
				.apply(lambda e: e.assign(Idx_Left   = lambda d: d['MaxIndex'] - d['index'], # Find how many indices the vehicle of the trip_id has left from its current record.
			                              Stp_Left   = lambda d: d['MaxStpSeq'] - d['stop_seque'], # Find how many stops the vehicle of the trip_id has left from its current record.
			                              Idx_Diff   = lambda d: d['Idx_Left'].diff(1), # The difference between index left values - potentially identifies stationary values - compares next set.
			                              Stp_Diff   = lambda d: d['Stp_Left'].diff(1), # The difference between stop left values - potentially identifies stationary values - compares next set.
			                              Status     = lambda d: d[['Idx_Diff', 'Stp_Diff', 'stop_seque', 'MaxStpSeq']].apply(lambda r: self._set_mvmt(*r), axis=1), # Pre-determine movement status of the vehicle (will require distance as well).
			                              val        = 1, # Set value
			                              idx        = lambda d: d['val'].cumsum(), # Cumulate the number of vehicle movements (aka - recordings; not original after QA/QC) per trip_id
			                              stat_shift = lambda d: d['Status'].shift(-1), # Shift the Status column up by 1 - consecutive pair comparison of movement status.
                                          pnt_shift  = lambda d: d['point'].shift(-1),  # Shift the point column up by 1 - consecutive pair comparison of distance via haversine in future
                                          time_shift = lambda d: d['Local_Time'].shift(-1), # Shift the Local_Time column up by 1 - consecutive recorded time pair comparison via timedelta.
                                          delta_time = lambda d: d[['Local_Time', 'time_shift']].apply(lambda r: TimeDelta(*r).change_time, axis=1), # Get the time delta between consecutive pair.
                                          delta_dist = lambda d: d[['Status', 'stat_shift', 'point', 'pnt_shift']].apply(lambda r: self._get_dist(*r), axis=1))) # Get the delta distance between consecutive pair - applies only stationary
				.drop(columns=['val'])
				[['trip_id', 'idx', 'barcode', 'Status', 'stat_shift',                          # trip_id - to movement status
				  'stop_id', 'stop_seque', 'MaxStpSeq', 'true_max_stp', 'Stp_Left', 'Stp_Diff', # stop information
				  'objectid', 'index', 'MaxIndex', 'Idx_Left', 'Idx_Diff',                      # index information of the undissolved segment
				  'x', 'y', 'wkid', 'point', 'pnt_shift',                                       # Point information
				  'Local_Time', 'time_shift', 'delta_time', 'arrival_time', 'departure_time',   # time information
				  'delta_dist', 'SHAPE'                                                         # spatial information (delta_dist = dist. covered; SHAPE = polyline of undissolved seg.)
				  ]]
		)

		ori_length = df.shape[0]
		fin_length = suppl_df.shape[0]

		retained = round((fin_length / ori_length)*100, 2)

		file_name = f"{output_folder}/{raw_date}_{unique_val}_processed.csv"
		suppl_df.to_csv(file_name, index=False)

		L3.append(f"{unique_val},{raw_date},{folder_date},{retained}")

		return suppl_df