"""
Author: Anastassios Dardas - PhD, Higher Education Specialist at Education & Research at Esri Canada.
Date: Re-modified Q1 - 2022

About: From per transit route and grouped trip_id, this performs spatio-temporal interpolation
	   operations to pre-determine on-time performance per stop per trip_id per transit route.

Warning Note: This operation runs in parallel from downstream operation.
			  Although not recommended, it can be used outside parallel
			  environment to perform spatio-temporal interpolation
			  operations at a time.

			  Big O Notation is 2nd highest after geoapi.py due to
			  large codebase with nested apply functions and
			  performing spatial operations in-memory.
"""

from pandas import DataFrame, concat
from ..util import BtwnStps, OneStp, SameStp
from ..util import CalcEnhanceDf, CalcSemiDf


class SpaceTimeInterp:

	def __init__(self, enrich_df: DataFrame, undiss_df: DataFrame, stop_times: DataFrame, wkid, folder_date, output_folder, raw_date, unique_val, L4):
		"""
		Perform spatio-temporal interpolation to estimate on-time performance.

		:param enrich_df: The enriched dataframe based on the schema:
						trip_id        -> The ID related to the transit route with its own schedule.
						idx            -> An updated index to track transit recordings per trip_id.
						barcode        -> Original index to track transit recordings per trip_id. If not aligned with idx,
									      then some recordings have been omitted.
						Status         -> Travel status type identified based on delta distance (e.g., movement, stationary, terminus).
						stat_shift     -> Shift Status field by -1 to compare consecutive pair (e.g., idx #1 -> idx #2).
						stop_id        -> The stop id number (i.e., transit stop) of the transit route that the veh. is en-transit.
						stop_seque     -> The stop sequence number (tied to stop_id) along the transit route.
						MaxStpSeq      -> The maximum stop sequence number of the transit route.
						true_max_stp   -> Whether the maximum stop sequence is true (i.e., reliable) or not from the GTFS.
						Stp_Left       -> # of stops left (MaxStpSeq - stop_seque) to travel before completing route.
						Stp_Diff       -> Identifies how many stops have travelled between the consecutive pair.
										  (0 = no stops have passed; -1 = passed 1 stop; -2 = passed 2 stops; -nth)
						objectid       -> ObjectID number of the undissolved segment.
						index          -> The index value of the undissolved segment in which the veh. is on.
						MaxIndex       -> The maximum index value of the undissolved segment dataframe.
						Idx_Left       -> # of indices left (MaxIndex - index) (i.e., # of undissolved segments) to travel
										  before completing the route.
						Idx_Diff       -> Identifies how many indices have travelled between the consecutive pair.
										  (0 = no index difference; -4 = 4 indices / undissolved segments have been travelled;
										  -8 = 8 indices / undissolved segments have been travelled; nth).
						x              -> The snapped x-coordinate (longitude) of the vehicle recorded.
						y              -> The snapped y-coordinate (latitude) of the vehicle recorded.
						wkid           -> Spatial reference #.
						point          -> Shape Point Geometry (x, y).
						pnt_shift      -> Shift point field by -1 to compare consecutive pair.
						Local_Time     -> The recorded time of the vehicle from GTFS-RT.
						time_shift     -> Shift the Local_Time field by -1 to compare consecutive pair.
						delta_time     -> The recorded time gap (sec.) between consecutive pair (time_shift - Local_Time).
						arrival_time   -> Provided by the GTFS static file, the scheduled arrival time for the stop_id.
						departure_time -> Provided by the GTFS static file, the schedule departure time for the stop_id.
						delta_dist     -> Rough estimate of the distance travelled between the two consecutive points.
										 (point --> distance gap --> pnt_shift).
						SHAPE          -> The SHAPE of the undissolved segment queried from the index field.

		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
		:param stop_times: The schedule (from GTFS static) per stop_id per trip_id (vehicle's id associated to transit rte.).
		:param wkid: Spatial reference to project polylines.
		:param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-09-30).
		:param output_folder: Contents exported & stored in the output folder.
		:param raw_date: The date of the collected raw GTFS-RT.
		:param unique_val: The unique-rte currently inspecting.
		:param L4: The list that is part of the Manager in Multiprocessing - report error rate & error type (if applicable).

		:returns: Enhanced interpolated dataframe (see returned value in _execEnhanced for schema details).
		"""

		self.enhanced_df = self._complexEng(enrich_df=enrich_df,
		                                    undiss_df=undiss_df,
		                                    stop_times=stop_times,
		                                    wkid=wkid,
		                                    folder_date=folder_date,
		                                    output_folder=output_folder,
		                                    raw_date=raw_date,
		                                    unique_val=unique_val,
		                                    L4=L4)


	def _projspeed(self, tot_dist, delta_time):
		"""
		Calculates projected travel speed (distance / time).

		:param tot_dist:  The total distance travelled (meters).
		:param delta_time: Change in time (seconds).

		:return: Speed in km/hr.
		"""

		proj_speed = round((tot_dist / delta_time)*3.6, 1)
		return proj_speed


	def _execEnhanced(self, semi_df, trip_id, proj_speed, status, stat_shift, conx,
	                  local_time, time_shift, future_dist,
	                  stop_times, idx, x1, y1, x2, y2, travel_type):
		"""
		Data augmentation class - stitch all missing travel information together and
								  add additional features (see return statement below for schema details).

		:param semi_df: The semi-final dataframe (stop_seque, end_path, dist, Tot_Dist) used to be augmented.
		:param trip_id: The trip_id of the transit route being processed.
		:param proj_speed: Calculated projected travel speed (km/hr.) from time and distance delta between the
						   consecutive pair.
		:param status: Current travel status of the 1st veh.
		:param stat_shift: Current travel status of the 2nd veh.
		:param conx: The connection type - that typically happens in-between.
		:param local_time: The recorded time of the 1st veh. of the consecutive pair.
		:param time_shift: The recorded time of the 2nd veh. of the consecutive pair.
		:param future_dist: The distance that will need to be travelled in the near-future for the 2nd veh.
		:param stop_times: Static GTFS file with scheduled/expected arrival_time and departure time for each stop per trip_id.
		:param idx: The index - determining the number of vehicle movement in the consecutive group.
		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 1st veh.
		:param x2: Snapped x-coordinate (longitude) of the 2nd veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param travel_type: The type of movement travel (e.g., movement, one stop, same stop - same segment,
														 same stop - different segment).

		:return: Enhanced final dataframe with the following schema:
					trip_id    = Identifier of the transit route.
					idx        = The cumulative number of vehicle movements - grouped per consecutive pair.
					stop_id    = Identifier of the transit stop.
					stop_seque = The sequence number (order) associated to the stop_id.
					status     = Travel status of the vehicle.
					proj_speed = Projected travel speed (km/h.) from time and distance delta between consecutive pair.
					x          = The snapped x-coordinate of where the vehicle was located.
					y          = The snapped y-coordinate of where the vehicle was located.
					Tot_Dist   = The total distance (m) travelled from the 1st to the 2nd veh. consecutive pair.
					dist       = Distance traveled on each stop sequence segment - the last observation in the group calculates of what has past (reverse).
					dist_futr  = The distance required for the last observation in the group needed to arrive from its current stop_sequence path (forward).
					futr_trvel = The amount of travel time (sec.) projected to complete the future distance - last observation in the group.
					proj_trvel = The amount of travel time (sec.) projected to complete from 1st veh to 2nd veh (last observation) in the group - from dist and proj_speed.
					curr_time  = The recorded timestamp from the 1st veh. and 2nd veh. (last observation) in the group.
					est_arr    = The estimated arrival time based on proj_trvel and curr_time - cumulative, except the last observation.
					off_earr   = Official estimated arrival time for all observations including the last observation (future).
					tmp_arr    = The scheduled/expected arrival time reformatted - excludes the last observation in the group.
					sched_arr  = Official scheduled/expected arrival time reformatted - includes the last observation in the group.
					arr_tmedif = Arrival time difference calculated from estimated arrival time and scheduled/expected arrival time - excludes last observation.
					off_arrdif = Official time difference calculated from estimated arrival time and scheduled/expected arrival time - includes last observation (forward).
					perc_chge  = Percent change in official time difference - estimates how much of a change there has been in travel over time.
					perf_rate  = Classification of on-time performance: Late (<= -120 sec.); On-Time (120 < x < 300); Early (>= 300).
					dept_time  = The scheduled/expected departure time (not reformatted).
					end_path   = The linestring paths (nested coordinates) that can be drawn out spatially if required.
		"""

		final_df = CalcEnhanceDf(semi_final_df=semi_df,
		                         trip_id=trip_id,
		                         proj_speed=proj_speed,
		                         status=status,
		                         stat_shift=stat_shift,
		                         mid_stat=conx,
		                         local_time=local_time,
		                         time_shift=time_shift,
		                         future_dist=future_dist,
		                         stop_times=stop_times,
		                         idx=idx,
		                         x1=x1, y1=y1,
		                         x2=x2, y2=y2,
		                         travel_type=travel_type).final_df

		return final_df


	def _augmentTravel(self, trip_id, x1, y1, stp_seq, index, status,
                       x2, y2, stp_seq2, index2, stat_shift, stp_diff_shift,
                       delta_time, local_time, time_shift, idx,
                       undiss_df: DataFrame, stop_times: DataFrame, wkid):
		"""
		The "brains" of the entire operation that inspects the consecutive pair of the grouped trip_id and determines
		the type of travel that happened, initiates data augmentation per type of stop (i.e., mobility/travel).

		The individual values from each defined feature in the dataframe:
		:param trip_id: Identifier of the transit route.
		:param x1: Snapped x-coordinate (longitude) of the 1st recorded of the pair.
		:param y1: Snapped y-coordinate (latitude) of the 1st recorded of the pair.
		:param stp_seq: The stop sequence number that is associated to the stop_id of the 1st recorded of the pair.
		:param index: The value of the undissolved segment of the 1st recorded of the pair.
		:param status: The movement status of the 1st recorded of the pair.
		:param x2: Snapped x-coordinate (longitude) of the 2nd recorded of the pair.
		:param y2: Snapped y-coordinate (latitude) of the 2nd recorded of the pair.
		:param stp_seq2: The stop sequence number that is associated to the stop_id of the 2nd recorded of the pair.
		:param index2: The value of the undissolved segment of the 2nd recorded of the pair.
		:param stat_shift: The movement status of the 2nd recorded of the pair.
		:param stp_diff_shift: The stop sequence difference between the 1st and 2nd recorded of the pair.
		:param delta_time: The time difference between the 1st and 2nd recorded of the pair.
		:param local_time: The timestamp of the 1st recorded of the pair.
		:param time_shift: The timestamp of the 2nd recorded of the pair.
		:param idx: The cumulative number of vehicle movement per trip_id.

		Supplement data used during spatio-temporal interpolation process:
		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
		:param stop_times: The schedule (from GTFS static) per stop_id per trip_id.
		:param wkid: Spatial reference used to project ArcGIS Polyline geometries.

		Dependent Classes:
			1) BtwnStps   --> The build of what happened between the 1st and 2nd veh. of consec. pair - more than 1 transit
						      stop has been passed and not recorded.
			2) OneStp     --> The build of what happened between the 1st and 2nd veh. of consec. pair - from a one-stop diff.
			3) SameStp    --> The build of what happened between the 1st and 2nd veh. of consec. pair - from the same stop.
			4) CalcSemiDf --> Creates a semi-final dataframe for data augmentation
							  (schema: stop_seque, end_path, dist, Tot_Dist).

		Dependent Function(s): _projspeed, _execEnhanced

		:return: Final dataframe to be extracted and refined. Refer to the return section in the
				_execEnhanced for schema details.
		"""

		try:


			if status == "Movement" or (status == "Terminus" and stat_shift == "Terminus"): # Deals with multiple terminus status
				##########################################################################################################
				############## Multiple transit stops that happened in-between during consecutive time period ############
				##########################################################################################################
				if (stp_seq != stp_seq2) and (stp_diff_shift < -1):
					spatial_info = BtwnStps(stp_seq=stp_seq,
					                        stp_seq2=stp_seq2,
					                        x1=x1, y1=y1,
					                        x2=x2, y2=y2,
					                        index=index,
					                        index2=index2,
					                        undiss_df=undiss_df,
					                        wkid=wkid).btwn_info

					conx        = spatial_info[0]   # Connection type
					tot_dist    = spatial_info[1]   # Total distance traveled
					semi_df     = spatial_info[2]   # Semi-final dataframe
					future_dist = spatial_info[3]   # Future distance from 2nd veh.
					travel_type = "Multiple Stops"  # Travel type
					proj_speed  = self._projspeed(tot_dist=tot_dist,
					                              delta_time=delta_time)

					final_df = self._execEnhanced(semi_df=semi_df,
					                              trip_id=trip_id,
						                          proj_speed=proj_speed,
						                          status=status,
						                          stat_shift=stat_shift,
						                          conx=conx,
						                          local_time=local_time,
						                          time_shift=time_shift,
						                          future_dist=future_dist,
						                          stop_times=stop_times,
						                          idx=idx,
						                          x1=x1, y1=y1,
						                          x2=x2, y2=y2,
						                          travel_type=travel_type)

					return final_df

				##########################################################################################################
				######################### One Stop Difference during consecutive time period #############################
				##########################################################################################################
				elif (stp_seq != stp_seq2) and (stp_diff_shift == -1):
					spatial_info = OneStp(stp_seq=stp_seq,
					                      stp_seq2=stp_seq2,
					                      x1=x1, y1=y1,
					                      x2=x2, y2=y2,
					                      index=index,
					                      index2=index2,
					                      undiss_df=undiss_df,
					                      wkid=wkid).one_info

					conx        = spatial_info[0]
					tot_dist    = spatial_info[1]
					semi_df     = spatial_info[2]
					future_dist = spatial_info[3]
					travel_type = "One Stop"
					proj_speed  = self._projspeed(tot_dist=tot_dist,
					                              delta_time=delta_time)

					final_df = self._execEnhanced(semi_df=semi_df,
						                          trip_id=trip_id,
						                          proj_speed=proj_speed,
						                          status=status,
						                          stat_shift=stat_shift,
						                          conx=conx,
						                          local_time=local_time,
						                          time_shift=time_shift,
						                          future_dist=future_dist,
						                          stop_times=stop_times,
						                          idx=idx,
						                          x1=x1, y1=y1,
						                          x2=x2, y2=y2,
						                          travel_type=travel_type)

					return final_df

				##########################################################################################################
				########################## Same Stop - no diff. during consecutive time period ###########################
				##################################### Applies in terminus as well ########################################
				##########################################################################################################
				elif (stp_seq == stp_seq2) and (stp_diff_shift == 0):
					#################################################
					############ Same stop - same segment ###########
					#################################################
					stp_type = ["Terminus" if status == "Terminus" else "Same Stop"][0]

					if index == index2:
						conx_type    = f"{stp_type} - Same Segment"
						spatial_info = SameStp(stp_seq=stp_seq,
						                       stp_seq2=stp_seq2,
						                       x1=x1, y1=y1,
						                       x2=x2, y2=y2,
						                       index=index,
						                       index2=index2,
						                       undiss_df=undiss_df,
						                       wkid=wkid,
						                       conx_type=conx_type).same_info

						conx        = spatial_info[0]
						tot_dist    = spatial_info[1]
						semi_df     = spatial_info[2]
						future_dist = spatial_info[3] # Future distance from 2nd veh.
						first_dist  = spatial_info[4] # Future distance from 1st veh.
						proj_speed  = self._projspeed(tot_dist=tot_dist,
						                              delta_time=delta_time)
						future_dist = [first_dist, future_dist]

						final_df = self._execEnhanced(semi_df=semi_df,
							                          trip_id=trip_id,
							                          proj_speed=proj_speed,
							                          status=status,
							                          stat_shift=stat_shift,
							                          conx=conx,
							                          local_time=local_time,
							                          time_shift=time_shift,
							                          future_dist=future_dist,
							                          stop_times=stop_times,
							                          idx=idx,
							                          x1=x1, y1=y1,
							                          x2=x2, y2=y2,
							                          travel_type=conx_type)

						return final_df

					#################################################
					############ Same stop - Diff segment ###########
					#################################################
					elif index != index2:
						conx_type    = f"{stp_type} - Different Segment"
						spatial_info = SameStp(stp_seq=stp_seq,
						                       stp_seq2=stp_seq2,
						                       x1=x1, y1=y1,
						                       x2=x2, y2=y2,
						                       index=index,
						                       index2=index2,
						                       undiss_df=undiss_df,
						                       wkid=wkid,
						                       conx_type=conx_type).same_info

						conx        = spatial_info[0]
						tot_dist    = spatial_info[1]
						semi_df     = spatial_info[2]
						future_dist = spatial_info[3]
						first_dist  = spatial_info[4]
						proj_speed  = self._projspeed(tot_dist=tot_dist,
						                              delta_time=delta_time)
						future_dist = [first_dist, future_dist]

						final_df = self._execEnhanced(semi_df=semi_df,
							                          trip_id=trip_id,
							                          proj_speed=proj_speed,
							                          status=status,
							                          stat_shift=stat_shift,
							                          conx=conx,
							                          local_time=local_time,
							                          time_shift=time_shift,
							                          future_dist=future_dist,
							                          stop_times=stop_times,
							                          idx=idx,
							                          x1=x1, y1=y1,
							                          x2=x2, y2=y2,
							                          travel_type=conx_type)

						return final_df


			elif status == "Stationary":

				semi_df = CalcSemiDf(consec_stpseq=[stp_seq],
				                     consec_pths=[[[x1, y1]]],
				                     consec_dist=0,
				                     btwn_df=None).semi_df

				proj_speed  = 0
				stat_shift  = None
				conx        = None
				time_shift  = None
				future_dist = None
				travel_type = "Stationary"

				final_df = self._execEnhanced(semi_df=semi_df,
					                          trip_id=trip_id,
					                          proj_speed=proj_speed,
					                          status=status,
					                          stat_shift=stat_shift,
					                          conx=conx,
					                          local_time=local_time,
					                          time_shift=time_shift,
					                          future_dist=future_dist,
					                          stop_times=stop_times,
					                          idx=idx,
					                          x1=x1, y1=y1,
					                          x2=x2, y2=y2,
					                          travel_type=travel_type)

				return final_df


			elif status == "Terminus" and stat_shift == None:
				return None


		except Exception as e:
			return "Not Applicable"


	def _complexEng(self, enrich_df: DataFrame, undiss_df: DataFrame, stop_times: DataFrame, wkid, folder_date, output_folder, raw_date, unique_val, L4):
		"""
		Initiates the "brains" of the operation (i.e., _augmentTravel function) including complex data engineering in
		Pandas and the ArcGIS API for Python for spatio-temporal interpolation processes. After the operation, extract
		and concat dataframes from the dataframe itself (i.e., stored in enhanced_df field), export as CSV file, and
		compute the error rate (i.e., % data has been lost during interpolation process due to data integrity or
		spatial operation issue).

		:param enrich_df: The enriched dataframe.
		:param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
		:param stop_times: The schedule (from GTFS static) per stop_id per trip_id (vehicle's id associated to transit rte.).
		:param wkid: Spatial reference to project polylines.
		:param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-09-30).
		:param output_folder: Contents exported & stored in the output folder.
		:param raw_date: The date of the collected raw GTFS-RT.
		:param unique_val: The unique-rte currently inspecting.
		:param L4: The list that is part of the Manager in Multiprocessing - report error rate & error type (if applicable).

		:return: Either a concatenated dataframe that has undergone spatio-temporal interpolation processes or None
				 due to failure. For schema of concatenated dataframe, see _execEnhanced for more details.
		"""

		stop_times = stop_times.rename(columns={'stop_sequence' : 'stop_seque'})

		try:
			final_df = (
				enrich_df
					.groupby(['trip_id'])
					.apply(lambda e: e.assign(stp_id_shift   = lambda d: d['stop_id'].shift(-1),
				                              stp_sq_shift   = lambda d: d['stop_seque'].shift(-1),
				                              stp_diff_shift = lambda d: d['Stp_Diff'].shift(-1),
				                              index_shift    = lambda d: d['index'].shift(-1),
				                              x_shift        = lambda d: d['x'].shift(-1),
				                              y_shift        = lambda d: d['y'].shift(-1),
				                              enhanced_df    = lambda d: d[['trip_id', 'x', 'y', 'stop_seque', 'index', 'Status',
					                                                        'x_shift', 'y_shift', 'stp_sq_shift', 'index_shift',
					                                                        'stat_shift', 'stp_diff_shift', 'delta_time',
					                                                        'Local_Time', 'time_shift', 'idx']]
				                                                            .apply(lambda r:
				                                                                    self._augmentTravel(*r, undiss_df,
				                                                                                        stop_times, wkid),
				                                                                   axis=1)))
			)

			extract_df = (
				final_df.assign(val_type=lambda d: d['enhanced_df'].apply(lambda r: type(r)))
			)

			str_df     = extract_df[extract_df['enhanced_df'].apply(lambda x: isinstance(x, str))] # Errors that should have been part of the concat_dfs
			na_df      = extract_df[extract_df['enhanced_df'].apply(lambda x: isinstance(x, type(None)))] # N/A --> 2nd consec. does not exist; check for errors
			errors     = len(na_df) - len(na_df.query('stat_shift.isnull()', engine="python")) # The # of miscalculated ones that should have been part of the concat_dfs

			get_df     = extract_df[extract_df['enhanced_df'].apply(lambda x: isinstance(x, DataFrame))]
			concat_dfs = get_df['enhanced_df'].pipe(lambda r: concat(r.tolist())) # The final dataframe

			error_rate = round((len(str_df) + errors) / (len(concat_dfs) + errors + (len(str_df))),3)

			L4.append(f"{unique_val},{raw_date},{folder_date},{error_rate}")

			df_name = f"{output_folder}/{raw_date}_{unique_val}_interpolated.csv"
			concat_dfs.to_csv(df_name, index=False)

			return concat_dfs

		except ValueError as e:
			L4.append(f"{unique_val},{raw_date},{folder_date},Failure to concat")
			return None