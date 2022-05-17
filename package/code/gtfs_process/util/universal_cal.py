"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada.
Date: Re-modified Q1-2022

About: 3 Classes with their own unique purpose.
		a) CalcSemiDf - starts building the dataframe in preparation for data augmentation.
					  - Schema: stop_seque, end_paths, dist, Tot_Dist

	    b) CalcEnhanceDf - Data Augmentation
	                     - Schema: trip_id, idx, stop_id, stop_seque, status, proj_speed, x, y,
	                               Tot_Dist, dist, dist_futr, futr_trvel, proj_trvel, curr_time,
	                               est_arr, off_earr, tmp_arr, sched_arr, arr_tmedif, off_arrdif,
	                               perc_chge, perf_rate, dept_time, end_path

       c) RefineDf -
"""

from pandas import DataFrame, concat
from typing import List
from .deltas import TimeDelta
import datetime as dt


class CalcSemiDf:

	def __init__(self, consec_stpseq: List, consec_pths: List, consec_dist: List, btwn_df: DataFrame):
		"""
		Concatenates the 1st and 2nd consecutive veh. with dataframe that happened in-between (only mult-stop).

		:param consec_stpseq: A list containing the first and last stop sequence of the consecutive pair.
		:param consec_pths: A list containing the drawn paths from the 1st veh to its stop sequence (forward) and the 2nd veh past from its previous stop sequence (reverse).
		:param consec_dist: A list containing the list travelled from the 1st veh to its stop sequence and the 2nd veh travelled past from its previous stop sequence (reverse).
		:param btwn_df: If applicable (in-between stops classifier), the dataframe of the stops in-between 1st & 2nd veh; Schema: stop_seque, end_path, dist.

		:returns: Semi-final dataframe with the following schema: stop_seque, end_path, dist, and Tot_Dist (total distance covered).
		"""

		self.semi_df = self._enhance_semi_df(consec_stpseq=consec_stpseq,
		                                     consec_pths=consec_pths,
		                                     consec_dist=consec_dist,
		                                     btwn_df=btwn_df)


	def _enhance_semi_df(self, consec_stpseq, consec_pths, consec_dist, btwn_df):
		"""
		:param consec_stpseq: A list containing the first and last stop sequence of the consecutive pair.
		:param consec_pths: A list containing the drawn paths from the 1st veh to its stop sequence (forward) and the 2nd veh past from its previous stop sequence (reverse).
		:param consec_dist: A list containing the list travelled from the 1st veh to its stop sequence and the 2nd veh travelled past from its previous stop sequence (reverse).
		:param btwn_df: If applicable (in-between stops classifier), the dataframe of the stops in-between 1st & 2nd veh; Schema: stop_seque, end_path, dist.
		:return: Semi-final dataframe.
		"""

		# If in-between dataframe exists - otherwise proceed to else.
		if btwn_df is not None:

			tmp_df = DataFrame({'stop_seque': consec_stpseq,
			                    'end_path': consec_pths,
			                    'dist'    : consec_dist})

			robust_df = (
				concat([tmp_df, btwn_df])
					.sort_values(['stop_seque'])
					.pipe(lambda d: d.assign(Tot_Dist = d['dist'].sum()))
			)

			return robust_df

		else:

			tmp_df = (
				DataFrame({'stop_seque' : consec_stpseq,
		                   'end_path'   : consec_pths,
		                   'dist'       : consec_dist})
					.sort_values(['stop_seque'])
					.pipe(lambda d: d.assign(Tot_Dist = d['dist'].sum()))
			)

			return tmp_df


class CalcEnhanceDf:

	def __init__(self, semi_final_df: DataFrame, trip_id, proj_speed, status, stat_shift, mid_stat, local_time,
	             time_shift, future_dist, stop_times: DataFrame, idx, x1, y1, x2, y2, travel_type):
		"""
		Enhances the semi-final dataframe with important variables - aka data augmentation.

		:param semi_final_df: Concatenated dataframe with the following schema: stop_seque, end_path, dist, and Tot_Dist
		:param trip_id: The trip_id being currently assessed.
		:param proj_speed: Calculated projected travel speed (km/h) from time and distance delta between the consecutive pair.
		:param status: Current travel status of the 1st veh.
		:param stat_shift: Current travel status of the 2nd veh.
		:param mid_stat: The connection type that happened in between the 1st and 2nd veh. (e.g., In-Between, One-Stop, etc.).
		:param local_time: The timestamp recorded from the 1st veh.
		:param time_shift: The timestamp recorded from the 2nd veh.
		:param future_dist: The distance that will need to be travelled in the near-future for the 2nd veh.
		:param stop_times: Static GTFS file with scheduled/expected arrival_time and departure time for each stop per trip_id.
		:param idx: The index - determining the number of vehicle movement in the consecutive group.
		:param x1: Snapped x-coordinate of the 1st vehicle.
		:param y1: Snapped y-coordinate of the 1st vehicle.
		:param x2: Snapped x-coordinate of the 2nd vehicle.
		:param y2: Snapped y-coordiante of the 2nd vehicle.
		:param travel_type: The type of movement travel (e.g., movement, one stop, same stop - same segment, same stop - different segment).

		:return: An enhanced final dataframe - see the _enhance_final_df function of what the output schema is.
		"""

		self.final_df = self._enhance_final_df(semi_final_df=semi_final_df,
		                                       trip_id=trip_id,
		                                       proj_speed=proj_speed,
		                                       status=status,
		                                       stat_shift=stat_shift,
		                                       mid_stat=mid_stat,
		                                       local_time=local_time,
		                                       time_shift=time_shift,
		                                       future_dist=future_dist,
		                                       stop_times=stop_times,
		                                       idx=idx,
		                                       x1=x1, y1=y1,
		                                       x2=x2, y2=y2,
		                                       travel_type=travel_type)


	def _addTime(self, tmp_curr_time, proj_trvel):
		"""
		Estimates the arrival time from the current timestamp and projected travel time.

		:param tmp_curr_time: The current timestamp.
		:param proj_trvel: Projected travel time estimated (seconds).

		:return: New timestamp - formatted: 'YYYY-mm-DD HH:MM:SS'
		"""

		return str(dt.datetime.strptime(tmp_curr_time, '%Y-%m-%d %H:%M:%S') + dt.timedelta(0, proj_trvel))


	def _estTime(self, curr_time, proj_trvel, btwn_df):
		"""
		Iteratively & cumulatively estimate the arrival time for each observation in the group, except last.

		Dependent function(s): _addTime

		:param curr_time: Current timestamp - starting with the 1st veh.
		:param proj_trvel: Estimated travel time - starting with the 1st veh.
		:param btwn_df: In-between dataframe (between 1st and 2nd veh.) to assess estimated travel time.

		:return: A list with estimated arrival time for each observation in the group, except last - set as None.
		"""

		frst_esttime = self._addTime(tmp_curr_time=curr_time, proj_trvel=proj_trvel)
		idx_esttime  = frst_esttime # Variable used to switch values and calculate estimated arrival time.

		est_time = [frst_esttime]  # Add estimate arrival time for first observation
		for tmp_time in btwn_df['proj_trvel']:  # In-between including one-stoppers
			tmp_esttime = self._addTime(tmp_curr_time=idx_esttime, proj_trvel=tmp_time)
			idx_esttime = tmp_esttime
			est_time.append(tmp_esttime)

		est_time.append(None)  # Add None for the last observation b/c distance to its stop sequence is not calculated in this instance.

		return est_time


	def _classifyOnTime(self, value):
		"""
		Classifies each observation if it is late, on-time, or early.

		:param value: Individual value (seconds) from off_arrdif.

		:return: Str value that classified on-time performance.
		"""

		if value <= -120:
			return "Late"

		elif -120 < value < 300:
			return "On-Time"

		elif value >= 300:
			return "Early"


	def _perfChange(self, value, value2):
		"""
		Identify performance change over time. Higher would indicate more abrupt change in travel.
		Abrupt changes could possibly indicate traffic incident or surge in passenger boardings / alightings or
		speeding up to catch up with their next transit stops. These may happen over space-time broadly.

		:param value: Individual value from off_arrdif.
		:param value2: 2nd individual value (shift -1) from off_arrdif.

		:return: Percentage value.
		"""

		try:
			change      = value2 - value
			perf_change = round((change / value)*100, 2)

			# if value2 is greater than value and the performance change is less than zero - turn to positive.
			# Indicate improvement
			if (value < value2) and (perf_change < 0):
				return -perf_change

			else:
				return perf_change

		except Exception as e:
			return None


	def _enhance_final_df(self, semi_final_df: DataFrame, trip_id, proj_speed, status, stat_shift, mid_stat,
	                      local_time, time_shift, future_dist, stop_times, idx, x1, y1, x2, y2, travel_type):
		"""
		Enhance the semi-final dataframe with important variables.

		Dependent function(s): _estTime, _perfChange, _classifyOnTime
		Dependent classes: TimeDelta -> Identify time changes in seconds.

		:param semi_final_df: Concatenated dataframe with the following schema: stop_seque, end_path, dist, and Tot_Dist
		:param trip_id: The trip_id being currently assessed.
		:param proj_speed: Calculated projected travel speed (km/h) from time and distance delta between the consecutive pair.
		:param status: Current travel status of the 1st veh.
		:param stat_shift: Current travel status of the 2nd veh.
		:param mid_stat: The connection type that happened in between the 1st and 2nd veh. (e.g., In-Between, One-Stop, etc.).
		:param local_time: The timestamp recorded from the 1st veh.
		:param time_shift: The timestamp recorded from the 2nd veh.
		:param future_dist: The distance that will need to be travelled in the near-future for the 2nd veh.
		:param stop_times: Static GTFS file with scheduled/expected arrival_time and departure time for each stop per trip_id.
		:param idx: The recorded movement indicator.
		:param x1: Snapped x-coordinate of the 1st vehicle.
		:param y1: Snapped y-coordinate of the 1st vehicle.
		:param x2: Snapped x-coordinate of the 2nd vehicle.
		:param y2: Snapped y-coordinate of the 2nd vehicle.
		:param travel_type: The type of movement travel (e.g., movement, one stop, same stop - same segment, same stop - different segment, stationary, terminus).

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

		# Query by trip_id in the stop_times GTFS static file (improve efficiency) - acquire scheduled/expected arrival_time and departure_time
		sub_stp_time_df = stop_times.query('trip_id == @trip_id')

		keep_col = ['trip_id', 'idx', 'stop_id', 'stop_seque', 'status', 'proj_speed', 'x', 'y', 'Tot_Dist', 'dist',
		            'dist_futr', 'futr_trvel', 'proj_trvel', 'curr_time', 'est_arr', 'off_earr', 'tmp_arr',
				    'sched_arr', 'arr_tmedif', 'off_arrdif', 'perc_chge', 'perf_rate', 'dept_time', 'end_path']

		if travel_type == "Multiple Stops" or travel_type == "One Stop":
			# Repeat the number of times in between the status. Exclude beginning and end; hence subtract by 2
			repeat_mid = len(semi_final_df) - 2

			# Project travel time for the future - after the 2nd vehicle based on current projected speed and distance need to travel.
			future_trvel = [round(((future_dist / 1000) / proj_speed) * 3600) if future_dist is not None else None][0]

			# Estimate arrival time at the end of destination of its current stop sequence - for the 2nd vehicle.
			future_arr   = [self._addTime(tmp_curr_time=time_shift, proj_trvel=future_trvel) if future_trvel is not None else None][0]

			# Pre-build the dataframe as lists:
			# movement status, current time (recorded), distance required to complete (applies only 2nd veh.), and future projected travel time.
			order_stat = [status] + repeat_mid * [mid_stat] + [stat_shift]
			curr_time  = [local_time] + repeat_mid * [None] + [time_shift]
			dist_futr  = [None] + repeat_mid * [None] + [future_dist]
			future_lst = [None] + repeat_mid * [None] + [future_trvel]
			idx        = [idx] + repeat_mid * [idx] + [idx]
			x          = [x1] + repeat_mid * [None] + [x2]
			y          = [y1] + repeat_mid * [None] + [y2]

			# Build the dataframe and merge with the queried stop_times.
			fin_df = (
				semi_final_df
					.assign(trip_id    = trip_id,
                            idx        = idx,
                            x          = x,
                            y          = y,
				            curr_time  = curr_time,
				            proj_speed = proj_speed,
				            proj_trvel = lambda d: round(((d['dist'] / 1000) / proj_speed) * 3600), # Get projected travel time in seconds
				            status     = order_stat,
				            dist_futr  = dist_futr,
				            futr_trvel = future_lst)
					.merge(sub_stp_time_df, on=['trip_id', 'stop_seque'])
			)

			## Prepare to calculate iteratively - estimated arrival time
			frst_proj_trvel = fin_df['proj_trvel'].iloc[0]  # Get the first projected travel time (sec.) - aka 1st veh.
			frst_curr_time  = fin_df['curr_time'].iloc[0]  # Get the recorded time of the 1st veh. (consecutive pair)
			btwn_df         = fin_df.iloc[1:-1]  # Get only in-between to assess estimate time - exclude 1st and last observation
			est_time_list   = self._estTime(curr_time=frst_curr_time,
			                                proj_trvel=frst_proj_trvel,
			                                btwn_df=btwn_df)

			# Build the final dataframe
			final_df = (
				fin_df
					.assign(est_arr    = est_time_list,  # Estimated arrival time
				            draft_date = lambda d: d['est_arr'].str.split(" ").str[0],
				            sched_arr  = lambda d: d['draft_date'].iloc[0] + " " + d['arrival_time'],
				            tmp_arr    = lambda d: d['draft_date'] + " " + d['arrival_time'],           # Combine day with hour and second (e.g., 2021-09-30 13:40:30)
				            # The arrival time difference - comparison between estimated arrival time and expected arrival time
				            arr_tmedif = lambda d: d[['est_arr', 'tmp_arr']].apply(lambda r: TimeDelta(*r).change_time, axis=1),
				            off_earr   = est_time_list[0:-1] + [future_arr]) # Official estimated arrival_time including the 2nd veh. loc
			)

			## Prepare to calculate the time difference from estimated arrival time and scheduled arrival time for the 2nd veh. loc.
			last_off_est_arr = final_df['off_earr'].iloc[-1]
			last_sched_arr   = final_df['sched_arr'].iloc[-1]
			arr_tme_dif      = list(final_df['arr_tmedif'])[0:-1] # From first to second last observation during consecutive pair.
			last_off_tme_dif = [TimeDelta(last_off_est_arr, last_sched_arr).change_time if (last_sched_arr and last_sched_arr) is not None else None][0]
			off_tme_dif      = arr_tme_dif + [last_off_tme_dif]

			## Finalize the interpolated dataframe and return
			final_df = (
				final_df
					.assign(off_arrdif       = off_tme_dif,  # Assign official time difference in all observations - determine what is late, on-time, early
							off_arrdif_shift = lambda d: d['off_arrdif'].shift(-1),
	                        tmp_change       = lambda d: d[['off_arrdif', 'off_arrdif_shift']].apply(lambda e: self._perfChange(*e), axis=1),
	                        perc_chge        = lambda d: d['tmp_change'].shift(1),
	                        perf_rate        = lambda d: d['off_arrdif'].apply(lambda e: self._classifyOnTime(e)))
					.rename(columns = {'departure_time' : 'dept_time'})
				[keep_col]
			)

			return final_df


		elif (travel_type == "Stationary"):

			final_df = (
				semi_final_df
					.assign(trip_id    = trip_id,
                            idx        = idx,
                            x          = x1,
                            y          = y1,
				            curr_time  = local_time,
				            proj_speed = proj_speed,
				            proj_trvel = None,
				            status     = 'Stationary',
				            dist_futr  = None,
				            futr_trvel = None)
					.merge(sub_stp_time_df, on=['trip_id', 'stop_seque'])
					.rename(columns = {'departure_time' : 'dept_time'})
					.assign(est_arr    = None,
				            draft_date = lambda d: d['curr_time'].str.split(" ").str[0],
				            tmp_arr    = lambda d: d['draft_date'] + " " + d['arrival_time'],
				            off_earr   = None,
				            sched_arr  = None,
				            arr_tmedif = None,
				            off_arrdif = None,
				            perc_chge  = None,
				            perf_rate  = None)
				[keep_col]
			)

			return final_df


		elif (travel_type == "Same Stop - Same Segment") or (travel_type == "Same Stop - Different Segment") or (travel_type == "Terminus - Same Segment") or (travel_type == "Terminus - Different Segment"):

			if travel_type == "Same Stop - Same Segment" or travel_type == "Terminus - Same Segment":
				seg_stat = "Same Segment"

			else:
				seg_stat = "Different Segment"


			# Flatten out nested distance
			first_dist  = future_dist[0]
			future_dist = future_dist[1]

			if future_dist is None:
				future_dist = 0

			order_stat = [f"{status}-{seg_stat}", f"{stat_shift}-{seg_stat}"]
			curr_time = [local_time, time_shift]
			dist_futr = [first_dist, future_dist]
			idx       = [idx, idx]
			x = [x1, x2]
			y = [y1, y2]

			try:
				# Calculate future travel for both veh. points.
				future_trvel_1 = round(((first_dist/1000)/proj_speed)*3600)
				future_trvel_2 = round(((future_dist/1000) / proj_speed) * 3600)

				future_lst = [future_trvel_1, future_trvel_2]
				proj_trvel = [round(((f/1000)/proj_speed)*3600) for f in future_lst]

				# Use futr_trvel to estimate proj_trvel instead!!
				final_df = (
					semi_final_df
						.assign(trip_id    = trip_id,
                                idx        = idx,
                                x          = x,
                                y          = y,
					            curr_time  = curr_time,
					            proj_speed = proj_speed,
					            futr_trvel = future_lst,
					            proj_trvel = proj_trvel,
					            status     = order_stat,
					            dist_futr  = dist_futr,
					            est_arr    = lambda d: d[['curr_time', 'proj_trvel']].apply(lambda e: self._addTime(*e), axis=1),
					            draft_date = lambda d: d['curr_time'].str.split(" ").str[0])
						.merge(sub_stp_time_df, on=['trip_id', 'stop_seque'])
						.rename(columns={'departure_time': 'dept_time'})
						.assign(sched_arr  = lambda d: d['draft_date'].iloc[0] + " " + d['arrival_time'],
					            tmp_arr    = lambda d: d['draft_date'] + " " + d['arrival_time'],
					            arr_tmedif = lambda d: d[['est_arr', 'tmp_arr']].apply(lambda r: TimeDelta(*r).change_time, axis=1),
					            off_earr   = lambda d: d['est_arr'],
					            off_arrdif = lambda d: d['arr_tmedif'],
                                off_arrdif_shift = lambda d: d['off_arrdif'].shift(-1),
                                tmp_change = lambda d: d[['off_arrdif', 'off_arrdif_shift']].apply(lambda e: self._perfChange(*e), axis=1),
                                perc_chge  = lambda d: d['tmp_change'].shift(1),
                                perf_rate  = lambda d: d['off_arrdif'].apply(lambda e: self._classifyOnTime(e))
					    )
					[keep_col]
				)

				return final_df

			# RESOLVE THIS
			except Exception as e:

				# For any failure to calculate.
				fin_df = (
					semi_final_df
						.assign(trip_id=trip_id,
                                idx=idx,
                                x  = x,
                                y  = y,
					            curr_time=curr_time,
					            proj_speed=proj_speed,
					            futr_trvel=None,
					            proj_trvel=None,
					            status=order_stat,
					            dist_futr=dist_futr,
					            est_arr=None,
					            sched_arr=None,
                                tmp_arr=None,
                                arr_tmedif=None,
                                off_earr=None,
                                off_arrdif=None,
                                perc_chge=None,
                                perf_rate=None)
						.merge(sub_stp_time_df, on=['trip_id', 'stop_seque'])
						.rename(columns={'departure_time': 'dept_time'})
					[keep_col]
				)

				return fin_df