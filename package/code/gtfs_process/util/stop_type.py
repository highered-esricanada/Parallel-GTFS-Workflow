"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada.
Date: Re-modified Q1 - 2022

About: * Building paths by connection/travel type from the consecutive pair - HIGHLY CONTINGENT ON THIS!
	   * Calculate total distance travelled
	   * Initiate construction of the dataframe (stop_seque, end_path, dist, Tot_Dist) in preparation for data augmentation.
	   * Calculate future distance travelled (2nd veh - in all connection types; 1st veh - ONLY in same stop type)
"""

from .build_segs import PrepareSeg, BridgeVehRestSeg
from .deltas import SpatialDelta
from .universal_cal import CalcSemiDf
from pandas import DataFrame, concat, json_normalize


class BtwnStps:

	def __init__(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid):
		"""
		What happened between the 1st and 2nd veh of consec. pair - more than 1 transit stop has been passed and not recorded.
		Draw paths:
			1st veh           --> stop sequence en-transit to.
			nth stop sequence --> nth stop sequence
			nth stop sequence --> 2nd veh (en-transit).
			2nd veh           --> future stop sequence en-transit to.

		Conclude:
		    1. Type of path (in-between, or pseudo in-between (missing link))
		    2. Distance covered
		    3. Structured dataframe in preparation for data augmentation - schema:
		        a) stop_seque                      --> Filled in all including unrecorded
		        b) end_path (aka paths drawn)      --> Filled in all stop sequence including unrecorded
		        c) dist (covered)                  --> Filled in all stop sequence per individual path
		        d) Tot_Dist (Total dist traveled)  --> Filled in all stop sequence (same value across all for consistency).
		    4. Future distance (2nd veh --> stop sequence en-transit to)

		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 1st veh.
		:param x2: Snapped x-coordinate (longitude) of the 2nd veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.
		"""

		self.btwn_info = self._btwn_stops(stp_seq=stp_seq, stp_seq2=stp_seq2,
		                                  x1=x1, y1=y1, x2=x2, y2=y2,
		                                  index=index, index2=index2,
		                                  undiss_df=undiss_df, wkid=wkid)


	def _build_multi_path(self, undiss_rte: DataFrame, wkid):
		"""
		Automatically build the paths that happen in between the 1st and 2nd veh. of the consecutive pair.

		Dependent Classes: SpatialDelta;

		:param undiss_rte: Queried undissolved spatial dataframe - to extract coordinates and link them together
							in preparation for measurement.

		:return: Tuple (0: Path in-between; 1: DataFrame built in-between).
		"""

		btwn_df = (
			concat([json_normalize(undiss_rte['SHAPE']), undiss_rte.reset_index()], axis=1)
				.assign(end_path=lambda d: d['paths'].apply(lambda e: e[0][-1]))
			[['stop_seque', 'end_path']]
		)

		# Generate the appropriate geometry path for in-between.
		# Used to inject between the first en-transit and second en-transit.
		btwn_path = [[b] for b in btwn_df['end_path']]

		btwn_dist = (
			concat([json_normalize(undiss_rte['SHAPE']), undiss_rte.reset_index()], axis=1)
			[['stop_seque', 'paths']]
				.pipe(lambda d: d.assign(dist=[SpatialDelta(paths=path, wkid=wkid).dist for path in d['paths']]))
				.groupby(['stop_seque'], as_index=False)
				.apply(lambda e: e.assign(end_path=lambda d: d['paths'].apply(lambda f: f[0][-1])))
				.groupby(['stop_seque'], as_index=False)
				.agg({'end_path': lambda x: x.tolist(), 'dist': 'sum'})
			[['stop_seque', 'end_path', 'dist']]
		)

		return (btwn_path, btwn_dist)


	def _btwn_stops(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid):
		"""
		See conclude section (~line 28) and param section (~line 38-47) for more details.

		Dependent function(s): _build_multi_path - ONLY if in-between actually happened.

		Dependent Classes:
			1) PrepareSeg   --> Build paths (1st veh. -> stop seq.; nth stop seq. -> nth stop seq.;
											 nth stop seq -> 2nd veh.; 2nd veh. -> nth stop seq.)
			2) SpatialDelta --> Build Geometry Polyline (ArcGIS) & acquire length (aka distance).
			3) CalcSemiDf   --> Build the dataframe (schema: stop_seque, end_paths, dist, tot_dist)
								in preparation for data augmentation.

		:return: Tuple (0: Connection Type; 1: Total distance travelled;
						2: Semifinal DataFrame; 3: Future distance to be traveled).
		"""

		# Trace segment information - first and last
		segs = PrepareSeg(x1=x1, y1=y1, stp_seq=stp_seq, index=index,
		                  x2=x2, y2=y2, stp_seq2=stp_seq2, index2=index2,
		                  undiss_df=undiss_df, wkid=wkid).traced_seg

		build_segs    = segs[0] # List - Nested containing either [first_path, end_path] or [first_path].
		consec_pths   = segs[1] # List - Path drawn for 1st veh. (forward) and 2nd veh. (backward)
		consec_dist   = segs[2] # List - Distance travelled by 1st veh. (forward) and 2nd veh. (backward)
		consec_stpseq = segs[3] # List - 1st veh en-transit stop seq. and 2nd veh. en-transit stop seq.
		future_dist   = segs[4] # Future distance value

		# Generate - stop range in between
		stp_range = [r for r in range(int(stp_seq) + 1, int(stp_seq2))]

		# Get shape undissolved segments from the identified in-between stops from undissolved shapefile.
		btwn_undiss_rte = undiss_df.query('stop_seque in @stp_range').sort_values(['stop_seque', 'index'])

		# Safety switch - if there is really nothing in the query but the stop range appears greater than or equal to 1
		#                 then, it indicates likely data integrity issue in the GTFS static files - Compensate to adjust.
		if len(btwn_undiss_rte) == 0 and len(stp_range) >= 1:

			# Calculate distance travelled total
			connect_seg = [ss for s in build_segs for ss in s]
			dist        = SpatialDelta(paths=connect_seg, wkid=wkid).dist

			# Create Semi-final dataframe in preparation for data augmentation
			robust_df = CalcSemiDf(consec_stpseq=consec_stpseq,
			                       consec_pths=consec_pths,
			                       consec_dist=consec_dist,
			                       btwn_df=None).semi_df    # Nothing happened in-between, set to None

			return ('One Stop (Not In-Between) - By Veh. Pair', dist, robust_df, future_dist)

		else:
			# Build in-between path via stop sequences that the vehicle has passed during consecutive recording
			btwn_stop_seg = self._build_multi_path(undiss_rte=btwn_undiss_rte, wkid=wkid)
			btwn_pth      = btwn_stop_seg[0]

			# Between dataframe used to append with 1st and 2nd veh.
			# aka the lists (consc_stpseq, consec_pths, consec_dist).
			btwn_df       = btwn_stop_seg[1]
			robust_df     = CalcSemiDf(consec_stpseq=consec_stpseq,
			                           consec_pths=consec_pths,
			                           consec_dist=consec_dist,
			                           btwn_df=btwn_df).semi_df

			dist = robust_df['Tot_Dist'].iloc[0]

			return ('In-Between', dist, robust_df, future_dist)


class OneStp:

	def __init__(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid):
		"""
		What happened between the 1st and 2nd veh of consec. pair - from a one-stop difference.
		Draw paths:
			1st veh           --> stop sequence en-transit to.
			stop sequence     --> 2nd veh (en-transit).
			2nd veh           --> future stop sequence en-transit to.

		Conclude:
		    1. Type of path (one-stop)
		    2. Distance covered
		    3. Structured dataframe in preparation for data augmentation - schema:
		        a) stop_seque                      --> Filled in all including unrecorded
		        b) end_path (aka paths drawn)      --> Filled in all stop sequence including unrecorded
		        c) dist (covered)                  --> Filled in all stop sequence per individual path
		        d) Tot_Dist (Total dist traveled)  --> Filled in all stop sequence (same value across all for consistency).
		    4. Future distance (2nd veh --> stop sequence en-transit to)

		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 1st veh.
		:param x2: Snapped x-coordinate (longitude) of the 2nd veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.
		"""

		self.one_info = self._onestop(stp_seq=stp_seq, stp_seq2=stp_seq2,
					      			  x1=x1, y1=y1, x2=x2, y2=y2,
									  index=index, index2=index2,
									  undiss_df=undiss_df, wkid=wkid)


	def _onestop(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid):
		"""
		See conclude section (~line 170) and param section (~line 180-189) for more details.

		Dependent Classes:
			1) PrepareSeg   --> Build paths (1st veh. -> stop seq.;  stop_seq -> 2nd veh.; 2nd veh. -> nth stop seq.)
			2) SpatialDelta --> Build Geometry Polyline (ArcGIS) & acquire length (aka distance).
			3) CalcSemiDf   --> Build the dataframe (schema: stop_seque, end_paths, dist, tot_dist)
								in preparation for data augmentation.

		:return: Tuple (0: Connection Type; 1: Total distance travelled;
						2: Semifinal DataFrame; 3: Future distance to be traveled).
		"""

		# Trace segment information - first and last
		segs = PrepareSeg(x1=x1, y1=y1, stp_seq=stp_seq, index=index,
		                  x2=x2, y2=y2, stp_seq2=stp_seq2, index2=index2,
		                  undiss_df=undiss_df, wkid=wkid).traced_seg

		build_segs    = segs[0] # List - Nested containing either [first_path, end_path] or [first_path].
		consec_pths   = segs[1] # List - Path drawn for 1st veh. (forward) and 2nd veh. (backward).
		consec_dist   = segs[2] # List - Distance travelled by 1st veh. (forward) and 2nd veh. (backward).
		consec_stpseq = segs[3] # List - 1st veh. en-transit stop seq. and 2nd veh. en-transit stop seq.
		future_dist   = segs[4] # List - Future distance value (2nd veh. -> stop seq.).

		connect_seg = [ss for s in build_segs for ss in s] # Reformat the connecting segments start (1st) to end (2nd).
		dist        = SpatialDelta(paths=connect_seg, wkid=wkid).dist # Acquire the total distance travelled.

		# Create Semi-final dataframe in preparation for data augmentation
		robust_df = CalcSemiDf(consec_stpseq=consec_stpseq,
		                       consec_pths=consec_pths,
		                       consec_dist=consec_dist,
		                       btwn_df=None).semi_df    # Nothing in-between happened (> 1 stop), set to None.

		return ('One Stop', dist, robust_df, future_dist)


class SameStp:

	def __init__(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid, conx_type):
		"""
		What happened between the 1st and 2nd veh of consec. pair - from the same stop (no real difference).
		Draw paths:
			For Same Stop - Same Segment:   1st veh --> 2nd veh. (en-transit).

			Same Stop - Different Segment: 	1st veh --> nth segment --> nth segment --> 2nd veh. (en-transit)

			To compare projections: 1st veh --> 2nd veh. --> stop seq (same). (Future Distance: 1st veh --> stop seq)
									            2nd veh. --> stop seq (same). (Future Distance: 2nd veh --> stop seq)

		Conclude:
		    1. Type of path (Same Stop - Same Segment; Same Stop - Different Segment)
		    2. Distance covered
		    3. Structured dataframe in preparation for data augmentation - schema:
		        a) stop_seque                      --> Filled in all
		        b) end_path (aka paths drawn)      --> Filled in all stop sequence
		        c) dist (covered)                  --> Filled in all stop sequence per individual path
		        d) Tot_Dist (Total dist traveled)  --> Filled in all stop sequence (same value across all for consistency).
		    4. Future distance (2nd veh --> stop sequence en-transit to)
		    5. Future distance (1st veh --> stop sequence en-transit to)

		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 1st veh.
		:param x2: Snapped x-coordinate (longitude) of the 2nd veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.
		:param conx_type: The connection type: Same Stop - Different Segment; Same Stop - Same Segment
		"""

		self.same_info = self._samestp(stp_seq=stp_seq, stp_seq2=stp_seq2,
		                               x1=x1, y1=y1, x2=x2, y2=y2,
		                               index=index, index2=index2,
		                               undiss_df=undiss_df, wkid=wkid,
		                               conx_type=conx_type)


	def _futureseg(self, stp_seq2, index2, dist, veh_loc_2nd, undiss_df: DataFrame, wkid):
		"""
		Estimates future distance required to travel for both 1st veh and 2nd veh. individually.

		Dependent Classes:
			1) BridgeVehRestSeg --> Build paths (1st veh. --> nth segment --> nth segment --> 2nd veh.; 1st veh --> 2nd veh)
			2) SpatialDelta     --> Build Geometry Polyline (ArcGIS) & acquire length (aka distance).

		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param dist: Distance travelled either from 1st veh --> nth segment --> 2nd veh OR 1st veh --> 2nd veh.
		:param veh_loc_2nd: Nested x,y snapped coordinate list of the 2nd veh. to build future segment paths.
		:param undiss_df: The spatial datafram of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.

		:return: List (0: Future distance from the 2nd veh. OR None if not applicable; 1: Future distance from the 1st veh.)
		"""

		query = f"stop_seque == @stp_seq2 and index == @index2"
		queried_reslt = undiss_df.query(query)
		fut_seg = queried_reslt['SHAPE'].iloc[0]['paths'][0][-1]

		query = f"stop_seque == @stp_seq2 and index > @index2"
		filt_undiss = undiss_df.query(query)

		# Get future distance from 2nd veh. and then add it to the 1st veh.
		if len(filt_undiss) >= 1:
			future_seg      = [veh_loc_2nd, fut_seg]
			future_remain   = BridgeVehRestSeg(filt_undiss=filt_undiss).rest_seg
			future_end_path = [future_seg, future_remain]
			future_dist     = SpatialDelta(paths=future_end_path, wkid=wkid).dist
			# Add future dist from the 2nd veh and dist (1st veh --> 2nd veh.) to get total future distance travel from 1st veh.
			first_dist      = future_dist + dist

			return [future_dist, first_dist]

		else:
			return [None, dist]


	def _samestp(self, stp_seq, stp_seq2, x1, y1, x2, y2, index, index2, undiss_df: DataFrame, wkid, conx_type):
		"""
		See conclude section (~line 248) and param section (~line 259-269) for more details.

		Dependent function(s): _futureseg

		Dependent Classes:
			1) BridgeVehRestSeg --> Build paths (1st veh. --> nth segment --> nth segment --> 2nd veh.; 1st veh --> 2nd veh)
			2) SpatialDelta     --> Build Geometry Polyline (ArcGIS) & acquire length (aka distance).
			3) CalcSemiDf       --> Build the dataframe (schema: stop_seque, end_paths, dist, tot_dist)
								    in preparation for data augmentation.

		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 1st veh.
		:param x2: Snapped x-coordinate (longitude) of the 2nd veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.
		:param conx_type: The connection type: Same Stop - Different Segment; Same Stop - Same Segment

		:return: Tuple (0: Connection Type; 1: Total distance travelled;
						2: Semifinal DataFrame; 3: Future distance to be traveled (2nd veh.);
						4: Future distance to be traveled (1st veh.)).
		"""

		consec_stpseq = [int(stp_seq), int(stp_seq2)]
		veh_loc_1st   = [x1, y1]
		veh_loc_2nd   = [x2, y2]

		if (conx_type == "Same Stop - Different Segment") or (conx_type == "Terminus - Different Segment"): # 1st veh --> nth segment --> nth segment --> 2nd veh
			# Build 1st segment - first
			# Get the segment coordinates of which the 1st vehicle is snapped on to connect.
			query = f"stop_seque == @stp_seq and index == @index"
			# Get the last coord pair (after) of the undissolved segment
			fst_seg = undiss_df.query(query)['SHAPE'].iloc[0]['paths'][0][-1]
			build_1st_seg = [[veh_loc_1st, fst_seg]]

			# Build 2nd segment - end
			query = f"stop_seque == @stp_seq2 and index == @index2"
			# Get the first coord pair (before) of the undissolved segment
			scd_seg = undiss_df.query(query)['SHAPE'].iloc[0]['paths'][0][0]
			build_2nd_seg = [scd_seg, veh_loc_2nd]

			# Build segments in-between first and end - multiple segments (> 1)
			query = f"stop_seque == @stp_seq2 and (@index < index < @index2)"
			filt_undiss = undiss_df.query(query)
			if len(filt_undiss) >= 1:
				btwn_remain = BridgeVehRestSeg(filt_undiss=filt_undiss).rest_seg
				end_path    = [btwn_remain, build_2nd_seg]


				frst_dist   = SpatialDelta(paths=build_1st_seg, wkid=wkid).dist # Dist for 1st veh --> segment (forward)
				end_dist    = SpatialDelta(paths=end_path, wkid=wkid).dist # Dist for nth segment --> 2nd veh. (reverse)
				dist        = frst_dist + end_dist # Total distance travelled (1st veh --> nth segment --> 2nd veh.)
				consec_dist = [dist, 0] # 0 is placed because the 2nd veh is still in the same en-transit stop seq as 1st.

				# To append to DataFrame - consistency purposes
				beg_seg_match = [ss for s in build_1st_seg for ss in s]
				end_seg_match = [ss for s in end_path for ss in s]
				consec_pths   = [beg_seg_match, end_seg_match]

			# Connect segments directly if one or less segment of a difference: 1st veh --> 2nd veh
			else:
				build_seg   = [[veh_loc_1st, fst_seg], build_2nd_seg]
				dist        = SpatialDelta(paths=build_seg, wkid=wkid).dist # Distance travelled 1st veh --> 2nd veh.
				consec_dist = [dist, 0]
				consec_pths = build_seg

			robust_df = CalcSemiDf(consec_stpseq=consec_stpseq,
			                       consec_pths=consec_pths,
			                       consec_dist=consec_dist,
			                       btwn_df=None).semi_df

			futr_inf = self._futureseg(stp_seq2=stp_seq2,
			                           index2=index2,
			                           dist=dist,
			                           veh_loc_2nd=veh_loc_2nd,
			                           undiss_df=undiss_df,
			                           wkid=wkid)

			future_dist = futr_inf[0]
			first_dist  = futr_inf[1]

			return (conx_type, dist, robust_df, future_dist, first_dist)


		elif (conx_type == "Same Stop - Same Segment") or (conx_type == "Terminus - Same Segment"): # 1st veh --> 2nd veh.

			build_1st_seg = [veh_loc_1st, veh_loc_2nd]

			# Get distance between the two
			dist = SpatialDelta(paths=[build_1st_seg], wkid=wkid).dist
			consec_dist = [dist, 0]
			consec_pths = [build_1st_seg, build_1st_seg]

			robust_df = CalcSemiDf(consec_stpseq=consec_stpseq,
			                       consec_pths=consec_pths,
			                       consec_dist=consec_dist,
			                       btwn_df=None).semi_df

			futre_inf = self._futureseg(stp_seq2=stp_seq2,
				                        index2=index2,
			                            dist=dist,
			                            veh_loc_2nd=veh_loc_2nd,
			                            undiss_df=undiss_df,
			                            wkid=wkid)

			future_dist = futre_inf[0]
			first_dist  = futre_inf[1]

			return (conx_type, dist, robust_df, future_dist, first_dist)