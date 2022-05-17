"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada.
Date: Re-modified Q1-2022

About: 2 Classes with their own unique purpose.
	a) BridgeVehRestSeg - Converts the coordinate pairs that have been queried into an appropriate
						  format to build Polyline geometry.
					    - Used in the PrepareSeg Class for consecutive pair connection type: in-between and one-stop.
					    - Used individually for consecutive pair connection type: same stop.

	b) PrepareSeg       - Converts the coordinate pairs that have been queried into an appropriate
						  format to build Polyline geometry via BridgeVehRestSeg.
						- Builds the segment paths from 1st veh        --> stop_seque
														nth stop_seque --> 2nd veh.
						- Calculates distance of each segment path (1st veh --> stop seque; nth stop seque --> 2nd veh.)
"""

from pandas import json_normalize, DataFrame
from .deltas import SpatialDelta


class BridgeVehRestSeg:

	def __init__(self, filt_undiss: DataFrame):
		"""
		Constructs appropriate format of nested coordinate pair paths.

		:param filt_undiss: A queried dataframe from the undissolved segment.
		"""

		self.rest_seg = self._bridge_veh_restseg(filt_undiss=filt_undiss)


	def _bridge_veh_restseg(self, filt_undiss):
		"""
		:param filt_undiss: A queried dataframe from the undissolved segment.

		:return: Formatted nested list of coordinate pairs.
		"""

		# Dictionary to automatically create new columns
		col_order_from_split = {
			1: "y",
			2: "x"
		}

		coord_list = (
			json_normalize(filt_undiss['SHAPE']) # Convert json-style to columns
				.assign(start_coord=lambda d: d['paths'].apply(lambda e: e[0][0]), # start coordinate (x)
			            end_coord=lambda d: d['paths'].apply(lambda e: e[0][-1]))  # end coordinate (y)
			[['start_coord', 'end_coord']]
				.stack()      # Stack these columns into one - start_coord, end_coord
				.reset_index()
				.rename(columns={0: 'Coord'})
				.pipe(lambda d:
				      d.assign(**{col_order_from_split[i + 1]: d['Coord'].str[i] # Split each obs. in coord col.
				                  for i in range(d['Coord'].str.len().max())}))  # Automatically assign x,y fields
				.assign(unique=lambda d: d['y'].astype(str) + "," + d['x'].astype(str)) # Create unique x,y coord
				.drop_duplicates(['unique']) # Drop unnecessary duplicates
		)['Coord'].tolist() # Convert final column of coordinates into list

		return coord_list


class PrepareSeg:

	def __init__(self, x1, y1, stp_seq, index, x2, y2, stp_seq2, index2, undiss_df: DataFrame, wkid):
		"""
		Constructs appropriate format of nested coordinate pair paths, builds segment paths, and calculates distance.

		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param x2: Snapped x-coordinate (longitude) of the 1st veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.
		"""

		self.traced_seg = self._trace_seg(x1=x1, y1=y1, stp_seq=stp_seq, index=index,
		                                  x2=x2, y2=y2, stp_seq2=stp_seq2, index2=index2,
		                                  undiss_df=undiss_df, wkid=wkid)


	def _prepare_seg(self, x1, y1, stp_seq, index, x2, y2, stp_seq2, index2, undiss_df: DataFrame, wkid):
		"""
		Querying segments to build paths from:  1st veh to stop seque;
											    nth stop seque to 2nd veh;
											    2nd veh. to stop seque (if applicable).

		Dependent Classes: BridgeVehRestSeg, SpatialDelta

		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param x2: Snapped x-coordinate (longitude) of the 1st veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.

		:return: Nested list of paths and future distance either:
					a) start, end, future dist
					b) start, end
					c) start
		"""

		######################################################################################################
		########### Build the Start path - from snapped location of 1st veh to its stop sequence #############
		######################################################################################################

		# Create the 1st veh - snapped coordinate
		veh_loc_1st   = [x1, y1]
		query         = f"stop_seque == @stp_seq and index == @index"  # Get the segment coordinates of which the 1st vehicle is snapped on to connect.
		fst_seg       = undiss_df.query(query)['SHAPE'].iloc[0]['paths'][0][-1]  # Get the last coord pair (after) of the undissolved segment
		build_1st_seg = [veh_loc_1st, fst_seg]

		# Build the rest of the path that goes towards the first stop sequence - if applicable (more than one dissolved segment to arrive its stop sequence)
		query = f"stop_seque == @stp_seq and index > @index"
		filt_undiss = undiss_df.query(query)

		if len(filt_undiss) >= 1:

			start_remain_path = BridgeVehRestSeg(filt_undiss=filt_undiss).rest_seg
			final_start_path  = [build_1st_seg, start_remain_path]  # Multi-1st index path to its first stop sequence

		else:
			final_start_path = [build_1st_seg]  # Single index path to its first stop sequence

		######################################################################################################
		########### Build the Εnd path - from snapped location of 2nd veh to its stop sequence ###############
		######################################################################################################

		# Create the 2nd veh - snapped coordinate
		# Certainly if reaching the last observation - then it won't have a consecutive pair - keep safety switch on
		try:
			veh_loc_2nd   = [x2, y2]
			query         = f"stop_seque == @stp_seq2 and index == @index2"
			queried_reslt = undiss_df.query(query)
			# Get the first coord pair (before) of the undissolved segment
			end_seg       = queried_reslt['SHAPE'].iloc[0]['paths'][0][0]
			# Get the other coord pair (future) of the undissolved segment
			fut_seg       = queried_reslt['SHAPE'].iloc[0]['paths'][0][-1]
			build_end_seg = [end_seg, veh_loc_2nd]

			# Build the rest of the path that has past its last stop sequence
			# (aka - go backwards of where it is en-transit to)
			# if applicable (more than one dissolved segment that has past en-transit).
			query       = f"stop_seque == @stp_seq2 and index < @index2"
			filt_undiss = undiss_df.query(query)

			# Building path backwards
			if len(filt_undiss) >= 1:
				end_remain_path = BridgeVehRestSeg(filt_undiss=filt_undiss).rest_seg
				final_end_path  = [end_remain_path, build_end_seg]

				# Build path forward - future
				query       = f"stop_seque == @stp_seq2 and index > @index2"
				filt_undiss = undiss_df.query(query)
				if len(filt_undiss) >= 1:
					future_remain   = BridgeVehRestSeg(filt_undiss=filt_undiss).rest_seg
					future_seg      = [veh_loc_2nd, fut_seg]
					future_end_path = [future_seg, future_remain]
					future_dist     = SpatialDelta(paths=future_end_path, wkid=wkid).dist
					return [[final_start_path, final_end_path], future_dist]

			else:
				final_end_path = [build_end_seg]

			return [[final_start_path, final_end_path]]

		except Exception as e:
			return [final_start_path]


	def _trace_seg(self, x1, y1, stp_seq, index, x2, y2, stp_seq2, index2, undiss_df: DataFrame, wkid):
		"""
		Constructs appropriate format of nested coordinate pair paths, builds segment paths, and calculates distance.

		Dependent function(s): _prepare_seg
		Dependent Classes: SpatialDelta

		:param x1: Snapped x-coordinate (longitude) of the 1st veh.
		:param y1: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq: Stop sequence of the 1st veh. from consecutive pair.
		:param index: The index value of the undissolved segment where the 1st veh. is located.
		:param x2: Snapped x-coordinate (longitude) of the 1st veh.
		:param y2: Snapped y-coordinate (latitude) of the 2nd veh.
		:param stp_seq2: Stop sequence of the 2nd veh. from consecutive pair.
		:param index2: The index value of the undissolved segment where the 2nd veh. is located.
		:param undiss_df: The spatial dataframe of the undissolved polyline segment.
		:param wkid: Spatial reference to project geometry paths.

		:return: Tuple (0: Nested lists of paths - length of 2 = [[final_start_path, final_end_path], future_dist]
												 - length of 1 = [[final_start_path, final_end_path]] or [final_start_path]
						1: Nested list of reformatted paths of 1st veh. and 2nd veh.
						2: List of distances travelled from 1st veh. (forward) and 2nd veh. (reverse).
						3: List of stop sequences en-transit from 1st veh. (toward) and 2nd veh. (toward).
						4: A value of the future distance to be travelled from the 2nd veh.
		"""

		segs = self._prepare_seg(x1=x1, y1=y1, stp_seq=stp_seq, index=index,
		                         x2=x2, y2=y2, stp_seq2=stp_seq2, index2=index2,
		                         undiss_df=undiss_df, wkid=wkid)

		build_segs = segs[0]
		frst_pth   = build_segs[0]
		end_pth    = build_segs[1]

		# if the length is two, then it indicates that the future distance to be travelled from the 2nd veh. exists.
		if len(segs) == 2:
			future_dist = segs[1]

		else:
			future_dist = None

		# Path of beginning segment - to be used to calculate distance & derive estimated time arrival later.
		# Path of end segment
		beg_seg = [s for s in frst_pth]
		end_seg = [s for s in end_pth]

		# To append to DataFrame - consistency purposes
		beg_seg_match = [ss for s in frst_pth for ss in s]
		end_seg_match = [ss for s in end_pth for ss in s]

		beg_dist = SpatialDelta(paths=beg_seg, wkid=wkid).dist  # Dist from start to its stop sequence en-transit.
		end_dist = SpatialDelta(paths=end_seg, wkid=wkid).dist  # Dist from 2nd last stop seq. to end of consecutive veh.

		consec_pths   = [beg_seg_match, end_seg_match] # List - paths of the 1st veh, 2nd veh
		consec_dist   = [beg_dist, end_dist]           # List - distances covered from the 1st veh and 2nd veh.
		consec_stpseq = [int(stp_seq), int(stp_seq2)]  # List - stop sequences en-transit from 1st veh and 2nd veh.

		return (build_segs, consec_pths, consec_dist, consec_stpseq, future_dist)