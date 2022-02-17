## Enrich Transit Vehicle's Location & Mobility Pattern

### A) Purpose
This component of the workflow sets up a trip profile of each consecutive pair of the transit vehicle's locations with additional features. The enriched dataframe contains detailed transit stop, index, geometry point, time, and distance information. These detailed characteristics supplements mobility pattern in preparation to calculate transit metrics. 


### B) Function Details 
The <strong><a href=''>enrich_travel.py</a></strong> script consists of 7 data engineering functions bringing a total of 250 lines of code. Except the <strong>__init__</strong> function, all other functions are described in the table below. 

| Name of Function | Lines | Purpose | 
| :---: | ----- | ----- | 
| ***_get_max_seq_idx*** | 40-68 | An extra layer of QA/QC by validating the quality of the GTFS static files. More specifically, validates if the max stop sequence identified from the stop shapefile exists in the undissolved shapefile. For example, if the terminus is supposed to be 63rd stop of the route, it may be up to 62nd stop in the undissolved shapefile. If that's the case, then it will define the 62nd stop as the terminus and set the validation match to False. Additionally, this function acquires the max index value of the undissolved segment. |
| ***_last_clean*** | 71-85 | A final QA/QC by removing any straggling unordered trends. |
|***_set_mvmt*** | 88-121 | Estimates the movement status of the vehicle by comparing to the next recorded observation. This is done by taking the difference of the stop sequence from the max stop sequence and index from the max index value between the consecutive pair. Details of what would be considered stationary, movement, and terminus stage of the vehicle's mobility status are provided after this table. |
|***_eval_pnts*** | 124-147 | Parses out the string-like geometry point of the consecutive pair. This applies only to consecutive pairs that have been flagged as **stationary-stationary**, **stationary-movement**, and **stationary-terminus**. The parsed out contents are in preparation to construct geometry polyline paths to estimate distance covered over time. |
| ***_get_dist*** | 150-184 | Uses the **_eval_pnts** function and estimates the distance between the consecutive pair via the **SpatialDelta** class. Applies only to consecutive pairs that have been flagged as **stationary-stationary**, **stationary-movement**, and **stationary-terminus**. It validates whether the vehicle has truly been idled/stuck en-transit by having values **less than or equal to 20 metres.** | 
| ***_main*** | 187-250 | The main function that performs data and spatial engineering operations including the creation of new features (i.e., fields) that is binded to the current dataframe. Additionally, it computes the retention rate of the data, which is appended to a list manager, and exports the output as a csv file in the main directory folder - **../data/4_processed/{folder_date}**. The output is an enriched dataframe that is ready for interpolation processing. | 

<br>
<strong>Stationary</strong>: If the difference of the index values <strong>and</strong> stop sequence is zero, then it indicates that the vehicle is likely to be idling/stuck or moving very slowly en-transit (over space and time).
<br>
<br>
<strong>Movement</strong>: If the difference of the index values <strong>and</strong> stop sequence is not zero <strong>and</strong> has not reached terminus stage (max stop sequence and/or max index value), then the vehicle is moving en-transit. 
<br>
<br>
<strong>Terminus</strong>: If the current stop sequence is equal to the max stop sequence <strong>and</strong>, has an index or stop difference greater than zero <strong>or</strong> has an index or stop difference equal to zero. 
<br>
<br>

### C) Required Parameters (Indirect)

The user is not required to insert the parameters for the qaqc.py script. Rather, the backend processes from transform.py inserts it programmatically as part of the downstream workflow. 

| Parameter | Type | Purpose | 
| :-------: | :---: | ------ | 
| ***clean_df*** | DataFrame | The spatial dataframe (in-memory) containing stop_id, stop_sequence, index, objectid, SHAPE of Polyline (undissolved segment), barcode, Local_Time, trip_id, point of the snapped vehicle location, x coordinate, y coordinate, and wkid. |
| ***undiss_df*** | DataFrame | The spatial dataframe (in-memory) of the undissolved segments for the transit route. |
| ***stp_df*** | DataFrame | The spatial dataframe (in-memory) of the transit stop information for the transit route. | 
| ***stop_times*** | DataFrame | DataFrame of the scheduled information (from one of the static GTFS files) per stop_id (transit stop) per trip_id (vehicle's id associated to transit route). |
| ***folder_date*** | Str | The date that belongs to the static GTFS update across the project directory used to process the raw GTFS-RT. An example is "2021-09-30" from "../data/0_external/2021-09-30". |
| ***raw_date*** | Str | The date of the raw GTFS-RT. | 
| ***unique_val*** | Str | The unique transit route currently being processed (e.g., "4-40066-Outbound"). |
| ***L3*** | List | Part of the Manager function in Multiprocessing, it is a list distributed across all CPU cores and stores the following variables: unique_val, raw_date (date of the GTFS-RT), folder_date, and retained (retention rate). After parallel processing, the variables are written in a text file as comma delimited. | 


 
### D) Step Details 
Below are the backend steps (in order) briefly explained followed by a graphic that encapsulates it. 
<ol>
	<li>Execute <strong>self._main</strong> (lines 187-250), get the max stop sequence and index value and validate the quality of the GTFS static file (undissolved shapefile) by running <strong>self._get_max_seq_idx</strong> (lines 203).
		<ul>
			<li>Merge the current dataframe (clean_df) with stop_times dataframe and drop duplicates (lines 206-209).</li>
			<li>Assign three new variables - <strong>MaxIndex, MaxStpSeq, true_max_stp</strong> (lines 210-212).</li>
			<li>Perform final QA/QC sweep three times to remove any potential unorder trends that can tarnish data integrity for future transit metrics downstream of the workflow, and drop the Idx_Diff column (lines 213-216).</li>
			<li>Group by trip_id and create 12 new variables using spatial and data engineering operations (lines 218-229).</li>
			<li>Line 222 when creating <strong>Status</strong> field - execute the <strong>self._set_mvmt</strong> function.</li>
			<li>Line 228 when creating <strong>delta_time</strong> field - execute the <strong>TimeDelta</strong> class from the <strong>util</strong> package.</li>
			<li>Line 229 when creating <strong>delta_dist</strong> field - execute the <strong>self._get_dist</strong> function (indirectly using <strong>SpatialDelta</strong> from the <strong>util</strong> package.)</li>
			<li>Line 230 - drop the <strong>val</strong> field and reorganize the fields (lines 231-237)</li>
		</ul>	
	</li>
	<br>
	<br>
	<li>Get the retention value (lines 240-243), append retention information (lines 248), export the cleaner dataframe to csv file (lines 245-246), and return cleaner dataframe for further downstream processing (lines 250).
</ol>
<br>
<p align='center'><img src='../../../documentation/enrich_flow.JPG' width="700"/></p>
<br>


### E) Spatial DataFrame Output 
| Field | Category | Description | 
| :---: | ---------| ----------- | 
| ***trip_id*** | ID | From the GTFS-RT, the transit route identifier of the vehicle. | 
| ***idx*** | Movement | Cumulate the number of vehicle movements (aka - recordings; not original after QA/QC) per trip_id. | 
| ***barcode*** | Movement | An index to track certain components of the data, particularly trip_id. If the increasing order by 1 (i.e., 1,2,3,4) seems ***out-of-place*** (e.g,. 1,3,4), it indicates that an observation has been omitted due to QA/QC issues. |
| ***Status*** | Movement | Pre-determine the vehicle's mobility status of the vehicle (will require distance as well). |
| ***stat_shift*** | Movement | Shift back by 1 the next observation of the vehicle's mobility status to identify delta mobility. |
| ***stop_id*** | Stop | Identifier of the transit stop of the transit route provided by the static GTFS files. |
| ***stop_seque*** | Stop | Tied to stop_id, the sequence number of the transit stop of the transit route. |
| ***MaxStpSeq*** | Stop | Maximum stop sequence number of the transit route. | 
| ***true_max_stp*** | Stop | Flags the validity of the GTFS static file by comparing the max stop sequence from the stop shapefile with the undissolved shapefile. If true, then the last stop is the true terminus. If false, then whichever is the last stop is the false terminus, which will omit any observations beyond that. This is to maintain data integrity when calculating transit metrics. |
| ***Stp_Left*** | Stop | Calculates how many stops the current vehicle's location of the trip_id has left along its transit route. **Stp_Left** = **MaxStpSeq** - **stop_seque** |
| ***Stp_Diff*** | Stop | Gets the difference of the stop left value (**Stp_Left**) from the previous observation to the current one. **Stp_Diff** = **Stp_Left.diff(1)** | 
| ***objectid*** | Index | Typically one value above the **index** value. Not much value in this field, except it enacts as a divider with the organization of the fields. |
| ***index*** | Index | The value of the index / undissolved segment where the vehicle is accurately located along the transit route. |
| ***MaxIndex*** | Index | Similar to **MaxStpSeq**, identifies the maximum index value of the undissolved segments of the transit route. |
| ***Idx_Left*** | Index | Similar to **Stp_Left**, it finds how many indices (i.e., segments) the current vehicle's location of the trip_id has left along its transit rotue. **Idx_Left** = **MaxIndex** - **index** | 
| ***Idx_Diff*** | Index | Similar to **Stp_Diff**, gets the difference of the index left value (**Idx_Left**) from the previous observation to the current one. **Idx_Diff** = **Idx_Diff.diff(1)** | 
| ***x*** | Point | The x-coordinate (longitude) of the snapped point. | 
| ***y*** | Point | The y-coordinate (latitude) of the snapped point. |
| ***wkid*** | Point | The spatial reference of the snapped point. |
| ***point*** | Point | The shape (as a string to avoid conflict with SHAPE) of the vehicle's snapped point position. |
| ***pnt_shift*** | Point | Shift back by 1 the next observation of the vehicle's snapped point (i.e., location) to calculate delta distance, if applicable to the **Status**-**stat_shift** criteria. |
| ***Local_Time*** | Time | From the GTFS-RT, the timestamp of the vehicle location recorded. |
| ***time_shift*** | Time | Shift back by 1 the next observation of the timestamp of the vehicle's recorded location to calculate delta time. | 
| ***delta_time*** | Time | Calculate the delta time (in seconds) between the consecutive recorded pair of the vehicle. **delta_time** = **time_shift** - **Local_Time** |
| ***arrival_time*** | Time | From the stop time static GTFS file, provides the expected arrival time of the stop_id. | 
| ***departure_time*** | Time | From the stop time static GTFS file, provides the expected departure time from the stop_id. |
| ***delta_dist*** | Spatial | If applicable to the **Status**-**stat_shift** criteria, it'll estimate the distance covered over time. This will identify if the vehicle is precisely stationary/idle/stuck or too slow to move forward. **delta_dist** = **polyline(point, pnt_shift).get_length** | 
| ***SHAPE*** | Polyline | The shape of the undissolved segment as ArcGIS Geometry - Polyline. | 

<br> 
<br> 

| trip_id | idx | barcode | Status | stat_shift | stop_id | stop_seque | MaxStpSeq | true_max_stp | Stp_Left | Stp_Diff | objectid | index | MaxIndex | Idx_Left | Idx_Diff | x | y | wkid | point | pnt_shift | Local_Time | time_shift | delta_time | arrival_time | departure_time | delta_dist | SHAPE |
| ------- | --- | ------- | ------ | ---------- | ------- | ---------- | --------- | ------------ | -------- | -------- | -------- | ----- | -------- | -------- | -------- | --- | --- | ---- | ----- | ------ | --------- | ---------- | ---------- | ------------ | -------------- | ---------- | ----- |
| 57436553 | 1 | 1 | Movement |	Movement | 5042 | 2 | 13 | TRUE | 11 | N/A | 4 | 3 | 119 | 116 | N/A | -114.1138535 | 51.052491 | 4326 | {'x': -114.11385350485796, 'y': 51.052491004134424, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.10929875897381, 'y': 51.05249544381581, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 15:42:42 | 9/30/2021 15:43:42 | 60 | 15:44:00 | 15:44:00 | N/A | {'paths': [[[-114.11478299999999, 51.052490000000034], [-114.11293099999995, 51.05249200000003]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 574365532 | 2 | 3 | Movement | Movement |	8019 | 3 | 13 | TRUE | 10 |	-1 | 8 | 7 | 119 | 112 | -4 | -114.1092988 | 51.05249544 | 4326 | {'x': -114.10929875897381, 'y': 51.05249544381581, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.10399627685548, 'y': 51.05250099992128, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |	9/30/2021 15:43:42 | 9/30/2021 15:44:28 | 46 | 15:46:00 | 15:46:00 | N/A | {'paths': [[[-114.11048199999999, 51.05249400000008], [-114.10802399999994, 51.05249700000007]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
57436553 | 3 |4 | Movement | Movement |	7370 | 4 | 13 | TRUE | 9 | -1 | 16 | 15	| 119 |	104 | -8 | -114.1039963 | 51.052501 | 4326 | {'x': -114.10399627685548, 'y': 51.05250099992128, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.10186009851503, 'y': 51.05250287000256, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 15:44:28 | 9/30/2021 15:44:42 | 14 | 15:48:00 | 15:48:00 | N/A | {'paths': [[[-114.10399799999999, 51.05250100000006], [-114.103096, 51.05250100000006]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 57436553 | 4 | 5 | Movement |	Movement | 5043 | 5 | 13 | TRUE | 8 | -1 | 20 |	19 | 119 | 100 | -4 | -114.1018601 | 51.05250287 | 4326 | {'x': -114.10186009851503, 'y': 51.05250287000256, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.09886927460917, 'y': 51.05251395950055, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 15:44:42 | 9/30/2021 15:45:12 | 30 | 15:50:00 | 15:50:00 | N/A | {'paths': [[[-114.10245299999997, 51.05250200000006], [-114.10108999999994, 51.052504000000056]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 57436553 | 5 | 6 | Movement | Movement | 5043 | 5 | 13 | TRUE	| 8 | 0 | 24 | 23 |	119 | 96 | -4 |	-114.0988693 | 51.05251396 | 4326 | {'x': -114.09886927460917, 'y': 51.05251395950055, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.0956573486328, 'y': 51.0525169997598, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 15:45:12 | 9/30/2021 15:45:58 | 46 | 15:50:00 | 15:50:00 | N/A | {'paths': [[[-114.09889799999996, 51.05251400000003], [-114.09818199999995, 51.05251300000003]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 57436553 | 6 | 7 | Movement |	Stationary | 5044 | 6 | 13 | TRUE |	7 |	-1 | 35 | 34 | 119 | 85 | -11 |	-114.0956573 | 51.052517 | 4326 | {'x': -114.0956573486328, 'y': 51.0525169997598, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.0956573486328, 'y': 51.0525169997598, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |	9/30/2021 15:45:58 | 9/30/2021 15:46:13 | 15 | 15:51:00 | 15:51:00 | N/A | {'paths': [[[-114.09614199999999, 51.05251700000008], [-114.09559399999995, 51.05251700000008]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 57436553 | 7 | 8 | Stationary | Movement | 5044 |	6 | 13 | TRUE |	7 | 0 | 35 | 34 | 119 |	85 | 0 | -114.0956573 |	51.052517 |	4326 | {'x': -114.0956573486328, 'y': 51.0525169997598, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.09544380239085, 'y': 51.05251606499545, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 15:46:13 |	9/30/2021 15:46:58 | 45 | 15:51:00 | 15:51:00 | 14.97 | {'paths': [[[-114.09614199999999, 51.05251700000008], [-114.09559399999995, 51.05251700000008]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... | .... |
| 57436553 | 40 | 41 | Terminus | Terminus | 3392 | 13 | 13 | TRUE | 0 | 0 | 119 | 118 | 119 | 1 | 0 | -114.0653509 | 51.04749952 |	4326 | {'x': -114.0653508695647, 'y': 51.04749951936606, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | {'x': -114.0653508695647, 'y': 51.04749951936606, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | 9/30/2021 16:02:42 | 9/30/2021 16:03:12 | 30 | 16:06:00 | 16:06:00 |	N/A | {'paths': [[[-114.06534899999997, 51.04753000000005], [-114.06537899999995, 51.047041000000036]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |
| 57436553 | 41 | 42 | Terminus | N/A | 3392 | 13 | 13 | TRUE | 0 | 0 | 119 | 118 | 119	| 1 | 0	| -114.0653509 | 51.04749952 | 4326 | {'x': -114.0653508695647, 'y': 51.04749951936606, 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} | N/A | 9/30/2021 16:03:12 | N/A | N/A | 16:06:00 |	16:06:00 |	N/A | {'paths': [[[-114.06534899999997, 51.04753000000005], [-114.06537899999995, 51.047041000000036]]], 'spatialReference': {'wkid': 4326, 'latestWkid': 4326}} |



### F) Packages Used & Purpose 
| Package | Purpose | 
| :-----: | ----- | 
| ***Pandas (indirect)***  | Data Engineering operations including apply, assign, query, groupby, drop_duplicates, and diff for dataframes. |
| ***..util*** | Execute ***TimeDelta*** (***NumPy*** package indirectly) and ***SpatialDelta*** (***ArcGIS API for Python*** indirectly) classes to compute temporal and spatial changes. These classes are in the ***deltas.py*** script. | 