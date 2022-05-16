## Refine Interpolated Files 

### A) Purpose
This final component of the workflow finalizes the interpolated (cleaned version) by aggregating based on the following operations: 

<ol> 
  <li>Get sum of observations (recorded & interpolated) per on-time performance values (Early, On-Time, Late) by grouping <strong>route_id</strong>, <strong>trip_id</strong>, <strong>stop_seque</strong>, and <strong>sched_arr</strong>.</li>
  <br>
  <li>Using the interpolated (cleaned) csv file and aggregation from the <strong>first bullet point</strong> to do a general aggregation by grouping <strong>route_id</strong>, <strong>trip_id</strong>, <strong>stop_seque</strong>, <strong>stop_id</strong>, and <strong>sched_arr</strong>.</li>
  <br>
  <li>Use the outcome from the <strong>second bullet point</strong> to aggregate into an <strong>hourly</strong> basis by grouping <strong>route_id></strong>, <strong>stop_seque</strong>, and <strong>ref_hr</strong>.</li>
  <br>
  <li>Use the outcome from the <strong>third bullet point</strong> to aggregate into an <strong>overall daily</strong> basis by grouping <strong>route_id</strong> and <strong>stop_seque</strong>.</li>
 </ol>

<br> 
From the 2nd to the 4th bullet point, these outcomes are concatenated and exported into three shapefiles. The first stored in the <strong>6_analyses</strong> folder and the latter two outcomes in the <strong>7_requests</strong> folder. Schema details with table samples are provided further in this document. 


### B) Function Details - Need to update 
The <strong><a href=''>aggregation.py</a></strong> script consists of 4 data engineering functions bringing a total of 144 lines of code. Except the <strong>__init__</strong> function, all other functions are described in the table below. Terminology, such as <strong>degree order</strong> and <strong>trending order</strong> are described after the table. 


| Name of Function | Lines | Purpose | 
| :---: | ----- | ----- | 
| ***_filt_df*** | 35-48 | Filters out unwanted observations that may distort aggregate calculations.   |
| ***_clean_df*** | 51-108 | Cleaning process - removes unwanted observations including illogical observations that have very high speed and estimated extreme arrival times (> 20 min. as the threshold). |
| ***_mainprocess*** | 111-144 | The main process to clean, filter, and concat final output per interpolated csv file. This entire process is done in conventional parallel processing. | 


### C) Required Parameters (Indirect) - Need to update

The user is not required to insert the parameters for the prep_agg_parallel.py script. Rather, the backend processes from transform.py inserts it programmatically as part of the downstream workflow. 

| Parameter | Type | Purpose | 
| :-------: | :---: | ------ | 
| ***start_method*** | Str | "spawn" (Windows or use of ArcPy) or "fork" (Linux without use of ArcPy) to spin up parallel processing.  | 
| ***L*** | List | Part of the Manager function in Multiprocessing, it is a list distributed across all CPU cores and stores any errors during cleaning process. After parallel processing, the errors are written in a text file as comma delimited. | 
| ***trips_txt*** | DataFrame | DataFrame of the trips.txt from the static GTFS files. | 

 
### D) Outcome Schema & Table Samples 
Below are outcome schema and their respective table samples. 

<strong>Table 1A:</strong> General Aggregation 
<br>

| Field | Description | 
| :-----: | :-------: | 
| route_id | The transit route. | 
| trip_id  | The ID related to the transit route with its own schedule. |
| stop_seque | Tied to stop_id, the sequence number of the transit stop of the transit route. |
| stop_id | Identifier of the transit stop of the transit route provided by static GTFS files. |
| sched_arr | The expected scheduled arrival of that transit stop of the route. | 
| off_earr | The last projected observation (before transitioning to the next stop) of arrival time of he vehicle (trip_id). | 
| Lprfrte | The last projected on-time performance observation (before transitioning to next stop). |
| ref_hr | Reference hour extracted from sched_arr. |
| AvgSpd | The overall average projected speed (km/h). |
| Avg_ArrDif | The overall average arrival time difference (km/h). | 
| idx | The cumulative number of vehicle movements - there will be gaps (e.g. 1 -> 3 -> 7). The gaps indicate that there were multiple recorded (and interpolated) observations that happened in-between and were filtered out after aggregation. |
| TotalObs | Not entirely related to the difference of idx between stops; however, this provides the total observations that occurred for that trip_id at that stop_seque. For instance, trip_id X at stop_seque Y had 4 occurrences. This factors in the calculations for the columns <strong>Late</strong> till <strong>PrcObsUns</strong>. Keep in mind this does not directly measure probability of on-time performance, but rather the stability in status updates. |
| Late | The number of observations per route_id, trip_id, and stop_seque that have been projected to arrive Late. | 
| On-Time | The number of observations per route_id, trip_id, and stop_seque that have been projected to arrive On-Time. |
| Early | The number of observations per route_id, trip_id, and stop_seque that have been projected to arrive Early. |
| Satis | Short for satisfactory, factoring only the total observations that fall in the on-time field. |
| Unsatis | Short for unsatisfactory, combining late and early total observations. |
| PrcObsSat | Short for percentage of all observations projected to have an on-time performance of satisfactory. Signifies stability of status change. Higher indicates more stable status change and higher probability to be on-time. |
| PrcObsUns | Short for percentage of all observations projected to be early and/or late. Higher indicates less stable status change and higher probability to be late or early. | 
| spdList | Nested list of all observations (from first to last observation) of that trip_id and stop_seque of projected speed. Often won't match with TotalObs due to duplicates. |
| arrdifList | Nested list of all observations (from first to last observation) of that trip_id and stop_seque of arrival time difference. Often won't match with TotalObs due to duplicates. |
| x | The last observed (recorded or interpolated) x (lon) coordinate of the vehicle. |
| y | The last observed (recorded or interpolated) y (lat) coordinate of the vehicle. |

<br>
<strong>Table 1B:</strong>Sample Table of General Aggregation

| route_id | trip_id | stop_seque | stop_id | sched_arr | off_earr | Lprfrte | ref_hr | AvgSpd | Avg_ArrDif | idx | TotalObs | 
| :------: | :------: | :------: | :------: | :------: | :------: | :------: | :------: | :------: | :------: | :------: | :------:

### E) Packages Used & Purpose - Need to update 
| Package | Purpose | 
| :-----: | ----- | 
| ***Pandas (indirect)***  | Data Engineering operations including apply, assign, query, groupby, drop_duplicates, and diff for dataframes. |
