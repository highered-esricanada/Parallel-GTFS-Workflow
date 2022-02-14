## Quality Assurance & Quality Control (QA/QC)

### A) Purpose
This component of the workflow checks the quality of the data after geoprocessing and removes out-of-place observations. In GIS, when there is an overlapping polyline (a route looping around a neighbourhood or cul-de-sac and returning but opposite direction) and a point is within it, the output would show at least 2 polylines. The question is which observation of the two to keep? This can be answered by inspecting the rest of the observations and identify their trends via value of the polyline's index. It is important to perform this component to prevent inaccurate calculations of transit metrics downstream. The output is a cleaner spatial dataframe (if applicable) and a text delimited file reporting percentage of data observations retained after cleaning. 


### B) Function Details 
The <strong><a href=''>qaqc.py</a></strong> script consists of 3 data engineering functions bringing a total of 97 lines of code. Except the <strong>__init__</strong> function, all other functions are described in the table below. Terminology, such as <strong>degree order</strong> and <strong>trending order</strong> are described after the table. 


| Name of Function | Lines | Purpose | 
| :---: | ----- | ----- | 
| ***_check_diffs*** | 31-49 | Checks the difference per observation by degree order and omits row(s) that have a negative difference, thus indicating out-of-place values of the trending order. |
| ***_improve_data*** | 52-97 | Main function that checks the data quality by omitting out-of-place observations per grouped trip_id. Calls out the ***_check_diffs*** function and reports retention rate. |


<strong>Degree order</strong>: The number of rows going back from the current row being assessed for any difference. For instance, if the degree order is 3, then the current row will check the difference with another observation 3 rows prior. The function starts off with degree order of 3, and then checks by 2, and finalizes by 1. 

<strong>Trending order</strong>: The expected ascending order trend of the values, specifically stop sequence and the index values of the undissolved segment. Below is a sample graphic demonstrating which observation would be out-of-placed. 
<br>
<br>
<p align='center'><img src="../../../documentation/trending_order.JPG"/></p>
<br>
<strong>Retention Rate</strong>: The percentage of observations retained after QA/QC by taking the number of observations in the final dataset divided by the number of observations in the original dataset. 

### C) Required Parameters (Indirect)

The user is not required to insert the parameters for the qaqc.py script. Rather, the backend processes from transform.py inserts it programmatically as part of the downstream workflow. 

| Parameter | Type | Purpose | 
| :-------: | :---: | ------ | 
| ***df*** | DataFrame | The spatial dataframe (in-memory) containing stop_id, stop_sequence, index, objectid, SHAPE of Polyline (undissolved segment), barcode, Local_Time, trip_id, point of the snapped vehicle location, x coordinate, y coordinate, and wkid. | 
| ***unique_val*** | Str | The unique transit route currently being processed (e.g., "4-40066-Outbound"). |
| ***L2*** | List | Part of the Manager function in Multiprocessing, it is a list distributed across all CPU cores and stores the following variables: unique_val, raw_date (date of the GTFS-RT), folder_date, and retained (retention rate). After parallel processing, the variables are written in a text file as comma delimited. | 
| ***raw_date*** | Str | The date of the raw GTFS-RT. | 
| ***folder_date*** | Str | The date that belongs to the static GTFS update across the project directory used to process the raw GTFS-RT. An example is "2021-09-30" from "../data/0_external/2021-09-30". |

 
### D) Step Details 
Below are the backend steps (in order) briefly explained followed by a graphic that encapsulates it. 
<ol>
	<li>Execute <strong>self._improve_data</strong> (lines 52-97), drops unnecessary duplicate values (lines 67-73). These are trip_ids that have been recorded originally, but have not changed in the next recording. Not all GPS devices are equally synchronized.
		<ul>
			<li>Omit negative values by group by trip_id and apply <strong>self._check_diffs</strong> with defined degree order. Repeat this two more times (degree order - 2 and 1).</li>
		</ul>	
	</li>
	<br>
	<li>In the <strong>self._check_diffs</strong> (lines 31-49) function, assign the difference by stop sequence and index value fields and keep values that are NA or greater than or equal to zero. Drop the created fields to maintain current dataframe.
	</li>
	<br>
	<li>Get the retention value (lines 86-89), append retention information (lines 91), export the cleaner dataframe to csv file (lines 93-95), and return cleaner dataframe for further downstream processing (lines 97).
</ol>
<br>
<p align='center'><img src='../../../documentation/qaqc_flow.JPG' width="600"/></p>
<br>

### E) Packages Used & Purpose 
| Package | Purpose | 
| :-----: | ----- | 
| ***Pandas (indirect)***  | Data Engineering operations including apply, assign, query, groupby, drop_duplicates, and diff for dataframes. |