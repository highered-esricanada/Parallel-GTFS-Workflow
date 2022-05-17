"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at the Education & Research Group at Esri Canada.

Date: Remodified started Q1 - 2022; Completed Q2 - 2022.

About: The main GTFS-RT workflow process in parallel with extensive spatial and data engineering operations. The workflow
        process are as followed:

        Iterate through each raw GTFS-RT file that has not been processed:
            1) Create folder structure based on the GTFS-RT date within the GTFS main folder date, if it does not exist.

            2) Within the self._spatial_and_dataeng_ops function that is run in parallel:
                a) Perform spatial operations - identify precisely all vehicle locations collected over time.
                b) Perform Qa/Qc of the data - remove any unnecessary / junk data.
                c) Enrich the dataset with additional attributes
                d) Perform Interpolation

            3) Run in parallel: Refine & clean up any junk / unnecessary data in the interpolated files that has not
               been flagged earlier.

            4) Run in parallel: The data aggregation processes.

More workflow details can be found in the Github page: https://github.com/highered-esricanada/Parallel-GTFS-Workflow,
or in the Python scripts via annotations or published manuscript (output & workflow will be different, but the
fundamental concepts are the same).
"""

from . import Ingestion, Maingeo, QaQc, RteEnricher, SpaceTimeInterp, RefineInterp, AggResults
from ..util import ParallelPool, AutoMake
from functools import partial 
from multiprocessing import Manager
import os
import re


class ExecuteProcess:

    def __init__(self, csv_inf, start_method, wkid):
        """
        :params csv_inf: DataFrame that contains information of each raw GTFS-RT csv file to be processed.
        :params start_method: The method to initiate - typically in Linux -> "fork" (unless ArcPy use "spawn"); Windows -> "spawn".
        :params wkid: The spatial reference to project. 
        """

        print('Reading relevant files per transit route, generating polyline in memory, and subsetting in parallel.')
        self._iterate_raw_gtfsrt(csv_inf=csv_inf, 
                                 start_method=start_method, 
                                 wkid=wkid)


    def _identify_vehicle_loc(self, folder_date, output_folder, raw_date, indiv_rte, wkid, unique_val, L):
        """
        Calls out the Maingeo class from geoapi.py. This is the most time consuming operation as it performs nested
        apply functions of geoprocessing functions. This is to identify the locations of the vehicle along the transit route.
        
        :param folder_date: The date that belongs to the static GTFS update across the project directory
                            (e.g., 0_external/2021-11-17; 2-staging/2021-11-17).
        :param output_folder: The main folder where contents will be exported and stored.
        :param indiv_rte: Subsetted dataframe (per transit route) of the collected raw GTFS-RT data and file explorer merged.
        :param wkid: Spatial reference to project points and polylines.
        :param unique_val: A list containing unique transit routes.
        :param L: The list that is part of the Manager in Multiprocessing.

        :returns: Tuple (0 = Final DataFrame; 1 = undissolved dataframe; 2 = transit stop file)

                  The final DataFrame contains the following schema:
                    stop_id    -> Identifier of the transit stop.
                    stop_seque -> The sequence number (order) associated to the stop_id.
                    index      -> The index value of the undissolved segment in which the veh. is on.
                    objectid   -> ObjectID number of the undissolved segment.
                    SHAPE      -> ArcGIS Geometry - Polyline SHAPE of the identified undissolved segment.
                    barcode    -> Original index to track transit recordings per trip_id.
                    Local_Time -> The recorded time of the vehicle from GTFS-RT.
                    trip_id    -> The ID related to the transit route with its own schedule.
                    point      -> ArcGIS Geometry - Point snapped of the veh's location to the nearest undissolved seg.

                 Otherwise, return None indicating a spatial/data integrity issue <- PREVENTION TO PROCEED DOWNSTREAM.
        """

        spatial_df = Maingeo(folder_date=folder_date,
                             output_folder=output_folder,
                             raw_date=raw_date,
                             indiv_rte=indiv_rte, 
                             wkid=wkid, 
                             unique_val=unique_val,
                             L=L)

        return spatial_df.finalgeo


    def _quality_check(self, geo_df, unique_val, raw_date, folder_date, output_folder, L2):
        """
        Removes "hazardous" observations (if applicable) that can tarnish calculations of transit metrics downstream. 
        Outputs cleaner dataframe (in memory & in storage through 3_interim folder) and reports how much data has been retained after cleaning. 
        
        :param geo_df: The spatial dataframe assessed for quality control.
        :param unique_val: A list containing unique transit routes.
        :param raw_date: The date of the collected raw GTFS-RT data.
        :param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-11-17).
        :param output_folder: Contents to be exported & stored in the output folder.
        :param L2: The list that is part of the Manager in Multiprocessing - report retention.

        :returns: Cleaner dataframe. 
        """

        clean_df = QaQc(df=geo_df, 
                        unique_val=unique_val,
                        L2=L2,
                        raw_date=raw_date,
                        folder_date=folder_date,
                        output_folder=output_folder)

        return clean_df.clean_df


    def _enrich_details(self, clean_df, undiss_df, stp_df, stop_times, folder_date, output_folder, raw_date, unique_val, L3):
        """
        Enriches the cleaner version of the individual transit route with additional attributes - mainly estimate 
        vehicle movement type (stationary, movement, terminus) and set check points 
        (validates quality of static GTFS via maximum stop sequence.)
        
        :param clean_df: Dataframe of the cleaner version (from QaQc) of the GTFS-RT per transit route.
        :param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
        :param stp_df: The stop csv file as a dataframe of the transit route.
        :param stop_times: The schedule (from GTFS static) per stop_id per trip_id (vehicle's id associated to transit rte.)
        :param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-09-30).
        :param output_folder: Contents exported & stored in the output folder.
        :param raw_date: The date of the collected raw GTFS-RT.
        :param unique_val: The unique-rte currently inspecting.
        :param L3: The list that is part of the Manager in Multiprocessing - report retention.

        :return: An enriched dataframe based on the following schema:
                    trip_id        -> The ID related to the transit route with its own schedule.
                    idx            -> Cumulate the number of vehicle movement (aka - recordings; not original after Qa/Qc) per trip_id.
                    barcode        -> Original number of vehicle movements/recordings. If it seems out-of-place comparing to idx,
                                      then an observation(s) have been omitted due to QA/QC issues.
                    Status         -> Pre-determine the vehicle's mobility status (in future requires distance travelled).
                    stat_shift     -> Shift back the Status field by 1 per grouped trip_id. This is to identify delta mobility/travel.
                    stop_id        -> Identifier of the transit stop of the transit route provided by static GTFS files.
                    stop_seque     -> Tied to stop_id, the sequence number of the transit stop of the transit route.
                    MaxStpSeq      -> Maximum stop sequence number of the transit route.
                    true_max_stp   -> Validates whether the MaxStpSeq value is true with the undissolved shapefile.
                                      If true, then the last stop is the true terminus. If false, then take the last
                                      stop and set it as the terminus (though this is false).
                    Stp_Left       -> Calculates how many stops the current vehicle's location of the trip_id has left
                                      along its transit route. (Stp_Left = MaxStpSeq - stop_seque).
                    Stp_Diff       -> Gets the difference of the Stp_Left (Stp_Diff = Stp_Left.diff(1)).
                    objectid       -> Typically one value above the index field. Not much value in this field.
                    index          -> The value of the undissolved segment where the vehicle is accurately located along
                                      the transit route.
                    MaxIndex       -> Similar to MaxStpSeq, identifies the maximum index value of the undissolved
                                      segments of the transit route.
                    Idx_Left       -> Similar to Stp_Left, it finds how many indices (i.e., segments) the current
                                      vehicle's locations of the trip_id has left along its transit route.
                                      (Idx_Left = MaxIndex - index)
                    Idx_Diff       -> Similar to Stp_Diff, gets the difference of the index remaining value (Idx_Left)
                                      from the previous observation to the current one (Idx_Diff = Idx_Diff.diff(1)).
                    x              -> The snapped x-coordinate (longitude) of the vehicle's location.
                    y              -> The snapped y-coordinate (latitude) of the vehicle's location.
                    wkid           -> The spatial reference of the snapped point.
                    point          -> ArcGIS Point Geometry (as a string to avoid conflict with SHAPE) of the vehicle's
                                      snapped location.
                    pnt_shift      -> Shift back point field by 1 per grouped trip_id as a precursor to calculate
                                      delta distance, if applicable to the Status-stat_shift criteria (conditional).
                    Local_Time     -> From the GTFS-RT, the timestamp of the vehicle's location recorded.
                    time_shift     -> Shift back by 1 of Local_Time per grouped trip_id as a precursor to calculate
                                      delta time, if applicable to the Status-stat_shift criteria (conditional).
                    delta_time     -> Calculate the delta time (in seconds) between the consecutive recorded pair of
                                      the vehicle (delta_time = time_shift - Local_Time).
                    arrival_time   -> From the stop time static GTFS file, provides the scheduled expected arrival time
                                      of the stop_id.
                    departure_time -> From the stop time static GTFS file, provides the scheduled expected departure time
                                      of the stop_id.
                    delta_dist     -> If applicable to the Status-stat_shift criteria, estimates the distance covered
                                      over time. This will accurately identify if the vehicle is considered to be
                                      stationary (i.e., idle, stuck, or too slow to move forward to the next recording).
                                      (delta_dist = polyline(point, pnt_shift).get_length).
                    SHAPE          -> ArcGIS Polyline Geometry shape of the undissolved segment.
        """

        enrich_df = RteEnricher(clean_df=clean_df, 
                                undiss_df=undiss_df, 
                                stp_df=stp_df, 
                                stop_times=stop_times,
                                folder_date=folder_date,
                                output_folder=output_folder,
                                raw_date=raw_date,
                                unique_val=unique_val, 
                                L3=L3)

        return enrich_df.enrich_df


    def _spacetime_interpolation(self, enrich_df, undiss_df, stop_times, wkid, folder_date, output_folder, raw_date, unique_val, L4):
        """
        Performs spatio-temporal interpolation between consecutive pair (1st and 2nd recording at a time).
        Estimates projected travel speed & travel time to arrive stop_id destination, determines if it is on-time, late,
        or early.

        :param enrich_df: The enriched dataframe (see _enrich_details for schema).
        :param undiss_df: The undissolved shapefile read as a spatial dataframe of the transit route.
        :param stop_times: The schedule (from GTFS static) per stop_id per trip_id (vehicle's id associated to transit rte.).
        :param wkid: Spatial reference to project polylines.
        :param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-09-30).
        :param raw_date: The date of the collected raw GTFS-RT.
        :param unique_val: The unique-rte currently inspecting.
        :param L4: The list that is part of the Manager in Multiprocessing - report error rate & error type (if applicable).

        :return: Interpolated dataframe that has been augmented or None (if error arises).
                 If None <- PREVENT DOWNSTREAM
                 If interpolated dataframe (PROCEED DOWNSTREAM), it would consist the following schema:
                    trip_id    -> Identifier of the transit route.
                    idx        -> The cumulative number of vehicle movements - grouped per consecutive pair.
                    stop_id    -> Identifier of the transit stop.
                    stop_seque -> The sequence number (order) associated to the stop_id.
                    status     -> Travel status of the vehicle.
                    proj_speed -> Projected travel speed (km/h.) from time and distance delta between consecutive pair.
                    x          -> The snapped x-coordinate of where the vehicle was located.
                    y          -> The snapped y-coordinate of where the vehicle was located.
                    Tot_Dist   -> The total distance (m) travelled from the 1st to the 2nd veh. consecutive pair.
                    dist       -> Distance traveled on each stop sequence segment - the last observation in the group
                                  calculates of what has past (reverse).
                    dist_futr  -> The distance required for the last observation in the group needed to arrive from its
                                  current stop_sequence path (forward).
                    futr_trvel -> The amount of travel time (sec.) projected to complete the future distance - last
                                  observation in the group.
                    proj_trvel -> The amount of travel time (sec.) projected to complete from 1st veh to 2nd veh
                                  (last observation) in the group - from dist and proj_speed.
                    curr_time  -> The recorded timestamp from the 1st veh. and 2nd veh. (last observation) in the group.
                    est_arr    -> The estimated arrival time based on proj_trvel and curr_time - cumulative,
                                  except the last observation.
                    off_earr   -> Official estimated arrival time for all observations including the last observation (future).
                    tmp_arr    -> The scheduled/expected arrival time reformatted - excludes the last observation in the group.
                    sched_arr  -> Official scheduled/expected arrival time reformatted - includes the last observation
                                  in the group.
                    arr_tmedif -> Arrival time difference calculated from estimated arrival time and
                                  scheduled/expected arrival time - excludes last observation.
                    off_arrdif -> Official time difference calculated from estimated arrival time and
                                  scheduled/expected arrival time - includes last observation (forward).
                    perc_chge  -> Percent change in official time difference - estimates how much of a change there has
                                  been in travel over time - can be used in calculus.
                    perf_rate  -> Classification of on-time performance:
                                  Late (<= -120 sec.); On-Time (120 < x < 300); Early (>= 300).
                    dept_time  -> The scheduled/expected departure time (not reformatted).
                    end_path   -> The linestring paths (nested coordinates) that can be drawn out spatially if required.

        """

        interpolated_df = SpaceTimeInterp(enrich_df=enrich_df,
                                          undiss_df=undiss_df,
                                          stop_times=stop_times,
                                          wkid=wkid,
                                          folder_date=folder_date,
                                          output_folder=output_folder,
                                          raw_date=raw_date,
                                          unique_val=unique_val,
                                          L4=L4)

        return interpolated_df.enhanced_df


    def _spatial_and_dataeng_ops(self, unique_rte_values, multiLs, list_folders, suppl_rt_df, stop_times, folder_date, raw_date, wkid):
        """
        The main spatial and data engineering operations that process behind-the-scenes raw GTFS-RT 
        into transit metrics. Each self function below calls out a class to perform a specific 
        operation - it refers back to the workflow chart (see academic paper - Fig. 3). 

        :param unique_rte_values: A list containing unique rtes.
        :param multiLs: A nested list of list that is part of the Manager in Multiprocessing.
                        0 = Error logging placed in the Maingeo
                            (Schema: raw_date, folder_date, unique_val, error type).
                        1 = % of the dataframe retained in the QaQc - data loss greater than 30% is considered volatile.
                            (Schema: unique_val, raw_date, folder_date, retained).
                        2 = % of the QaQc dataframe that has been retained in the RteEnricher process - >30% considered volatile.
                            (Schema: unique_val, raw_date, folder_date, retained).
                        3 = % of the enriched data lost (error rate) during spatiotemporal interpolation process, error logging included.
                            (Schema: unique_val, raw_date, folder_date, error_rate)
        :params list_folders: List of folders to export contents to their dedicated folder.
        :params suppl_rt_df: DataFrame of the collected raw GTFS-RT data and file explorer merged.
        :params stop_times: DataFrame of stop_times.txt.
        :params folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-11-17; 2_staging/2021-11-17)
        :params raw_date: The date of the collected raw GTFS-RT data. 
        :params wkid: The spatial reference to project. 
        """

        # Go through each transit route in parallel.
        indiv_rte = suppl_rt_df.query('UniqueRte == @unique_rte_values') # Comment out during testing

        # Identify vehicle locations along the route - used to derive transit metrics precisely. 
        # In the workflow (Fig. 3 in the academic paper) - covers steps 2 (Geoprocess Vehicle Locations) and
        # 3 (Extract Geographic Information)
        # Comment out during testing
        spatial_df = self._identify_vehicle_loc(folder_date=folder_date,
                                                output_folder=list_folders[1],
                                                raw_date=raw_date,
                                                indiv_rte=indiv_rte,
                                                wkid=wkid,
                                                unique_val=unique_rte_values,
                                                L=multiLs[0])


        if spatial_df is not None:
            # Proceed to the rest of the workflow
            clean_df = self._quality_check(geo_df=spatial_df[0],
                                           unique_val=unique_rte_values,
                                           raw_date=raw_date,
                                           folder_date=folder_date,
                                           output_folder=list_folders[1],
                                           L2=multiLs[1])

            enrich_df = self._enrich_details(clean_df=clean_df,
                                             undiss_df=spatial_df[1],
                                             stp_df=spatial_df[2],
                                             stop_times=stop_times,
                                             folder_date=folder_date,
                                             output_folder=list_folders[2],
                                             raw_date=raw_date,
                                             unique_val=unique_rte_values,
                                             L3=multiLs[2])

            interp_df = self._spacetime_interpolation(enrich_df=enrich_df,
                                                      undiss_df=spatial_df[1],
                                                      stop_times=stop_times,
                                                      wkid=wkid,
                                                      folder_date=folder_date,
                                                      output_folder=list_folders[3],
                                                      raw_date=raw_date,
                                                      unique_val=unique_rte_values,
                                                      L4=multiLs[3])

        else:
            pass

        
    def _iterate_raw_gtfsrt(self, csv_inf, start_method, wkid):
        """
        Iterate through each unprocessed GTFS-RT file identified and perform spatial and data engineering operations in parallel 
        downstream to generate transit metrics as the final output. The self._spatial_and_dataeng_ops is the main function 
        to refer to the workflow (Fig. 3 in the academic paper).
    
        :params csv_inf: DataFrame that contains information of each raw GTFS-RT csv file to be processed.
        :params start_method: The method to initiate - typically in Linux -> "fork" (unless ArcPy use "spawn"); Windows -> "spawn".
        :params wkid: The spatial reference to project. 
        """

        # Iterate through each raw GTFS-RT for processing.
        for c in range(len(csv_inf)):
            use_files = Ingestion(individual_csv_df=csv_inf.iloc[c]).exp_df
            disc_docs  = use_files[0] # The discover docs

            # If discover docs is not empty, proceed major process.
            if len(disc_docs) > 0:
                #disc_docs.to_csv('../data/2_staging/fileexp.csv', index=False)
                folder_date = csv_inf.iloc[c].folder_date
                raw_date    = csv_inf.iloc[c].raw_date

                print(f"Parallel Processing GTFS-RT for {raw_date} at GTFS update {folder_date}.")

                rt_df       = use_files[1]
                #rt_df.to_csv('../data/2_staging/test.csv', index = False)
                trips       = use_files[2]
                shapes      = use_files[3]
                routes      = use_files[4]
                stops       = use_files[5]
                stop_times  = use_files[6]

                # Set up folders & create them if they don't exist (e.g., "../data/2_staging/2021-09-30/2021-10-01")
                staging_folder = f"../data/2_staging/{folder_date}/{raw_date}"
                interim_folder = f"../data/3_interim/{folder_date}/{raw_date}"
                processed_folder = f"../data/4_processed/{folder_date}/{raw_date}"
                conformed_folder = f"../data/5_conformed/{folder_date}/{raw_date}"
                analyses_folder = f"../data/6_analyses/{folder_date}/{raw_date}"
                requests_folder = f"../data/7_requests/{folder_date}/{raw_date}"

                AutoMake(storage_folder=staging_folder)
                AutoMake(storage_folder=interim_folder)
                AutoMake(storage_folder=processed_folder)
                AutoMake(storage_folder=conformed_folder)
                AutoMake(storage_folder=analyses_folder)
                AutoMake(storage_folder=requests_folder)

                list_folders = [staging_folder, interim_folder, processed_folder, conformed_folder]

                # Create an error file 
                geo_log   = open(f"{staging_folder}/errors.log", "a")
                inter_log = open(f"{interim_folder}/retention.txt", "a")
                enrch_log = open(f"{conformed_folder}/retention.txt", "a")
                intrp_log = open(f"{conformed_folder}/error_rate.txt", "a")

                # Create a list that will have to be stored while in parallel and then be acquired afterwards.
                multiLs = [Manager().list() for l in range(0,4)]

                # Merge the file explorer with the raw GTFS-RT csv file. 
                suppl_rt_df = (
                    disc_docs
                        [['route_id', 'trip_id', 'Undiss_Rte', 'Diss_Rte', 'Stop', 'UniqueRte', 
                          'Alt_Undiss_Rte', 'Alt_Diss_Rte', 'Alt_Stop']]
                        .drop_duplicates()
                        .merge(rt_df, left_on='trip_id', right_on='Trip_ID', how='inner', validate='1:m')
                )

                #print(disc_docs.columns)
                #print(suppl_rt_df.columns)

                unique_rtes = suppl_rt_df.UniqueRte.unique() # originally disc_docs

                ########################################################################################################
                ####################### MEGA WORKFLOW PROCESS CONDUCTED IN PARALLEL & IN-MEMORY ########################
                ####################### Starts: _identify_vehicle_loc                           ########################
                ####################### Ends: _spacetime_interpolation                          ########################
                ####################### Main function: _spatial_and_dataeng_ops                 ########################
                ########################################################################################################
                mult_argu = partial(self._spatial_and_dataeng_ops, 
                                    multiLs=multiLs,
                                    list_folders=list_folders,
                                    suppl_rt_df=suppl_rt_df,
                                    stop_times=stop_times,
                                    folder_date=folder_date, 
                                    raw_date=raw_date,
                                    wkid=wkid)

                ParallelPool(start_method=start_method, 
                             partial_func=mult_argu, 
                             main_list=unique_rtes)


                ########################################################################################################
                ####### Next Parallel Process - Refining Interpolated Results & Perform AND Export Aggregations ########
                ########################################################################################################
                other_multiLs = [Manager().list() for l in range(0,4)]

                print("2nd Parallel Processing - refining interpolated results.")
                RefineInterp(start_method=start_method,
                             L=other_multiLs[0],
                             path=conformed_folder,
                             trips_txt=trips)

                # Output will be directed to 6_analysis and 7_requests (geojson version).
                print("3rd Parallel Processing - aggregating results.")
                shp_path = f"../data/2_staging/{folder_date}/Routes"
                AggResults(start_method=start_method,
                           path=conformed_folder,
                           shp_path=shp_path,
                           analyses_folder=analyses_folder,
                           requests_folder=requests_folder,
                           L=other_multiLs[1],
                           L2=other_multiLs[2],
                           L3=other_multiLs[3])

                # Export error logs to their destined locations
                self._extract_list_manager(L=multiLs[0], log_file=geo_log)
                self._extract_list_manager(L=multiLs[1], log_file=inter_log)
                self._extract_list_manager(L=multiLs[2], log_file=enrch_log)
                self._extract_list_manager(L=multiLs[3], log_file=intrp_log)
                self._extract_list_manager(L=other_multiLs[0], log_file=intrp_log)

                # Rename the GTFS-RT csv file that it has been completed.
                csv_file = csv_inf['path'].iloc[c]
                csv_dir  = csv_inf['directory'].iloc[c]
                filename = re.sub(r"^.*/([^/]*)$", r"\1", csv_file)  # UNIX folder structures
                filename = re.sub(r"^.*\\([^\\]*)$", r"\1", filename)  # Windows folder structures
                os.rename(csv_file, f"{csv_dir}/Complete_{filename}")

            else:
                print('fail')
                pass


    def _extract_list_manager(self, L, log_file):
        """
        After parallel processing is complete, write out errors identified in a log file to be viewed later. 

        :params L: The list that is part of the Manager in Multiprocessing. It stores errors while parallel processed. 
        :params log_file: The log file that stores written errors. 
        """

        [log_file.write(f"{l}\n") for l in L]
