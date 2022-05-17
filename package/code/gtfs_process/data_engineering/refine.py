"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada. 
Date: Remodified Q4 - 2021 

About: Checks for any updates with GTFS static files and if so, create new GTFS routes and transit stops. 
       These files (i.e., routes, transit stops) are going to be used to help calculate transit metrics 
       for the GTFS-RT csv file. 

Warning Note: This only works for a specific hyperlink - if you have a different hyperlink, you will 
              likely need to create your own module. If so, you may need to replace the CheckGTFS class. 

Requires: 
    1) BeautifulSoup 
    2) ArcGIS API for Python
    3) ArcPy 
"""

import re 
import shutil
import zipfile
import os.path

from ..util import discover_docs
from ..util import ParallelProcess
from ..util import CalcTime

from multiprocessing import cpu_count
from requests import get
from bs4 import BeautifulSoup
from pandas import DataFrame, read_table, read_csv 
from numpy import where, array_split 
from arcgis.features import GeoAccessor, Feature, FeatureSet
from arcgis.geometry import Polyline, Point
from arcpy.management import Dissolve
from time import strptime
from tqdm import tqdm 


class GenShpGTFS:

    def __init__(self, folder_date, start_method, wkid):
        """
        Create transit routes (undissolved and dissolved) and stops as Shapefiles stored in the 2_staging folder via Routes and Stops respectively. 

        :params folder_date: The date that the GTFS static file & RT is being assessed.
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn).
        :params wkid: The spatial reference. 
        """

        ##############################################################################################################################################################
        ################################ List csv files and then create undissolved transit routes and transit stops as shapefiles. ##################################
        ##############################################################################################################################################################
        self.file_dfs = self._list_csvs(folder_date=folder_date)
        print('2-a. Parallel Processing - Creating Undissolved Route (Polyline) and Transit Stops (Point) Shapefiles.')
        start_time = CalcTime().starter()
        self._executeGIS(file_df=self.file_dfs[0], 
                         start_method=start_method, 
                         target_function=self._mainGIS, 
                         params=(self.file_dfs[0], self.file_dfs[1], wkid))
        end_time = CalcTime().finisher(start_time=start_time)
        print(f"Parallel Processing (Created Undissolved shapefiles & transit stops) - Complete: {end_time[0]} hr., {end_time[1]} min. and {round(end_time[2])} sec.")

        ##############################################################################################################################################################
        ################################# List recently created shapefiles and then create dissolved transit routes  as shapefiles. ##################################
        ##############################################################################################################################################################
        self.list_shps = self._list_shps(folder_date=folder_date, wkid=wkid)
        print('2-b. Parallel Processing - Creating Dissolved Route (Polyline) Shapefiles.')
        start_time = CalcTime().starter()
        self._executeGIS(file_df=self.list_shps, 
                         start_method=start_method, 
                         target_function=self._secondaryGIS,
                         params=(self.list_shps,))
        end_time = CalcTime().finisher(start_time=start_time)
        print(f"Parallel Processing (Created Dissolved shapefiles for transit routes) - Complete: {end_time[0]} hr., {end_time[1]} min. and {round(end_time[2])} sec.")
        

    def _list_csvs(self, folder_date):
        """
        List csv files containing individual route and transit stop information that were created in the GenCsvGTFS class. 

        :params folder_date: The date folder that has been recently updated (e.g., 2021-11-27)

        :returns: Two dataframes stored in a tuple - route_inf => the csv files for the transit routes. 
                                                   - stop_inf  => the csv files for the transit stops. 
        """

        file_inf = (
            discover_docs(path=f"../data/2_staging/{folder_date}")
            [['path', 'filename', 'directory']]
            .assign(is_route = lambda r: (r['directory'].str.contains('(?i).*Routes.*')), 
                    is_stop  = lambda r: (r['directory'].str.contains('(?i).*Stops.*')))
                    # Add bound column (Inbound, Outbound)
        )

        route_inf = file_inf.query('is_route == True')
        stop_inf  = file_inf.query('is_stop == True')

        return (route_inf, stop_inf)


    def _prepdfs(self, rte_df, stp_df):
        """
        Restructure dataframe - identify up to which indices does each transit stop belong to to create undissolved and dissolved transit routes. 

        :params rte_df: The transit route dataframe. 
        :params stp_df: The transit stop dataframe. 

        :returns: rte_index -> The restructured dataframe; indices -> the indices to remove later on in the spatial dataframe.
        """

        stps = stp_df.Stop_ID
        stpq = stp_df.Stop_Sequence

        indices = (
            DataFrame(
                rte_df
                    .duplicated(subset=['shape_dist_traveled'], keep='first')
                    .to_frame('RteIndex')
                    .query('RteIndex == True')
                    .index
                    .tolist(),
                columns = ['RteIndex']
            )
            .assign(FinIndex = lambda r: r['RteIndex']-1)
        )


        # Get the rte index by dropping duplicates. 
        # Identify what stops belong to which indices. 
        rte_index = (
            indices
            .append({'FinIndex' : rte_df.index.stop}, ignore_index=True) # Add the last stop 
            .assign(Stop_ID       = stps.shift(-1),                       # Take the 2nd stop and onwards
                    Stop_Sequence = stpq.shift(-1),                       # Take the 2nd stop sequence and onwards
                    index         = lambda r: r['FinIndex'])              # Create copy index
            [['FinIndex', 'RteIndex', 'Stop_ID', 'Stop_Sequence', 'index']]
            .set_index('index')
            .pipe(lambda d: 
                DataFrame(range(0, int(max(d.index))+1), columns=['new_index'])
                    .merge(d, left_on=['new_index'], right_index=True, how='left')
                    .fillna(method='bfill')
            )
            .merge(rte_df.assign(Index=lambda r: r.index), left_on='new_index', right_on='Index', how='left')  
            .iloc[:-1]
            [['Stop_ID', 'Stop_Sequence', 'shape_id', 'shape_pt_lat', 'shape_pt_lon', 
              'shape_pt_sequence', 'shape_dist_traveled', 'RouteNum', 'RouteName', 
              'RouteType', 'Direction', 'end_lat', 'end_lon', 'Index']]
        )

        return (rte_index, indices)


    def _main_gis_helper(self, filename, path_route, directory_route, path_stop, directory_stop, wkid):
        """
        See _mainGIS for more details of what the processes entail. 

        :params filename: Field - the name of the file. 
        :params path_route: Field - the directory path to the route csv file. 
        :params directory_route: Field - main directory that contains the route csv file. 
        :params path_stop: Field - the directory path to the stop csv file. 
        :params directory_stop: Field - main directory that contains the stop csv file.
        :params wkid: The spatial reference to project transit stops. 
        """

        tmp_rte  = read_csv(path_route)  # Read individual route csv file. 
        tmp_stp  = read_csv(path_stop)   # Read individual stop csv file. 
        
        rte_shp_name = f"{directory_route}{filename[:-4]}_Routes.shp" # To name the Undissolved route shapefile -> Polyline. 
        stp_shp_name = f"{path_stop[:-4]}_Stops.shp"  # To name the Transit stop shapefile -> Point. 

        info = self._prepdfs(rte_df=tmp_rte, 
                             stp_df=tmp_stp)

        self._undissolve_rtes(info_df=info[0],
                              rmv_indices=info[1], 
                              rte_shp_name=rte_shp_name, 
                              wkid=wkid)

        self._transit_stops(tmp_stp=tmp_stp, 
                            stp_shp_name=stp_shp_name, 
                            wkid=wkid)  


    def _mainGIS(self, list_rtes, stp_df, wkid):
        """
        Main geoprocess: 
            1) self._prepdfs -> restructure dataframe to identify which indices does each transit stop belong to to create undissolved and dissolved transit routes. 
            2) self._undissolve_rtes -> using the output from self._prepdfs to create undissolved transit routes. 
            3) self._transit_stops -> create transit stops per route. 

        :params list_rtes: A list of routes in the direction bound folder to be processed. 
        :params stp_df: Dataframe containing csv files for transit stops.  
        :params wkid: The spatial reference to project transit stops. 
        """ 

        processing_set = (
            list_rtes
                .query("filename.str.lower().str.endswith('csv')", engine="python")
                [["filename", "path", "directory"]]
                .merge(stp_df, on=["filename"], how="inner", validate="1:m", suffixes=("_route", "_stop"))
                .assign(wkid = wkid)
                [["filename", "path_route", "directory_route", "path_stop", "directory_stop", "wkid"]]
        )

        tqdm.pandas()

        _ = processing_set.progress_apply(lambda r: self._main_gis_helper(*r), axis=1)


    def _undissolve_rtes(self, info_df, rmv_indices, rte_shp_name, wkid):
        """
        Create undissolved (i.e., individual line segment) transit route. 

        :params info_df: Dataframe containing coordinates to draw each individual line segment. 
        :params rmv_indices: The indices to remove from the spatial dataframe due to empty shape. 
        :params rte_shp_name: The output shapefile name of the transit route. 
        :params wkid: The spatial reference to project transit routes. 
        """

        feature_set = []
        for t in range(len(info_df)):
            path = [[
                [info_df.shape_pt_lon.iloc[t], info_df.shape_pt_lat.iloc[t]],
                [info_df.end_lon.iloc[t], info_df.end_lat.iloc[t]]
            ]]

            tmp_line = Polyline({'spatialReference' : {'latestWkid' : wkid}, 'paths' : path})
            tmp_feat = Feature(tmp_line, attributes=info_df.iloc[t].to_dict())
            feature_set.append(tmp_feat)

        fset = FeatureSet(features=feature_set, 
                          geometry_type="Polyline", 
                          spatial_reference={'latestWkid' : wkid, 'wkid' : wkid}).sdf

        # Keep specific columns, remove invalid shapes, and convert to shapefile. 
        keep_col = ['Stop_ID', 'Stop_Sequence', 'shape_dist_traveled', 'RouteNum', 'RouteName', 
                    'RouteType', 'Direction', 'Index', 'OBJECTID', 'SHAPE']

        rename_col = {'Stop_ID' : 'stop_id', 'Stop_Sequence' : 'stop_seque', 'shape_dist_traveled' : 'dist_trvl', 
                      'RouteNum'  : 'route_num', 'RouteName' : 'route_nme', 
                      'RouteType' : 'route_type', 'Direction' : 'direction', 
                      'Index' : 'index', 'OBJECTID' : 'objectid'}

        try:
            (
                fset[keep_col]
                    .rename(columns = rename_col)
                    .drop(fset.index[rmv_indices.FinIndex])
                    .iloc[:-1]
                    .spatial.to_featureclass(rte_shp_name)
            )

        except Exception as e:
            pass

    
    def _transit_stops(self, tmp_stp, stp_shp_name, wkid):
        """
        Create transit stop for each route. 

        :params tmp_stp: Dataframe containing transit stop information. 
        :params stp_shp_name: The output shapefile name of the transit stop. 
        :params wkid: The spatial reference to project transit stops. 
        """

        feature_set = []
        for t in range(len(tmp_stp)):
            tmp_point = Point({'spatialReference' : {'latestWkid' : wkid}, 
                               'x' : tmp_stp.stop_lon.iloc[t], 
                               'y' : tmp_stp.stop_lat.iloc[t]})
            tmp_feat  = Feature(tmp_point, attributes=tmp_stp.iloc[t].to_dict())
            feature_set.append(tmp_feat)

        fset = FeatureSet(features=feature_set, 
                          geometry_type="Point", 
                          spatial_reference={"latestWkid" : wkid, 'wkid' : wkid}).sdf #wkid original -> 102100

        rename_col = {'Stop_ID' : 'stop_id', 'Stop_Sequence' : 'stop_seque', 
                      'RouteNum'  : 'route_num', 'RouteName' : 'route_nme', 
                      'Direction' : 'direction', 'OBJECTID' : 'objectid'}

        try:
            fset.rename(columns = rename_col).spatial.to_featureclass(stp_shp_name)
        
        except Exception as e:
            pass 


    def _list_shps(self, folder_date, wkid):
        """
        List shapefiles containing individual routes and stops. 

        :params folder_date: The date folder that has been recently updated (e.g., 2021-11-27). 

        :returns: One dataframe, route_inf -> the shapefiles for the transit routes.
        """

        processing_set = (
            discover_docs(path=f"../data/2_staging/{folder_date}")
            [['path', 'filename', 'directory']]
            .assign(is_shp   = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "shp").astype(int), 
                    is_route = lambda r: (r['directory'].str.contains('(?i).*Routes.*')), 
                    is_stop  = lambda r: (r['directory'].str.contains('(?i).*Stops.*')), 
                    wkid     = wkid)    
            .query("is_shp == 1 and is_route == True")
            [['directory', 'filename', 'path', 'wkid']]
        )

        return processing_set


    def _secondaryGIS(self, list_shps):

        tqdm.pandas()
        _ = list_shps.progress_apply(lambda r: self._dissolve_rtes(*r), axis=1)


    def _dissolve_rtes(self, directory, filename, path, wkid):
        """
        Dissolve the transit route shapefiles based on stop_id and stop_sequence. 
        
        :params list_rtes: A list of routes (i.e., shapefiles) in the direction bound folder to be processed
        """
        try:
            diss_name = f"{directory}{filename[:-4]}_dissolved.shp"
            Dissolve(path, diss_name, "stop_id;stop_seque", None, "MULTI_PART", "DISSOLVE_LINES")

        except Exception as e:
            pass 


    def _executeGIS(self, file_df, start_method, target_function, params):
        """
        Initiates GIS processes in Parallel based on direction bound. 

        :params file_df: The main dataframe that contains file information (i.e., directory, filename).
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn).
        :params target_function: The main function that will be used in parallel processing. 
        :params params: A tuple/list of parameters that will be used in parallel processing.
        :params main_list: NumPy array that is split into sub-lists (i.e., chunks) that will be used in parallel processing.
        """

        bnds = file_df.directory.unique()
        for b in bnds:
            sub_rtes  = file_df.query('directory == @b')
            split_val = array_split(sub_rtes, cpu_count()) 
            ParallelProcess(start_method=start_method, 
                            targeter=target_function, 
                            parameters=params, 
                            split_val=split_val)


class GenCsvGTFS:

    def __init__(self, txt_file_path, gen_file_path, start_method, folder_date):
        """
        Create transit routes and stops as CSV files stored in the 2_staging folder. 

        :params txt_file_path: Folder that hosts the static GTFS text files -> /data/0_external/{GTFS update}. 
        :params gen_file_path: Folder that will host the transit routes and stops shapefiles -> /data/2_staging. 
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn).
        :params folder_date: The date that the GTFS static file & RT is being assessed. 
        """

        self._check_dirs(gen_file_path=gen_file_path)
        self._mainCSV(txt_file_path=txt_file_path, 
                      gen_file_path=gen_file_path,
                      start_method=start_method)

    
    def _check_dirs(self, gen_file_path):
        """
        Check sub-folders in the 2_staging folder. If does not exist, then create. 

        :params gen_file_path: The 2_staging folder path. 
        """

        print("1-c. Checking - 2_staging folder. Create TripIDs, Stops, Routes if doesn't exist.")
        folders = ['TripIDs', 'Stops', 'Routes'] 
        bounds  = ['Inbound', 'Outbound']
        paths   = [f"{gen_file_path}/{f}/{b}" for f in folders for b in bounds]
        [os.makedirs(p) for p in paths if not os.path.exists(p)]


    def _process_trip_info(self, unique_trips, gen_file_path, trips_df, shapes, routes, stops, stop_time):
        """
        Creates 2 types of csv files for each unique route (e.g., 1-10144 Inbound) stored in these folders:
            1) ../data/2_staging/Routes/{direction_bound}/*.csv
            2) ../data/2_staging/Stops/{direction_bound}/*.csv

        :params unique_trips: A list of unique trip_ids from unique_inf field.
        :params gen_file_path: The 2_staging folder path. 
        :params trips_df: The trip_df from the trips.txt dataframe. 
        :params shapes: The shapes.txt dataframe. 
        :params routes: The routes.txt dataframe. 
        :params stops: The stops.txt dataframe.
        :params stop_time: The stop_time.txt dataframe. 
        """
        # For each unique trip 
        for u in tqdm(unique_trips):
            temp_df_tripshp = trips_df.query('UniqueInf == @u')
            temp_df_shapesp = shapes.query('shape_id == @temp_df_tripshp.shape_id.iloc[0]')
            temp_route      = routes.query('route_id == @temp_df_tripshp.route_id.iloc[0]')

            # Subset of trips.txt
            # Schema - route_id, service_id, trip_id, trip_headsign, direction_id, block_id, shape_id, Direction,
            #          UniqueInf, RouteNum, RouteName, RouteType  
            temp_df_tripshp = (
                temp_df_tripshp
                    .assign(RouteNum  = temp_route['route_short_name'].iloc[0], 
                            RouteName = temp_route['route_long_name'].iloc[0], 
                            RouteType = temp_route['route_type'].iloc[0])
            )

            fill_lat = temp_df_shapesp.shape_pt_lat.iloc[-1]
            fill_lon = temp_df_shapesp.shape_pt_lon.iloc[-1]

            # Subset of Shapes.txt
            # Schema - shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence, shape_dist_traveled, RouteNum,
            #          RouteName, RouteType, Direction, end_lat, end_lon 
            # Transit route information
            temp_df_shapesp = (
                temp_df_shapesp
                    .assign(RouteNum  = temp_route['route_short_name'].iloc[0], 
                            RouteName = temp_route['route_long_name'].iloc[0], 
                            RouteType = temp_route['route_type'].iloc[0], 
                            Direction = temp_df_tripshp['Direction'].iloc[0], 
                            end_lat   = temp_df_shapesp.shape_pt_lat.shift(-1, fill_value=fill_lat), 
                            end_lon   = temp_df_shapesp.shape_pt_lon.shift(-1, fill_value=fill_lon))
            )

            bound_direction = temp_df_shapesp.Direction.iloc[0]
            route_num       = temp_df_shapesp.RouteNum.iloc[0]
            shape_id        = temp_df_shapesp.shape_id.iloc[0] 

            route_file_name = f"{gen_file_path}/Routes/{bound_direction}/{route_num}-{shape_id}.csv"
            stop_file_name  = f"{gen_file_path}/Stops/{bound_direction}/{route_num}-{shape_id}.csv"

            temp_df_shapesp.to_csv(route_file_name, index=False)


            # Subset of stop_times.txt
            # Schema - stop_id, stop_sequence, unique_stps
            tmp_stp = (
                stop_time
                    .query('trip_id in @temp_df_tripshp.trip_id.unique()')
                    .sort_values(['stop_sequence', 'arrival_time'])
                    [['stop_id', 'stop_sequence']]
                    .assign(unique_stps = lambda r: r['stop_id'].astype(str) + "-" + r['stop_sequence'].astype(str))
            )

            tmp_df = (
                DataFrame({'Stop_ID'       : list(map(lambda x: int(x.split("-")[0]), tmp_stp.unique_stps.unique())), 
                           'Stop_Sequence' : list(map(lambda x: int(x.split("-")[1]), tmp_stp.unique_stps.unique())),
                           'Direction'     : temp_df_shapesp.Direction.iloc[0],
                           'RouteNum'      : temp_df_shapesp.RouteNum.iloc[0],
                           'RouteName'     : temp_df_shapesp.RouteName.iloc[0]})
            )


            # Schema - Stop_ID, Stop_Sequence, stop_lat, stop_lon, RouteNum, Direction, RouteName
            # Transit stop information
            tmp_stp_info_df = (
                stops
                    .query('stop_id in @tmp_df.Stop_ID.unique()')
                    [['stop_id', 'stop_lat', 'stop_lon']]
                    .rename(columns = {'stop_id' : 'Stop_ID'})
                    .merge(tmp_df, on=['Stop_ID'])
                    [['Stop_ID', 'Stop_Sequence', 'stop_lat', 'stop_lon', 
                      'RouteNum', 'Direction', 'RouteName']]
                    .sort_values('Stop_Sequence')
            )

            tmp_stp_info_df.to_csv(stop_file_name, index = False)


    def _mainCSV(self, txt_file_path, gen_file_path, start_method):
        """
        Read selected static GTFS files and create GTFS routes and transit stops as csv files. 

        :params txt_file_path: Folder that hosts the static GTFS text files -> /data/0_external/{GTFS update}.
        :params gen_file_path: Folder that will host the transit routes and stops shapefiles -> /data/2_staging.
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn).
        """

        gtfs_files = ['routes.txt', 'shapes.txt', 'stops.txt', 'stop_times.txt', 'trips.txt']

        txt_files = (
            discover_docs(path = txt_file_path)
            [['path', 'filename']]
            .assign(is_txt = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "txt").astype(int))          
            .query('is_txt == 1 and filename in @gtfs_files')
        )

        print("1-d. Reading selected GTFS static files.")

        # Read relevant static GTFS files 
        trips     = read_table(txt_files.query('filename == @gtfs_files[-1]').path.iloc[0], sep=",")
        shapes    = read_table(txt_files.query('filename == @gtfs_files[1]').path.iloc[0], sep=",")
        routes    = read_table(txt_files.query('filename == @gtfs_files[0]').path.iloc[0], sep=",")
        stops     = read_table(txt_files.query('filename == @gtfs_files[2]').path.iloc[0], sep=",")
        stop_time = read_table(txt_files.query('filename == @gtfs_files[-2]').path.iloc[0], sep=",")

        # Schema - route_id, service_id, trip_id, trip_headsign, direction_id, block_id, shape_id, Direction, UniqueInf
        trips_df = (
            trips
                .assign(Direction = lambda r: where(r['direction_id']==1, "Inbound", "Outbound"),
                        UniqueInf = lambda r: r['route_id'] + "-" + r['shape_id'].astype(str))
        )

        unique_trips = trips_df.UniqueInf.unique()
        
        main_list = array_split(unique_trips, cpu_count())

        print("1-e. Parallel Processing - Creating CSV files in 2_staging folder.")
        start_time = CalcTime().starter()

        ParallelProcess(start_method=start_method, 
                        targeter=self._process_trip_info, 
                        parameters=(unique_trips, gen_file_path, trips_df, shapes, routes, stops, stop_time), 
                        split_val=main_list)
        
        end_time = CalcTime().finisher(start_time=start_time)
        
        print(f"Parallel Processing (Create CSV files) - Complete: {end_time[0]} hr., {end_time[1]} min. and {round(end_time[2])} sec.")


class CheckGTFS:

    def __init__(self, main_link, pattern_txt, hyperlink, start_method, wkid):
        """
        Get the static GTFS file that matches to the collected GTFS-RT csv file that needs to be processed. 

        :params main_link: The main string inside the main hyperlink (e.g., https://transitfeeds.com)
        :params pattern_txt: The pattern text in the hyperlink (e.g., r".*/p/calgary-transit/238.*download.*")
        :params hyperlink: The main hyperlink (e.g., https://transitfeeds.com/p/calgary-transit/238/latest/download)
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork (spawn if using ArcPy); Windows OS -> spawn)
        :params wkid: The spatial reference (e.g., 4326) to project transit routes and stops. 
        """
        
        self.main_link    = main_link
        self.pattern_txt  = pattern_txt
        self.hyperlink    = hyperlink
        
        """
        self._gtfsrt_files()    -> File information of the inventory GTFS-RT csv files that need to be processed. 
        self._get_static_gtfs() -> Get the static GTFS file from the transit aegncy.  
        """

        self.csv_files = self._gtfsrt_files()
        self._get_static_gtfs(start_method=start_method, wkid=wkid)


    def _gtfsrt_files(self) -> DataFrame:
        """
        Get inventory of GTFS-RT csv files that need to be processed. 

        :returns: DataFrame with file information. 
        """

        print('1-a. Get inventory of CSV GTFS-RT files that need to be processed.')

        files_df = (
            discover_docs(path="../data/0_external/")
                [['path', 'filename']]
                .assign(is_txt = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "txt").astype(int), 
                        is_csv = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "csv").astype(int))
                .query("is_csv == 1")
                .assign(is_complete   = lambda r: (r['filename'].str.contains('(?i).*complete.*')), 
                        file_date     = lambda r: (r['filename'].str.extract("([0-9]+[0-9]+-[0-9]+-[0-9].)")), 
                        year          = lambda r: (r['file_date'].str.split("-").str[0]), 
                        month         = lambda r: (r['file_date'].str.split("-").str[1].str.zfill(2)),
                        day           = lambda r: (r['file_date'].str.split("-").str[2].str.zfill(2)), 
                        file_date_ref = lambda r: r['year'] + "-" + r['month'] + "-" + r['day'])
                .query("is_complete == False")
        )

        return files_df


    def _checkUpdate(self, gtfs_df, start_method, wkid):
        """
        Create directories if static GTFS files is needed and generate transit routes and stops via the GenCsvGTFS and GenShpGTFS classes. 

        :params gtfs_df: A DataFrame containing Clean_Date (e.g., 2021-12-02), Date_Info (e.g., 02122021), and Href (e.g., hyperlink)
        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn)
        :params wkid: The spatial reference to project transit routes and stops. 
        """

        # Identify what is compatible from the GTFS static date to date of the GTFS-RT file. 
        list_files, list_date, href_inf = [],[],[]
        for r in range(len(self.csv_files)):
            csv_file  = self.csv_files.path.iloc[r]
            gtfs_date = self.csv_files.file_date_ref.iloc[r]
            sub_gtfs  = gtfs_df.query('Clean_Date <= @gtfs_date')
            list_files.append(csv_file)
            list_date.append(sub_gtfs.Clean_Date.iloc[0])
            href_inf.append(sub_gtfs.Href.iloc[0])

        info_date_df = DataFrame({'List_Files' : list_files, 'List_Date' : list_date, 'Href' : href_inf})

        # Go through each unique date and move the csv files to their appropriate date folder
        for i in info_date_df.List_Date.unique():
            tmp_df   = info_date_df.query('List_Date == @i')
            tmp_date = tmp_df.List_Date.iloc[0]
            print(tmp_date)
            main_dirs = ["../data/0_external/GTFS/", "../data/1_raw/", "../data/2_staging/", 
                         "../data/3_interim/", "../data/4_processed/", "../data/5_conformed/", 
                         "../data/6_analyses/", "../data/7_requests/"]

            # Make directories if they don't exist
            dir_list  = [f"{d}{tmp_date}" for d in main_dirs]
            [os.makedirs(c) for c in dir_list if not os.path.exists(c)]
            # Move the GTFS-RT csv file to its date folder 
            [shutil.move(t, dir_list[0]) for t in tmp_df.List_Files]
            
            ######################################################################################################################
            ############### If static GTFS files do not exist, then download and create routes & transit stops. ##################
            ######################################### Execute GenCsvGTFS class ###################################################
            ######################################### Execute GenShpGTFS class ###################################################
            ######################################################################################################################

            is_txt = (
                discover_docs(path = f"../data/0_external/GTFS/{tmp_date}")
                [['path', 'filename']]
                .assign(is_txt = lambda r: (r['filename'].str.extract("([^.]+)$", expand=False).str.lower() == "txt").astype(int))
                [['is_txt']].iloc[0][0]         
            )

            txt_file_path = dir_list[0] # Folder to host static GTFS files 
            gen_file_path = dir_list[2] # Folder to host transit routes and stops as shapefiles 

            if is_txt == 0:
                gtfs_zip = get(tmp_df.Href.iloc[0])
                with open(f"{txt_file_path}/gtfs.zip", "wb") as my_zip:
                    my_zip.write(gtfs_zip.content)
                zipfile.ZipFile(f"{txt_file_path}/gtfs.zip", "r").extractall(txt_file_path)
            
                _ = GenCsvGTFS(txt_file_path=txt_file_path,
                               gen_file_path=gen_file_path, 
                               start_method=start_method, 
                               folder_date=tmp_date)
                
                _ = GenShpGTFS(folder_date=tmp_date, 
                               start_method=start_method, 
                               wkid=wkid)

                
    def _get_static_gtfs(self, start_method, wkid):
        """
        Get the static GTFS file from the transit agency. 

        :params start_method: The start method to initiate parallel processing (Linux OS -> fork; Windows OS -> spawn)
        :params wkid: The spatial reference to project transit routes and stops. 
        """

        print("1-b. Scrape date information of static GTFS files from transit agency.")

        # Identify the hyperlinks 
        gtfs_soup = list(BeautifulSoup(get(self.hyperlink).text, 'html.parser').findAll("a", href=re.compile(self.pattern_txt)))
        
        # Get the hyperlinks in text form
        href_info = ["".join([self.main_link, g.get('href')]) for g in gtfs_soup][1:]

        # Get the date information from the hyperlinks
        date_info = [re.sub(r'^.*/(\d{8,}|latest).*', r'\1', g.get('href')) for g in gtfs_soup][1:]

        # Refine the date into date string format (e.g., 20211117 -> 2021-11-17)
        ref_date  = [f"{d[0:4]}-{d[4:6]}-{d[6:]}" if "latest" not in d else "latest" for d in date_info]

        # Get the latest date for GTFS update. 
        get_latest_date = (
            BeautifulSoup(get(href_info[0][:re.search('download', href_info[0]).start()-1]).text, 'html.parser')
            .find('h1').text
            .replace('Latest', "").replace('(', "").replace(')', "")
            .lstrip()
            .split(" ")
        )

        # Format day if it is single digit 
        get_latest_date[0] = get_latest_date[0].zfill(2)

        # Format month string to month number
        get_latest_date[1] = str(strptime(get_latest_date[1], '%B').tm_mon).zfill(2)
        compile_date       = f"{get_latest_date[2]}-{get_latest_date[1]}-{get_latest_date[0]}"
        ref_date[0]        = compile_date
        date_info[0]       = "".join(get_latest_date)
        href_info[0]       = href_info[0].replace('latest', date_info[0])

        # GTFS update info
        gtfs_df = DataFrame({"Clean_Date" : ref_date, "Date_Info" : date_info, "Href" : href_info})
        self._checkUpdate(gtfs_df=gtfs_df, start_method=start_method, wkid=wkid)