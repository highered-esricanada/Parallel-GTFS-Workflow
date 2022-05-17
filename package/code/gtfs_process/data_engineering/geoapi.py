"""
Author: Anastassios Dardas, PhD - Higher Education Specialist at Education & Research at Esri Canada. 
Date: Remodified Q1 - 2022. 

About: From per transit route and trip_id, this geoprocessing operation identifies where along 
        the transit route was the vehicle at. 

Warning Note: This operation runs while in Parallel from previous operation beforehand. 
              Although not recommended, it can be used outside parallel environment and 
              finding vehicle locations from one transit route at a time. 

              Big O Notation is high due to the nested apply functions. In theory, there
              is a faster way to do this using ArcPy instead of the ArcGIS API for Python. 
              However, this would require writing and reading a lot of shapefiles, then 
              converting results as dataframes in memory, which altogether will slow 
              down the runtime operation and bloat disk space.

Requires: 
    1) ArcGIS API for Python 
"""

from arcgis.features import GeoAccessor
from arcgis.geometry import Point, Polyline
from pandas import json_normalize, DataFrame, Series, concat


class Maingeo:

    def __init__(self, folder_date, output_folder, raw_date, indiv_rte: DataFrame, wkid, unique_val, L):
        """
        Approximately find the geographic location of each vehicle's position.
        
        :param folder_date: The date that belongs to the static GTFS update across the project directory (e.g., 0_external/2021-11-17; 2_staging/2021-11-17)
        :param output_folder: The contents where the output will be exported and stored.
        :param raw_date: The date of the collected raw GTFS-RT data.
        :param indiv_rte: DataFrame of the collected raw GTFS-RT data and file explorer merged (i.e., individual route).
        :param wkid: The spatial reference number.
        :param unique_val: The unique-rte currently inspecting.
        :param L: The list that is part of the Manager in Multiprocessing.
        """
        
        # Read undissolved & dissolved transit routes and transit stop shapefiles matching the individual transit route.
        transit_files = self._read_relevant_files(indiv_rte=indiv_rte, 
                                                  unique_val=unique_val,
                                                  folder_date=folder_date,
                                                  raw_date=raw_date,
                                                  L=L)

        # Check if the necessary files and geometry paths exist, if so - return as a tuple (ArcGIS Geometry - Polyline; Recorded trip_ids identified)
        prep_inf_geo  = self._check_geoprocess(transit_files=transit_files, 
                                               wkid=wkid, 
                                               unique_val=unique_val, 
                                               L=L,
                                               folder_date=folder_date,
                                               raw_date=raw_date)

        # If all necessary static files and polyline route exist, then proceed to the main geoprocessing operation. 
        # NOTE: This is the most time consuming operation as it iterates every vehicle location recorded per trip_id
        #       snaps it to the nearest line on the transit route, from there iterates which dissolved segment it is 
        #       within and lists undissolved segment candidates, and then groups by trip_id and barcode to iterate 
        #       through the undissolved segment candidates and identifies one that is within.        
        if prep_inf_geo[1] == 1:
            self.finalgeo = self._geolocate(polyline_rte=prep_inf_geo[0], 
                                            indiv_rte=indiv_rte, 
                                            diss_file=transit_files[1], 
                                            undiss_file=transit_files[0],
                                            stop_file=transit_files[2],
                                            wkid=wkid,
                                            unique_val=unique_val, 
                                            folder_date=folder_date,
                                            output_folder=output_folder,
                                            raw_date=raw_date,
                                            L=L)
        else:
            self.finalgeo = None


    def _read_relevant_files(self, indiv_rte: DataFrame, unique_val, folder_date, raw_date, L):
        """
        Read the relevant shapefiles that are for each unique transit route. 
        
        :param indiv_rte: DataFrame of the individual route being assessed.
        :param unique_val: The unique-rte currently inspecting.
        :param folder_date: The date that belongs to the static GTFS update across the project directory
               (e.g., 0_external/2021-11-17; 2_staging/2021-11-17).
        :param raw_date: The date of the collected raw GTFS-RT data.
        :param L: The list that is part of the Manager in Multiprocessing.

        :returns: Conditional Tuple:
                    If True: (0 = The dissolved transit route;
                              1 = undissolved transit route;
                              2 = transit stop) <- all as dataframes & PROCEED DOWNSTREAM.

                    If false: 0 <- PREVENT DOWNSTREAM.
        """

        try:

            undiss_file = indiv_rte.Undiss_Rte.iloc[0]
            diss_file   = indiv_rte.Diss_Rte.iloc[0]
            stop_file   = indiv_rte.Stop.iloc[0]

            undiss_rte = GeoAccessor.from_featureclass(undiss_file) # undissolved transit route
            diss_rte   = GeoAccessor.from_featureclass(diss_file)   # dissolved transit route
            stop_df    = GeoAccessor.from_featureclass(stop_file)   # transit stop 

            return (undiss_rte, diss_rte, stop_df)

        except OSError as error:

            try:
                # If the previous search failed - use alternative naming - sometimes the route_id and shape_id together can't be found.
                # The altenative naming roots back to route_id. 
                # It also depends if all of the static files exist - if one does not, then it won't proceed downstream.
                undiss_file = indiv_rte.Alt_Undiss_Rte.iloc[0]
                diss_file   = indiv_rte.Alt_Diss_Rte.iloc[0]
                stop_file   = indiv_rte.Alt_Stop.iloc[0]

                undiss_rte = GeoAccessor.from_featureclass(undiss_file) # undissolved transit route
                diss_rte   = GeoAccessor.from_featureclass(diss_file)   # dissolved transit route
                stop_df    = GeoAccessor.from_featureclass(stop_file)   # transit stop

                return (undiss_rte, diss_rte, stop_df)

            except OSError as error:
                L.append(f"{raw_date},{folder_date},{unique_val},{error}")
                return 0


    def _check_geoprocess(self, transit_files, wkid, unique_val, L, folder_date, raw_date):
        """
        CRITICAL
        Check if the dissolved file exists and if so, proceed to making polyline rte in memory. 
        Check if paths exist during the polyline rte process. If so, proceed to downstream workflow.

        :param transit_files: Spatial DataFrames of undissolved & dissolved transit routes, and transit stop per individual transit route.
        :param wkid: The spatial reference used to create polyline.
        :param unique_val: The unique-rte currently used.
        :param L: The list that is part of the Manager in Multiprocessing.
        :param folder_date: The date that belongs to the static GTFS update across the project directory
               (e.g., 0_external/2021-11-17; 2_staging/2021-11-17).
        :param raw_date: The date of the collected raw GTFS-RT data.

        :returns: Conditional Tuple:
                    If true, ArcGIS geometry - Polyline and 1 <- PROCEED DOWNSTREAM.
                    If false, (0,0) <- STOP DOWNSTREAM
        """

        if transit_files is not 0:
            # Create Polyline in memory for each transit route  
            polyline_rte  = self._generate_polyline(dissolved_df=transit_files[1], 
                                                    wkid=wkid, 
                                                    unique_val=unique_val, 
                                                    L=L,
                                                    folder_date=folder_date,
                                                    raw_date=raw_date)

            # Set the 1st value list to 1 - indicating both polyline rte and all static GTFS files exist
            if polyline_rte is not 0:
                return (polyline_rte,1)

            else:
                return (0,0)

        else:
            return (0,0)


    def _generate_polyline(self, dissolved_df, wkid, unique_val, raw_date, folder_date, L):
        """
        Create a Polyline for the dissolved route. 

        :param dissolved_df: The dissolved transit route as a dataframe.
        :param wkid: The spatial reference number.
        :param unique_val: The unique-rte currently inspecting.
        :param raw_date: The date of the collected raw GTFS-RT data.
        :param L: The list that is part of the Manager in Multiprocessing.

        :returns: ArcGIS Geometry Polyline or 0 (downstream prevention).
        """

        try:
            paths = (
                dissolved_df
                    ['SHAPE']
                    .pipe(json_normalize)
                    ['paths']
                    .apply(lambda row: row[0])
                    .tolist()
            )

            return Polyline({'spatialReference' : {'latestWkid' : wkid}, 'paths' : paths})

        except KeyError as error:
            L.append(f"{raw_date},{folder_date},{unique_val},{error}")
            return 0 


    def _trace_point(self, x, y, wkid):
        """
        Creates a Point geometry. 

        :param x: The longitude coordinate.
        :param y: The latitude coordinate.
        :param wkid: The spatial reference to project.

        :returns: ArcGIS Point geometry. 
        """

        return Point({'spatialReference' : {'latestWkid' : wkid}, 'x' : x, 'y' : y})


    def _extract_point_datum(self, s: Series):
        """
        Evaluate the point and convert to json to dictionary. Update the original dataframe 
        and convert the entries to json format. 

        :param s: A series (individual row) from a dataframe.

        :returns: JSON format. 
        """

        # Per row
        entries = s.to_dict()

        point_dict = eval(s['point'])
        point_dict = json_normalize(point_dict).to_dict(orient='index')[0]

        entries.update(point_dict)

        return json_normalize(entries)


    def _extract_point_data(self, d: DataFrame):
        """
        Extract the point data into fields (x, y, spatialReference.latestWkid) and append to
        dataframe. 

        :param d: The DataFrame after the group by trip_id.
        
        :returns: Updated dataframe with appended columns. 
        """

        return (
            d
                .apply(self._extract_point_datum, axis=1)
                .pipe(lambda s: concat(s.tolist()))
                .drop(columns=["spatialReference.latestWkid"])
        )


    def _snap_pt_line(self, polyline_rte: Polyline, lon_val, lat_val, wkid):
        """
        Snap each vehicle location (i.e., point) to the nearest line of the transit route. 

        :param polyline_rte: ArcGIS Geometry - polyline of the transit route.
        :param lon_val: The longitude coordinate of the vehicle location.
        :param lat_val: The latitude coordinate of the vehicle location.
        :param wkid: The spatial reference to project the snapped point.

        :returns: ArcGIS Point geometry with new (i.e., snapped) coordinates. 
        """

        return polyline_rte.snap_to_line(self._trace_point(x=lon_val, y=lat_val, wkid=wkid))


    def _get_atomic_snap_pt_details(self, row: Series, polyline_rte: Polyline, wkid):
        """
        Get the snap point details for 1 associated combination of Trip ID & Lat/Lon.

        :param row: The individual row containing the vehicle spatiotemporal information.
        :param polyline_rte: ArcGIS Polyline geometry of the transit route.
        :param wkid: Spatial reference to project the ArcGIS Point of the vehicle's location.

        :returns: A single row DataFrame consists of Trip ID, timestamp, and snapped point details.
        """

        # Snap the point to the nearest line.
        snap_pt = self._snap_pt_line(polyline_rte=polyline_rte, 
                                     lon_val=row['Lon'], 
                                     lat_val=row['Lat'], 
                                     wkid=wkid)

        # Return a single row DataFrame consisting of Trip ID, Timestamp, and Snap Point details
        return (
            row
                .pipe(lambda s: DataFrame([s.values], columns=s.index)) # Convert row into a DataFrame
                [['trip_id', 'Local_Time']] # Select columns of interest
                .assign(point=str(snap_pt))
        )


    def _main_snap(self, polyline_rte:Polyline, e: Series, wkid):
        """
        Snap points to the nearest line of the transit route and 
        consolidate list of snap points details into a single DataFrame. 

        :param polyline_rte: ArcGIS geometry (Polyline) of the transit route.
        :param e: A series from a groupby via trip_id.
        :param wkid: Spatial reference to project the Points.

        :returns: A concatenated DataFrame of the snap point details. 
        """

        # Get list of DataFrame with each row consisting of Trip ID, Time, and snap point details
        snap_pts = [self._get_atomic_snap_pt_details(row=row, polyline_rte=polyline_rte, wkid=wkid)
                    for index, row in e.iterrows()]

        # Consolidate list of DataFrames into a single DataFrame
        return concat(snap_pts)


    def _trace_point_within_segment_set(self, point, segments):
        """
        Check if a point fits/is within a (un)dissolved line, return only the successful match.

        :param point: In string json format containing point information (x,y, spatial reference).
        :param segments: Tuple form containing - the shape of the polyline and its identifier (e.g., stop_sequence, index).

        :returns: The value of the identifier if within; otherwise, None.
        """

        pt = Point(eval(point))

        for (segment, identifier) in segments:
            if pt.within(segment):
                return identifier

        return None


    def _trace_undissolved_within_dissolved_set(self, tmp_df: DataFrame, undissolved_df: DataFrame, dissolved_lines):
        """
        Identify where the vehicles are on transit route via dissolved segments (generic). From there come up with a list of undissolved segment
        based on stop_sequence.
        
        :param tmp_df: A subset DataFrame based on per trip_id - contains snapped points and other attributes - see cols_trip_id.
        :param undissolved_df: A spatial dataframe containing the undissolved polylines with attributes.
        :param dissolved_lines: A tuple consisting shape of individual dissolved polyline and its associated stop sequence.

        :returns: DataFrame with undissolved segment candidates derived from the dissolved segment - includes 
                  stop_id, stop_seque, index, objectid, SHAPE, barcode, Local_Time, trip_id, point. 
        """

        cols_trip_id   = ['trip_id', 'Local_Time', 'point', 'x', 'y', 'wkid']
        cols_dissolved = ['stop_id', 'stop_seque', 'index', 'objectid', 'SHAPE']

        return (
            tmp_df[cols_trip_id]
                # Trace the stop sequence best affiliated with the dissolved line against x,y coordinates
                .assign(stop_sequence = lambda r: r.apply(lambda l: self._trace_point_within_segment_set(point=l['point'], 
                                                                                                         segments=dissolved_lines),
                                                                                                         axis=1)) 
                # Sort by 
                #.sort_values(['trip_id', 'Local_Time'], ascending=True)
                # Generate a unique identifier for each lookup that was performed. 
                .assign(counter=1, 
                        barcode=lambda r: r['counter'].cumsum())
                .drop(columns=['counter'])
                # Drop instances where stop sequence did not yield a match 
                .query('stop_sequence.notnull()', engine='python')
                # Cross reference the matched Stop ID (from the x,y lookup of dissolved lines) against the undissolved lines.
                .merge(undissolved_df[cols_dissolved], left_on=['stop_sequence'], right_on=['stop_seque'], how='inner', validate='m:m')
                # Select columns
                [cols_dissolved + ['barcode', 'Local_Time', 'trip_id', 'point', 'x', 'y', 'wkid']]
        )


    def _finalize_undissolved_candidate(self, tmp_df: DataFrame):
        """
        Goes through every undissolved segment corresponding to the grouped barcode and trip_id. Captures which
        undissolved segment is the snapped point truly within.

        :param tmp_df: A subset DataFrame via group by of barcode and trip_id.

        :returns: A column containing the index value of the undissolved segment including NaN.
                  NaN indicates not within, whereas a real value indicates identified.
        """

        return (
            tmp_df
                .assign(index_val = lambda r: r.apply(lambda l: self._trace_point_within_segment_set(point=l['point'], segments=[(Polyline(l['SHAPE']), l['index'])]), axis=1))
        )


    def _geolocate(self, polyline_rte: Polyline, indiv_rte: DataFrame, diss_file: DataFrame, undiss_file: DataFrame,
                   stop_file: DataFrame, wkid, unique_val, L, folder_date, output_folder, raw_date):
        """
        The main operation that identifies where each vehicle is exactly located along the transit route. 
        The geoprocesses are the following per transit route (processed in parallel):
            1) Group by trip_id and snap each vehicle location to the nearest point of the line on their transit route. 
            2) Group by trip_id, use their snapped points to identify which dissolved segment it is within.
               Acquire associated undissolved segment candidates of that dissolved segment.
            3) Group by trip_id and barcode, use their snapped points to identify which undissolved segment it is within. 

        :param polyline_rte: ArcGIS Geometry - Polyline of the transit route (dissolved).
        :param indiv_rte: A subset DataFrame per transit route containing GTFS-RT and their associated GTFS static
                          shapefiles and csv files.
        :param diss_file: A spatial DataFrame of the transit route containing its SHAPE and attribute information per
                          dissolved line segment (each represent a stop_seque and stop_id).
        :param undiss_file: A spatial DataFrame of the transit route containing its SHAPE and attribute information per
                            undissolved line segment (each represent a stop_seque and stop_id).
        :param stop_file: A spatial DataFrame of the transit route containing transit stop information.
        :param wkid: Spatial reference used to project snapped points.
        :param unique_val: The unique-rte currently inspecting.
        :param L: The list that is part of the Manager in Multiprocessing.
        :param folder_date: The date that belongs to the static GTFS update across the project directory
        :param output_folder: The contents where the output folder is exported and stored.
        :param raw_date: The date of the collected raw GTFS-RT data.

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

        # Set up dissolved lines info - individual polylines and correspond to their stop sequence
        diss_lines = [(Polyline(r['SHAPE']), r['stop_seque']) for i, r in diss_file.iterrows()]

        try:

            fin_df = (
                indiv_rte
                    # Group by trip_id and snap each vehicle location to the nearest line on the transit route
                    .groupby('trip_id', as_index=False)
                    .apply(lambda e: self._main_snap(polyline_rte=polyline_rte, e=e, wkid=wkid))
                    .pipe(self._extract_point_data)
                    .rename(columns = {'spatialReference.wkid' : 'wkid'})
                    # Group by trip_id - use their snapped point location to identify which dissolved segment
                    # they're within - acquire undissolved segment candidates.
                    .groupby(['trip_id'], as_index=False)
                    .apply(lambda r: self._trace_undissolved_within_dissolved_set(tmp_df=r, 
                                                                                  undissolved_df=undiss_file, 
                                                                                  dissolved_lines=diss_lines))
                    # Group by trip_id and barcode (liases as time stamp, stop_seque, and stop_id) to identify
                    # which undissolved segment the snapped point is within - final.
                    .groupby(['barcode', 'trip_id'], as_index=False)
                    .apply(lambda e: self._finalize_undissolved_candidate(tmp_df=e))
            )

            try: 
                fin_df = (
                    fin_df
                        .query('index_val.notnull()', engine='python')
                        .drop(columns=['index_val'])
                )

                ### Optional to omit - Best though to keep it there to produce output.
                fin_df.to_csv(f"{output_folder}/{raw_date}_{unique_val}_located.csv")

                return (fin_df, undiss_file, stop_file)

            except Exception as e:
                L.append(f"{raw_date},{folder_date},{unique_val}, unable to locate points in undissolved. Data integrity issue.")
                return None

        except Exception as e: 
            L.append(f"{raw_date},{folder_date},{unique_val}, failure during geoprocess.")
            return None