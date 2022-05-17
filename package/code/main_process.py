"""
Author: Anastassios Dardas, PhD - Higher Education Specialist, Education & Research Group at Esri Canada
About: 
Requirements: 

"""

from gtfs_process import CheckGTFS, NeedProcess, ExecuteProcess

if __name__ == "__main__":

    ################################################################################################
    ################## 2nd Pipeline: Check for any updates with GTFS static files. #################
    ################################################################################################
    CheckGTFS(main_link="https://transitfeeds.com", 
             pattern_txt= r".*/p/calgary-transit/238/.*download.*", 
             hyperlink="https://transitfeeds.com/p/calgary-transit/238", 
             start_method="spawn", 
             wkid=4326)

    ################################################################################################
    ######################### 
    ################################################################################################
    # Check which raw GTFS-RT csv files need processing
    csv_inf = NeedProcess(main_folder="../data/0_external/GTFS").csv_inf
    testing = ExecuteProcess(csv_inf=csv_inf, 
                             start_method="spawn", 
                             wkid=4326)