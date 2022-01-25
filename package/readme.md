# Overview of the Workflow
An overview of the workflow that can be downloaded as a Python package. For organizational purposes, documentation is divided into three components. These are: **1) Data Harvesting**, **2) Data Workflow (main)**, and **3) Data Storage** and they're written in detailed in separate files. The workflow is designed to be downstream, starting from data harvesting and ending at data storage. Code and extensive documentation can be viewed in the [**gtfs_harvester**](gtfs_harvester) folder.

## Data Harvesting 
Collects GTFS-RT every nth (e.g., 30) seconds for x (e.g., 14) hours per day, parses it, and appends to csv file. The csv file is named after the date of collection (e.g., GTFSRT_2022-01-15.csv). 

## Data Workflow
The main operation of the workflow that processes collected raw GTFS-RT data and outputs transit metrics. Approximately 95% of this component runs in parallel. 

## Data Storage
Sends the output transit metrics to MongoDB for storage. 
