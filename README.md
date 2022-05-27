<img src='/img/dashboard_transit.JPG' width='1000'/>

# Parallel GTFS-RT Workflow
A replicable workflow (i.e., data pipeline) that parallel processes GTFS (real-time + static) data to calculate various transit metrics using Geographic Information Systems (GIS) and data engineering techniques. Transit metrics from the workflow can be injected in a Web GIS dashboard to view spatiotemporal variations of transit reliability. 

**Remodification (code) progress**: ![100%](https://progress-bar.dev/100)

## Motivation
COVID-19 has revealed the entire world of how fragile our essential infrastructure is and has reminded us that system adaptation and resilience are crucial (OECD, 2021; Sneader and Lund, 2020). Essential infrastructure is defined as basic necessities that are crucial to the well-being and convenience of the general public. These include basic hygience, healthcare, emergency personnel, clean water, basic hygiene, fresh groceries, energy, communications, and transportation. All of these elements are interlinked and maintains the global economy and modern life. In the wake of rapid population growth, especially mass migrations to cities, and climate change/chaos, the need for resilience and adaptability is a must. Therefore, this research and development project presents a replicable workflow that intends to cover both of these needs in the context of urban public transit services.

## Manuscript
The manuscript can be viewed (with subscription / institutional access) via the link: https://onlinelibrary.wiley.com/doi/abs/10.1111/tgis.12942 
<br>

<strong>Note</strong>: the fundamental concepts remain the same, however, with code remodification the data structure is different and more intuitive in the GitHub than the one displayed in the manuscript.

## How to use
Download the repository as a zip file, unzip it, and proceed to the [**package**](/package) folder. This is where extensive documentation on the structure of the entire repository can be viewed. For direct use of the code, extensive documentation can be found in the [**code**](/package/code) folder with more information available nested to its sub-folders. There is sample GTFS-RT data that has been collected and processed throughout the entire workflow. This can be used to either test out the code for processing <strong><a href='package/code/main_process.py'>main_process.py</a></strong> or to inspect the data structures. If test driving the code, temporarily comment out CheckGTFS as this component is meant to be aligned with the most recent GTFS update in conjunction to the most recent collected GTFS-RT data.

Unzip and extract the [**data**](/package/data) folders - it is crucially important to retain this file directory structure as the backend processes heavily rely on it. Inside the <strong>0_external</strong> folder contains a sample of raw GTFS-RT for Sept. 30th, 2021. This can be used to run and test the entire backend process. If so, comment out the CheckGTFS code in the <strong>main_process.py</strong> script. On a 96 core machine, this entire process took about 14.5 minutes to complete. 


## Software Requirements
1) Python 3.x run by conda environment in ArcGIS 
   - Cloned conda env from ArcGIS. 
   - Packages:
     - ArcGIS API for Python 
     - ArcPy (to support the API) 
     - Pandas (usually part of Python base)
     - NumPy (usually part of Python base)
     - Multiprocessing (usually part of Python base)
     - Functools (usually part of Python base)
     - BeautifulSoup via bs4
     - tqdm 
     - Google Transit (for GTFS via pip install --upgrade gtfs-realtime-bindings)

Further (and more precise) instructions in how to clone your conda environment (with ArcPy & ArcGIS API) and set up the required packages can be viewed in the [**requirements.txt**](/package/code/requirements.txt). 

  
2) ArcGIS License to use ArcPy and by extension the ArcGIS API for Python
   - **Windows Users**: Single sign-on in ArcGIS Pro or ArcGIS Enterprise (Base Server - minimum). 
   - **Linux Users**: ArcGIS Enterprise (Base Server - minimum). 
   
3) [(Optional) MongoDB installed](https://docs.mongodb.com/manual/installation/) 


## Hardware Recommendations
This workflow is designed to run on two Azure VMs with crontab (Linux) and [**subprocess**](/package/code/transfer.py) configured separately: 
   - 1 small VM (2 CPUs) for harvesting the data and then securely transferring the file over to the large VM. 
   - 1 large VM (96 CPUs) for processing static and near real-time GTFS data 

The entire workflow can be done on a single machine with a minimum of 8 CPUs (i5, i7, or i9 series); however, the main processing time of converting raw to refined data aggregation will take substantial time (~ 4 to 6 hrs), which is fine to run overnight before the next harvesting process instantiates. 

 
### References
1. OECD. (2021, Feb. 22nd). COVID-19 and a new resilient infrastructure landscape. *Organisation for Economic Co-operation and  Development*. Available: https://read.oecd-ilibrary.org/view/?ref=1060_1060483-4roq9lf7eu&title=COVID-19-and-a-new-resilient-infrastructure-landscape, 2021.

2. Sneader, K. and Lund, S. (2020, Aug. 28th). COVID-19 and climate change expose dangers of unstable supply chains. *McKinsey Global Institute.* Available: https://www.mckinsey.com/business-functions/operations/our-insights/covid-19-and-climate-change-expose-dangers-of-unstable-supply-chains, 2020. 
