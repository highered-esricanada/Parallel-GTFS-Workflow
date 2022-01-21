![dashboard](/img/dashboard_transit.JPG)
# Parallel GTFS Workflow
A replicable workflow (i.e., data pipeline) that parallel processes GTFS (real-time + static) data to calculate various transit metrics using Geographic Information Systems (GIS) and data engineering techniques. Transit metrics from the workflow can be injected in a Web GIS dashboard to view spatiotemporal variations of transit reliability. 

**PLEASE NOTE** - The original code is undergoing modifications (e.g., better documentation) and will be published in increments over time - the entire repo is projected to be complete by Spring 2022.

## Motivation
COVID-19 has revealed the entire world of how fragile our essential infrastructure is and has reminded us that system adaptation and resilience are crucial (OECD, 2021; Sneader and Lund, 2020). Essential infrastructure is defined as basic necessities that are crucial to the well-being and convenience of the general public. These include basic hygience, healthcare, emergency personnel, clean water, basic hygiene, fresh groceries, energy, communications, and transportation. All of these elements are interlinked and maintains the global economy and modern life. In the wake of rapid population growth, especially mass migrations to cities, and climate change/chaos, the need for resilience and adaptability is a must. Therefore, this research and development project presents a replicable workflow that intends to cover both of these needs in the context of urban public transit services.

## Software Requirements
1) Python 3.x run by conda environment in ArcGIS 
   - Cloned env: If cloned conda environment has not been created, a **bat** file (for Windows) and **bash** file (for Linux) is provided to automate this process.
   - Packages: Comes with **requirements.txt** to download missing packages automatically to your cloned environment. 
2) ArcGIS License to use ArcPy and by extension the ArcGIS API for Python
3) [MongoDB installed](https://docs.mongodb.com/manual/installation/) 


 
### References
1. OECD. (2021, Feb. 22nd). COVID-19 and a new resilient infrastructure landscape. *Organisation for Economic Co-operation and  Development*. Available: https://read.oecd-ilibrary.org/view/?ref=1060_1060483-4roq9lf7eu&title=COVID-19-and-a-new-resilient-infrastructure-landscape, 2021.

2. Sneader, K. and Lund, S. (2020, Aug. 28th). COVID-19 and climate change expose dangers of unstable supply chains. *McKinsey Global Institute.* Available: https://www.mckinsey.com/business-functions/operations/our-insights/covid-19-and-climate-change-expose-dangers-of-unstable-supply-chains, 2020. 
