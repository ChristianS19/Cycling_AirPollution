Transport for London Santander Cycles Data Pipeline
===================================================
Transport for London (TfL) operate a public cycle hire scheme in London. In 2018 there are over 11,000 bikes located across 700 docking stations. TfL make available a [unified API](https://tfl.gov.uk/info-for/open-data-users/unified-api) to facilitate the open sharing of data for many modes of transportation, including the bike journey data. They have also made available [a bucket](https://cycling.data.tfl.gov.uk/) containing historical data detailing each of the journeys undertaken since 2015.

This repository contains a batch processing pipeline that uses Google Cloud Platform (GCP) to extract the TfL bike data, along with London weather, temperature, and air quality data from multiple sources and combine it into a single database for analytics.
The primary dataset contains details for each journey, including start time/location and end time/location. 
The pipeline also intergrates cycle data with [weather data from the Met Office](https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb) and [London Air Quality Data from TfL](https://www.londonair.org.uk/LondonAir/API/).

Weather observations (rainfall, maximum temperature and minimum temperature) and air quality (PM2.5) are taken as for the entire city. Grid location used for weather and air quality is taken as TQ 30000 80000, which roughly correlates to the AQ monitoring site of "Waterloo Place (The Crown Estate) - Roadside monitoring", which has been in operation since 21 Dec 2020.

Data range is taken as from 21 Dec 2020 until 02 Jan 2023.

# Overview of the pipeline

INGESTION (XML+CSV in Google Cloud Storage) > TRANSFORMATION (Apache Spark + Google BigQuery) > VISUALISATION (Data Studio)
SCHEDULING (Docker + Airflow)
INFRASTRUCTURE (Google Cloud + Terraform)

The technologies in the diagram are also listed below. The pipeline could have been simplified, however I wished to gain exposure to a set of new technologies and GCP.
- __GCP:__ Several google cloud services were used, including GCS, BigQuery, and Data Studio. 
- __Terraform:__ Terraform was used to manage GCP resources.
- __Docker:__ Docker was used to host Airflow in a VM. 
- __Airflow:__ Airflow was used to orchestrate the data ingestion steps and the submission of Spark jobs to GCP.
- __Spark:__ Spark was used to transform and integrate the locations, cycle and weather data, before loading them to BigQuery.

## Weather & Air Quality data

London wide temperatures, rainfall, and Air Quality data is collected for hourly averages. Due to the small location, low ride distances, and short duration of the majority of ride hires, it can be expected that there would not be significant differences between the start and end of trips, or across different ride hubs.



# Challanges & Limitations

The pipeline solves a number of challenges. For instance, while TfL's unified API is consistent, the data format and properties for the historical cycling data is not. The cycling data processed by the pipeline includes CSV, XML and XLS files. Furthermore, the weather data was stored in NetCDF format, which needed to be transformed to a compatible data type and integrated with the cycles data using latitude and longitude. Additionally, while the cycling data is released weekly, the weather data is released monthly, so the data is ingested separately before being combined into the final database at regular intervals.

Development of this project was restricted by the length of the GCP free trial.

## Documentation

Two forms of documentation are provided:
- Scripts, within the scripts are docstrings for python functions and other comments on purpose or method of code. 
- `docs/`, Markdown documents provide further and more in depth discussion on the pipeline, data, run guides, etc.

## Logging


## Quality checks and testing


## Complexity

The pipeline is more complex than needed considering the problem being solved. This is due to a desire to practice GCS, Airflow, and Spark.

An alternative approach could have bypassed cloud storage and utilised PostgreSQL instead of BigQuery.

## Dashboards

The intent of this project was not to generate comprehensive data analysis, and as such some simple simple proof of use dashboards were created using Data Studio.

Dashboards chosen (visualised below) show information regarding

. 
The journeys can be filtered by date, start, and end stations, with the number of journeys, and corresponding weather and air quality ratings displayed.

## Database

Due to the data being collected monthly, database tables (journey, Weather, Air Quality) are partitioned by monthly ranges to improve performance time and reduce query costs.

## Scheduling

The pipeline uses a set of directed acyclic graphs (DAGs), defined in [`airflow/dags`](airflow/dags/), to schedule the weekly retrieval of cycle journey data and monthly ingestion of weather data. 

# Pipeline documentation

A detailed explanation of the pipeline, along with instructions for repeated setup, are provided:

1. [Setup]()
2. [Data ingestion]()
3. [Data transformation]()
4. [Data visualisation]()
5. [Datasets]()

# License

The data processing pipeline operates under the GNU General Public License v3.0. Refer to the [LICENSE file](LICENSE) for detailed licensing information.

Licencing for Bike, Weather, and Air Quality data.