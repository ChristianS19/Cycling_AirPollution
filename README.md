Transport for London Santander Cycles Data Pipeline
===================================================
In partnership with Santander, Transport for London (TfL) operate a public cycle hire scheme in London. In 2018 there are over 11,000 bikes located across 700 docking stations. TfL make available a [unified API](https://tfl.gov.uk/info-for/open-data-users/unified-api) to facilitate the open sharing of data for many modes of transportation, including the bike journey data. They have also made available [a bucket](https://cycling.data.tfl.gov.uk/) containing historical data detailing each of the journeys undertaken since 2015.

This repository contains a batch processing pipeline that uses Google Cloud Platform (GCP) to extract the TfL bike data from multipole sources and combine it into a single database for analytics.
The primary dataset contains details for each journey, including start time/location and end time/location. 
The pipeline also intergrates cycle data with [weather data from the Met Office](https://catalogue.ceda.ac.uk/uuid/4dc8450d889a491ebb20e724debe2dfb) and [London Air Quality Data from TfL](https://www.londonair.org.uk/LondonAir/API/).

Weather observations (rainfall, maximum temperature and minimum temperature) and air quality (PM2.5) are interpolated onto a uniform grid (1km by 1km); the pipeline merges weather data over time for each cycle station using its nearest point on the grid.