#!/usr/bin/env python3
#
# File specifying Airflow DAGs for ingesting Met Office weather data

import logging                  # Python Logging facility
from os import environ          # Dictionary of enviromental values
from datetime import datetime   

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator

from ingest_bike_data import format_to_parquet


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_previous_month(execution_date): 
    """
    Gets year and month of the previous month.

    :param execution_date: The current date, as a datetime object.

    :return: Returns a tuple of the previous month's;
      -  year (str), 
      - month (str), 
      - last day of the month (str).
    """
    from calendar import monthrange

    # Convert str to int and subtract a month
    year = int(execution_date.strftime('%Y'))
    month = int(execution_date.strftime('%m')) - 1

    # Case: First month of the year, i.e last month was the previous year
    if month == 0:
        year -= 1
        month = 12

    # Gets the number of days in the month
    month_end_date = str(monthrange(year, month)[1])

    # 0 pad the month value
    if month < 10:
        month = f"0{month}"

    return str(year), month, month_end_date


def get_previous_month_as_yyyymm(execution_date):
    """
    Gets year and month of the previous month (using `get_previous_month`).

    :param execution_date: The current date, as a datetime object.

    :return: A string containing the previous month's date in "YYYYMM" format.
    """

    year, month, _ = get_previous_month(execution_date)

    return f"{year}{month}"


def get_weather_file_names_from_time(weather_type, execution_date):
    """
    Gets the name of the dataset in the FTP using the date of the DAG run.

    :param weather_type: The name of the weather variable to fetch

    :param execution_date: The current date, as a datetime object.

    :return: The name of the file to be fetched from FTP
    """

    # Get data for the previous month as it has just been published
    year, month, month_end_date = get_previous_month(execution_date)

    # The files are split by month with the following naming convention
    return f"{weather_type}_hadukgrid_uk_1km_day_{year}{month}01-{year}{month}{month_end_date}.nc"


def get_ftp_dataset(host, location, out_file):
    """
    Gets a dataset from FTP. Assumes FTP_USER and FTP_PASS environment variables are defined.

    :param host: The host location of the FTP to connect to.

    :param location: The location of the required file within the FTP. 

    :param out_file: The directory in which the file was downloaded to.
    """

    # Use ftplib to connect and download files
    # Import here as we don't want to import at top of file for Airflow
    import ftplib

    # Open the connection to the host using environment login variables
    with ftplib.FTP(host) as ftp_conn:

        # Use environment variables to login
        ftp_conn.login(environ.get("FTP_USER"), environ.get("FTP_PASS"))

        # Download dataset and save as a binary file
        ftp_conn.retrbinary(f"RETR {location}", open(out_file, "wb").write)


def reformat_netcdf(file_name, weather_type):
    """
    Converts weather data, stored in netCDF4 format, to a CSV file.

    :param file_name: The file name of the .nc file to be converted.

    :param weather_type: The name of the weather variable (e.g. rainfall, tasmin, tasmax) to be extracted. 

    :return: The name of the created CSV file.
    """
    
    # Import here as we don't want to import at top of file for Airflow
    import numpy as np  
    import pandas as pd
    import netCDF4 as nc

    # Coordinates of the target grid reference "TQ 30000 80000"
    target_lat = 51.5  # Grid reference to Latitude
    target_long = -0.13  # Grid reference to Longitude

    # This was obtained in the previous task
    locations_df = pd.read_parquet(f"{AIRFLOW_HOME}/dim_locations.parquet")

    # netCDF dataset, main matrix has dimensions: {time, projection_y_coordinate, projection_x_coordinate}
    nc_data = nc.Dataset(file_name)

    pd_location_to_weather_datasets = []
    
    # Map netCDF times to dates (netCDF time is hours since the year 1800)
    dates = nc.num2date(nc_data.variables["time"][:], nc_data.variables["time"].units)

    # Calculate the closest point in the weather dataset to the target grid reference
    lat_diff = abs(target_lat - nc_data.variables["latitude"][:])
    long_diff = abs(target_long - nc_data.variables["longitude"][:])

    euclidean_dist = np.sqrt(lat_diff ** 2 + long_diff ** 2)
    
    # Get the measurements for each day for the target coordinates
    y_coord, x_coord = np.unravel_index(
        np.argmin(euclidean_dist), 
        nc_data.variables["latitude"][:].shape
        )

    location_measurements = nc_data.variables[weather_type][:, y_coord, x_coord]

    # Creating DataFrame with weather data for the target grid reference
    weather_data = pd.DataFrame({
        "time": dates,
        weather_type: location_measurements
    })

    # Write data to CSV
    out_csv_file_name = file_name.replace(".nc", ".csv")
    out_csv_file_name = f"{AIRFLOW_HOME}/{weather_type}_TQ3000080000.csv"
    weather_data.to_csv(out_csv_file_name, index=False)

    return out_csv_file_name


def create_weather_dag(weather_type):
    """
    Creates a DAG for extracting a weather variable from the CEDA Archive FTP server. The DAG runs on the 3rd day 
    of each month, and ingests the weather data for the previous month. The function can create a DAG for any of the
    weather variables, including: 
    rainfall: Precipitation volume in mm 
    tasmin: Minimum daily surface air temp
    tasmax: Maximum daily surface air temp 

    :param weather_type: The name of the weather variable (e.g. rainfall, tasmin, tasmax) to be extracted. 

    :return: A DAG for ingesting the weather data.
    """

    ingest_weather_data = DAG(
        dag_id = f"ingest_{weather_type}_weather",
        schedule_interval = "0 0 3 * *",            # Get previous month's data on the 3rd
        catchup = True,
        max_active_runs = 1,
        tags = ["weather_digest", weather_type],
        start_date = datetime(2020, 12, 21),        # DAG start date. Mon 21 Dec 2020
        end_date = datetime(2022, 1, 3),            # DAG end date, Mon 02 Jan 2023 
        default_args = {
            "owner": "airflow",                     # Owner of DAG for permisions and access
            "depends_on_past": True,                # Current DAG run reliant on past run
            "retries": 0                            # No retries allowed
        }
    )

    with ingest_weather_data:
        
        # This is where the daily weather data grid is located in the FTP server (https://data.ceda.ac.uk/)
        ftp_path = f"/badc/ukmo-hadobs/data/insitu/MOHC/HadOBS/HadUK-Grid/v1.2.0.ceda/25km/{weather_type}/day/v20230328/"

        # Get the date (in "YYYYMM" format) of the data
        get_previous_month = PythonOperator(
            task_id = "get_previous_month",
            python_callable = get_previous_month_as_yyyymm
        )

        data_date = "{{ ti.xcom_pull(task_ids='get_previous_month') }}"
        logging.info(f"Data date: {data_date}")

        # Get the name of the file using the date of the DAG run
        get_file_names = PythonOperator(
            task_id = "get_file_names",
            python_callable = get_weather_file_names_from_time,
            op_kwargs = {"weather_type": weather_type}
        )

        ftp_file_name = "{{ ti.xcom_pull(task_ids='get_file_names') }}"
        logging.info(f"Pulled FTP file name: {ftp_file_name}")

        # The live dataset is downloaded as an XML
        # We only extract the static data concerning the bike pickup/dropoff locations
        download_file_from_ftp = PythonOperator(
            task_id = "download_file_from_ftp",
            python_callable = get_ftp_dataset,
            op_kwargs = {
                "host": "ftp.ceda.ac.uk",
                "location": f"{ftp_path}/{ftp_file_name}",
                "out_file": f"{AIRFLOW_HOME}/{ftp_file_name}"
            }
        )

        get_locations_dim_from_gcs = GCSToLocalFilesystemOperator(
            task_id = "get_locations_dim_from_gcs",
            bucket = GCP_GCS_BUCKET,
            object_name = "locations_data/livecyclehireupdates.parquet",
            filename = f"{AIRFLOW_HOME}/dim_locations.parquet"
        )

        # Extract the relevant parts of the dataset into a CSV
        ingest_data_to_csv = PythonOperator(
            task_id = "ingest_data_to_csv",
            python_callable = reformat_netcdf,
            op_kwargs = {
                "file_name": ftp_file_name, 
                "weather_type": weather_type
            }
        )

        # Get the new dataset name after conversion to CSV
        csv_file_name = "{{ ti.xcom_pull(task_ids='ingest_data_to_csv') }}"
        logging.info(f"Pulled CSV name: {csv_file_name}")

        get_file_names >> download_file_from_ftp >> ingest_data_to_csv 
        get_locations_dim_from_gcs >> ingest_data_to_csv

        # We convert to the columnar parquet format for upload to GCS
        convert_to_parquet = PythonOperator(
            task_id = "convert_to_parquet",
            python_callable = format_to_parquet,
            op_kwargs = {
                "csv_file_dir": AIRFLOW_HOME,
                "csv_file_name": csv_file_name
            }
        )

        # Get the new dataset name after conversion to parquet
        parquet_file_name = "{{ ti.xcom_pull(task_ids='convert_to_parquet') }}"
        logging.info(f"Pulled parquet name: {parquet_file_name}")

        # The local data is transferred to the GCS 
        # The data is stored in separate folders for each weather type
        # The data is also separated by subfolders by month
        transfer_data_to_gcs = LocalFilesystemToGCSOperator(
            task_id = "transfer_data_to_gcs",
            src = f"{AIRFLOW_HOME}/{parquet_file_name}",
            dst = f"weather_data/{weather_type}/{data_date}/{parquet_file_name}",
            bucket = GCP_GCS_BUCKET
        )

        ingest_data_to_csv >> convert_to_parquet >> transfer_data_to_gcs
        get_previous_month >> transfer_data_to_gcs

    return ingest_weather_data


# Create the DAG for each weather variable
rainfall_dag = create_weather_dag("rainfall")
tasmax_dag = create_weather_dag("tasmax")
tasmin_dag = create_weather_dag("tasmin")
