#!/usr/bin/env python3
# File specifying Airflow DAGs for ingesting TfL cycling data

import logging
from os import environ
from json import load
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator


# Get environment variables from the docker container pointing to the GCS project and data stores
GCP_PROJECT_ID = environ.get("GCP_PROJECT_ID")
GCP_GCS_BUCKET = environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = environ.get("BIGQUERY_DATASET", "bikes_data_warehouse")

# Local folder within the docker container
AIRFLOW_HOME = environ.get("AIRFLOW_HOME", "/opt/airflow/")


def get_start_date_data_range(execution_date):
    """
    Gets the start date of the extracted data range. 

    :param execution_date: Date of scheduled airflow run. A datetime object is 
        automatically passed to this argument by airflow

    :return: Date in "YYYYMM" format.
    """

    return (execution_date - timedelta(days=6)).strftime("%Y%m")


def get_file_names_from_time(execution_date):
    """
    Each (weekly) usage data file in the source bucket has a complex naming pattern including:
        - An ID incrementing with each week, e.g 195JourneyDataExtract01Jan2020-07Jan2020.csv
        - The start date of the file, e.g. 04Jan2020.xls 
        - The end date of the file, e.g. 10Jan2020.xls
        - The day of the week, e.g. Monday, Jul 30 2018.xls
    Some datasets have inconsistent names, duplication of ID's, and may not be CSV, rather XLS files instead.
    However, for files in the time period we will look at (2022 - 2023), they follow the ID increment format and are all CSV.

    Function generates the file name based on a consistent naming pattern for usage data files.
    :param execution_date: Date of scheduled airflow run. A datetime object is automatically passed to this argument by airflow
    
    :return: The formatted file name for the airflow run's time period as a string and the dataset start date in "YYYYMM" format.
    """
    # 2020 IDs start at 195 - this is added to the number of weeks since the first data chunk
    dataset_id = 195 + ((execution_date.date() - date(2020, 12, 21)).days // 7)

    # Two weeks in a row have the same ID, so increase by one after this point
    if execution_date.date() >= date(2021, 1, 5):
        dataset_id += 1

    # Generate the start and end ranges of the data
    dataset_start_range = (execution_date - timedelta(days=6)).strftime("%d%b%Y")
    dataset_end_range = execution_date.strftime("%d%b%Y")

    logging.info(f"Will extract data for the time period {dataset_start_range} to {dataset_end_range}")

    # Assuming a consistent naming convention
    file_ext = ".csv"  # Assuming all files have the CSV extension

    # Generate the properly formatted dataset name using the consistent naming pattern
    formatted_dataset_name = f"{dataset_id}JourneyDataExtract{dataset_start_range}-{dataset_end_range}{file_ext}"
    logging.info(f"Formatted dataset name: {formatted_dataset_name}")

    return formatted_dataset_name


def reformat_locations_xml(xml_file_dir, xml_file_name):
    """
    Converts a live XML corresponding to bike locations and extracts relevant data fields.

    :param xml_file_dir: The directory in which the XML file to be processed is stored.

    :param xml_file_name: The name of the XML file to be processed.  

    :return: The name of the CSV file saved within `xml_file_dir`. 
    """

    if not xml_file_name.lower().endswith(".xml"):
        raise TypeError("Can only extract data from XML files")

    # Modules for converting xml to csv
    # Import here as we don't want to import at top of file for Airflow
    import csv
    from xml.etree import ElementTree

    # Get the root of the XML
    locations_tree = ElementTree.parse(f"{xml_file_dir}/{xml_file_name}")
    stations = locations_tree.getroot()

    # These are the variables we wish to extract
    station_vars = ["id", "name", "terminalName", "lat", "long"]

    # Create replacement CSV
    csv_file_name = xml_file_name.replace(".xml", ".csv")
    logging.info(f"CSV dataset name: {csv_file_name}")

    with open(f"{xml_file_dir}/{csv_file_name}", "w") as output_file:

        # Write header to CSV
        output_csv = csv.writer(output_file)
        output_csv.writerow(station_vars)
        
        # Loop through each node (station) and find each variable, write to CSV
        logging.info("Preview of XML to CSV conversion:")
        logging.info(station_vars)

        for i, station in enumerate(stations):

            csv_row = [station.find(station_var).text for station_var in station_vars]

            if i < 5:
                logging.info(csv_row)

            output_csv.writerow(csv_row)

    return csv_file_name


def format_to_parquet(csv_file_dir, csv_file_name, column_types=None):
    """
    Converts a CSV file to a parquet file. Parquet files are ideal because they allow for
    efficient data compression while allowing queries to read only the necessary columns.

    :param csv_file_dir: The directory in which the CSV file is stored.

    :param csv_file_name: The name of the CSV file to be converted.

    :param column_types: If given, specifies the schema of the parquet file
        to be written.

    :return: The name of the parquet file, saved within `csv_file_dir`.
    """

    if not csv_file_name.lower().endswith(".csv"):
        raise TypeError("Can only convert CSV files to parquet")

    # Import here as we don't want to import at top of file for Airflow
    from pyarrow.csv import read_csv, ConvertOptions
    from pyarrow.parquet import write_table

    # Convert CSV to parquet
    parquet_file_name = csv_file_name.replace(".csv", ".parquet")
    logging.info(f"Parquetised dataset name: {parquet_file_name}")

    # If given schema, give to pyarrow reader instead of inferring column types
    if column_types is not None:
        convert_options = ConvertOptions(column_types=column_types)
    else:
        convert_options = None

    arrow_table = read_csv(f"{csv_file_dir}/{csv_file_name}", convert_options=convert_options)
    
    write_table(arrow_table, f"{csv_file_dir}/{parquet_file_name}")

    return parquet_file_name


# Bike Data ingestion DAG
with DAG(
    dag_id = "ingest_bike_usage",           # DAG id name
    schedule_interval = "0 23 * * 3",       # Every week on Wednesday(3), 11pm
    catchup = True,                         # Will catchup on any intervals missed
    max_active_runs = 3,                    # Allows for 3 instances to run concurrently
    tags = ["tfl_digest", "bike_usage"],    # Descriptive tags
    start_date = datetime(2020, 12, 21, 0), # DAG start date, Wed 01 Jan 2020 at Midnight
    end_date = datetime(2023, 1, 2, 0),     # DAG end date, Mon 02 Jan 2023 at Midnight
    default_args = {
        "owner": "airflow",                 # Owner of DAG for permisions and access
        "depends_on_past": True,            # Current DAG run reliant on past run
        "retries": 0                        # No retries allowed
    }
    
) as ingest_bike_usage:
    """
    This DAG ingests the primary dataset from the TfL bucket. 
    Currently, it is specified to run from the start of 2020 until the start of 2023. 
    As the TfL data is released on Tuesdays, the DAG is schedualed to run weekly on Wednesdays in order to 
    get the recent data, then after clearning the file names, moves them to GCS.
    """

    # Generate the start date to save the output to the correct GCS folder
    get_start_date = PythonOperator(
        task_id = "get_start_date",
        python_callable = get_start_date_data_range
    )

    data_date = "{{ ti.xcom_pull(task_ids='get_start_date') }}"             # Airflow Task Interface {{}}
    logging.info(f"Start date of the file's range (YYYYMM): {data_date}")

    # Get the name of the file using the date of the DAG run
    get_file_names = PythonOperator(
        task_id = "get_file_names",
        python_callable = get_file_names_from_time
    )

    # Get the return value of the get_file_names task
    file_name = "{{ ti.xcom_pull(task_ids='get_file_names') }}"
    logging.info(f"Pulled csv name: {file_name}")

    # Download weekly data to local machine
    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf 'https://cycling.data.tfl.gov.uk/usage-stats/{file_name}' > {AIRFLOW_HOME}/{file_name}"
    )

    # Get the new dataset name after conversion to parquet
    csv_file_name = "{{ ti.xcom_pull(task_ids='convert_to_csv') }}"
    logging.info(f"Pulled CSV name: {csv_file_name}")

    # Remove spaces from file header
    new_header = "Rental_Id,Duration,Bike_Id,End_Date,EndStation_Id,EndStation_Name,Start_Date,StartStation_Id,StartStation_Name"

    convert_csv_header = BashOperator(
        task_id = "convert_csv_header",
        bash_command = f"""new_header='{new_header}'
                           sed -i "1s/.*/$new_header/" {AIRFLOW_HOME}/{csv_file_name}
                           head {AIRFLOW_HOME}/{csv_file_name}
                           tail {AIRFLOW_HOME}/{csv_file_name}
                        """
    )

    # Convert the CSV to parquet (columnar) format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_file_dir": AIRFLOW_HOME,
            "csv_file_name": csv_file_name,
            "column_types": load(open(f"{AIRFLOW_HOME}/schema/journey_schema.json", "r"))
        }
    )

    # Get the new dataset name after conversion to parquet
    parquet_file_name = "{{ ti.xcom_pull(task_ids='convert_to_parquet') }}"
    logging.info(f"Pulled parquet name: {parquet_file_name}")

    # The local data is transferred to the GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{parquet_file_name}",
        dst = f"rides_data/{data_date}/{parquet_file_name}",
        bucket = GCP_GCS_BUCKET
    )

    # Setup dependencies
    get_file_names >> download_file_from_https >> convert_csv_header  # For extracting the bike data
    convert_csv_header >> convert_to_parquet >> transfer_data_to_gcs  # For converting bike data and transfering
    get_start_date >> transfer_data_to_gcs                            # For transfering already downloaded or converted data


with DAG(
    dag_id = "ingest_bike_locations",           # DAG ID name
    schedule_interval = "0 0 1 * *",            # Runs 1st day of month, midnight, any day or month
    catchup = False,                            # Will not check for missed intervals
    max_active_runs = 1,                        # Will run one concurrently
    tags = ["tfl_digest", "bike_locations"],    # Descriptive tags
    start_date = datetime(2020, 12, 21),        # DAG start date. Wed 01 Jan, 2020 
    default_args = {
        "owner": "airflow",                     # Owner of DAG for permisions and access
        "depends_on_past": True,                # Current DAG run reliant on past run
        "retries": 0                            # No retries allowed
    }

) as ingest_bike_locations:
    """
    This DAG extracts static location information from a live dataset. 
    It is updated each month to catch any changes in available locations.
    """
    xml_file_name = "livecyclehireupdates.xml"  # Live Dataset file to ingest
    xml_file_url = "https://tfl.gov.uk/tfl/syndication/feeds/cycle-hire/"

    download_file_from_https = BashOperator(
        task_id = "download_file_from_https",
        bash_command = f"curl -sSLf '{xml_file_url}{xml_file_name}' > {AIRFLOW_HOME}/{xml_file_name}"
    )

    # Convert the XML file to CSV
    convert_from_xml = PythonOperator(
        task_id = "convert_from_xml",
        python_callable = reformat_locations_xml,
        op_kwargs = {
            "xml_file_dir": AIRFLOW_HOME,
            "xml_file_name": xml_file_name
        }
    )

    csv_file_name = xml_file_name.replace(".xml", ".csv")

    # Convert the CSV to parquet (columnar) format for upload to GCS
    convert_to_parquet = PythonOperator(
        task_id = "convert_to_parquet",
        python_callable = format_to_parquet,
        op_kwargs = {
            "csv_file_dir": AIRFLOW_HOME,
            "csv_file_name": csv_file_name
        }
    )

    parquet_file_name = csv_file_name.replace(".csv", ".parquet")

    # Transfer locatl data to GCS 
    transfer_data_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "transfer_data_to_gcs",
        src = f"{AIRFLOW_HOME}/{parquet_file_name}",
        dst = f"locations_data/{parquet_file_name}",
        bucket = GCP_GCS_BUCKET
    )
    

    # Setup Sequence / Dependencies for Airflow
    download_file_from_https >> convert_from_xml >> convert_to_parquet  >> transfer_data_to_gcs