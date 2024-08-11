from tableauserverclient import PersonalAccessTokenAuth, Server, ServerResponseError
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode, HyperException
import snowflake.connector
import yaml
from dotenv import load_dotenv
import zipfile
import pathlib
import os
from typing import List, Dict
from multiprocessing import Pool


def get_configs() -> Dict:
    """
    Load configuration settings from the config.yml file.

    This function reads the configuration file and returns a dictionary containing
    the settings needed for the script, such as target schema names and table prefixes.

    Returns:
        Dict: A dictionary containing configuration settings extracted from config.yml.

    Raises:
        FileNotFoundError: If the config.yml file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    try:
        with open('config.yml', 'r') as file:
            config_yaml = yaml.safe_load(file)
        return config_yaml['config']
    except FileNotFoundError:
        raise FileNotFoundError("config.yml file not found. Please ensure the file is present.")
    except yaml.YAMLError as e:
        raise yaml.YAMLError(f"Error parsing config.yml: {e}")


def download_data_source_file() -> List[Dict]:
    """
    Download Tableau data sources from a specific project.

    This function authenticates with Tableau using a personal access token and downloads
    data sources from the "Admin Insights" project. The downloaded files are saved locally,
    and information about each file is returned in a list of dictionaries.

    Returns:
        List[Dict]: A list of dictionaries, each containing:
            - datasource_id (str): The ID of the downloaded data source.
            - datasource_name (str): The name of the downloaded data source.
            - tdsx_file (str): The file path of the downloaded .tdsx file.

    Raises:
        ServerResponseError: If there is an error with the Tableau server response.
        RuntimeError: If the authentication or download process fails.
    """
    tdsx_ls = []

    try:
        tableau_auth = PersonalAccessTokenAuth(
            token_name=os.environ.get("PAT_NAME"),
            personal_access_token=os.environ.get("PAT_SECRET"),
            site_id=os.environ.get("TABLEAU_CLOUD_SITE")
        )
        server = Server(os.environ.get("TABLEAU_CLOUD_URI"), use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            ds_ls = list(server.datasources.filter(project_name="Admin Insights"))

            for ds in ds_ls:
                file_path = server.datasources.download(ds.id, include_extract=True)
                tdsx_ls.append({"datasource_id": ds.id, "datasource_name": ds.name, "tdsx_file": file_path})
                print(f"{ds.name} Tableau Data Source downloaded.")
    except ServerResponseError as e:
        raise RuntimeError(f"Failed to interact with Tableau Server: {e}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while downloading Tableau data sources: {e}")

    return tdsx_ls


def extract_hyper_files(tdsx_file_path: str) -> str:
    """
    Extract the .hyper file from a Tableau .tdsx file.

    This function extracts the .hyper file from a .tdsx file (which is essentially a
    zip archive) and saves it in the current working directory.

    Args:
        tdsx_file_path (str): The file path to the .tdsx file.

    Returns:
        str: The file path to the extracted .hyper file.

    Raises:
        FileNotFoundError: If the .tdsx file does not contain a .hyper file.
        zipfile.BadZipFile: If the .tdsx file is not a valid zip archive.
        Exception: For other errors related to file operations.
    """
    try:
        target_dir = pathlib.Path.cwd()
        tdsx_file_name = pathlib.Path(tdsx_file_path).name
        target_path = pathlib.Path.joinpath(target_dir, tdsx_file_name.replace(".tdsx", ".hyper"))

        hyper_files = []
        with zipfile.ZipFile(tdsx_file_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith('.hyper'):

                    # Extract the .hyper file to the current directory
                    with zip_ref.open(file) as source_file:
                        with open(target_path, 'wb') as dest_file:
                            dest_file.write(source_file.read())
                            hyper_files.append(dest_file.name)

        if not hyper_files:
            raise FileNotFoundError("No .hyper file found in the .tdsx archive.")

        print(f"Extracted hyper files from {tdsx_file_name}")
        os.remove(tdsx_file_path)  # Clean up the .tdsx file

        return hyper_files[0]
    except zipfile.BadZipFile:
        raise zipfile.BadZipFile(f"{tdsx_file_path} is not a valid .tdsx file or is corrupted.")
    except Exception as e:
        raise Exception(f"An error occurred while extracting .hyper files: {e}")


def export_hyper_to_parquet(hyper_file: str, parquet_file: str) -> None:
    """
    Export data from a .hyper file to a .parquet file.

    This function reads data from the specified .hyper file and exports it
    into a .parquet file format, which can be later used for uploading to a data warehouse.

    Args:
        hyper_file (str): The path to the .hyper file to be exported.
        parquet_file (str): The path where the output .parquet file will be saved.

    Raises:
        HyperException: If an error occurs during the Hyper process.
        RuntimeError: If there are issues during file operations.
    """
    try:
        process_parameters = {"log_config": ""}
        with HyperProcess(
                telemetry=Telemetry.DO_NOT_SEND_USAGE_DATA_TO_TABLEAU,
                parameters=process_parameters
        ) as hyper:
            with Connection(
                    endpoint=hyper.endpoint,
                    database=hyper_file,
                    create_mode=CreateMode.NONE,
            ) as connection:
                hyper_sql = f"""copy "public"."Extract" to '{parquet_file}'"""
                connection.execute_command(hyper_sql)

        os.remove(hyper_file)  # Clean up the .hyper file
    except HyperException as e:
        raise HyperException(f"An error occurred with the Hyper process: {e}")
    except Exception as e:
        raise RuntimeError(f"An error occurred while exporting .hyper to .parquet: {e}")


def pq_to_snowflake(target_table: str, target_schema: str, parquet_file: str) -> None:
    """
    Upload data from a .parquet file to a Snowflake table.

    This function uploads the data stored in a .parquet file to a specified Snowflake table.
    It creates necessary schemas, stages, and formats before performing the copy operation.

    Args:
        target_table (str): The name of the target table in Snowflake where data will be loaded.
        target_schema (str): The schema in Snowflake under which the table resides.
        parquet_file (str): The path to the .parquet file to be uploaded.

    Raises:
        snowflake.connector.errors.DatabaseError: If the Snowflake operation fails.
        FileNotFoundError: If the .parquet file does not exist.
    """
    try:
        if not pathlib.Path(parquet_file).exists():
            raise FileNotFoundError(f"Parquet file {parquet_file} does not exist.")

        file_format = f"{target_table}_parquet_fmt"
        stage = f"stage_{target_schema.lower()}_{target_table}"

        print(f"Copying {target_table} parquet file to Snowflake.")
        with snowflake.connector.connect(
                account=os.environ.get("SNOWFLAKE_ACCOUNT"),
                warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE"),
                database=os.environ.get("SNOWFLAKE_EBI_DEV_DATABASE"),
                role=os.environ.get("SNOWFLAKE_ROLE"),
                schema=target_schema,
                user=os.environ.get("SNOWFLAKE_USER"),
                password=os.environ.get("SNOWFLAKE_PASSWORD"),
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {target_schema};")
                cur.execute(
                    f"""
                    CREATE OR REPLACE TEMPORARY FILE FORMAT {file_format}
                    TYPE = PARQUET
                    USE_LOGICAL_TYPE = TRUE
                    BINARY_AS_TEXT = FALSE;
                    """
                )
                cur.execute(f"CREATE OR REPLACE TEMPORARY STAGE {target_schema}.{stage} FILE_FORMAT = {file_format};")
                cur.execute(f"PUT file://{parquet_file} @{stage}")

                target_schema_table = f"{target_schema}.{target_table}"
                print(f"Creating structured data table: {target_schema_table}.")
                cur.execute(f"DROP TABLE IF EXISTS {target_schema_table};")
                cur.execute(
                    f"""
                    CREATE OR REPLACE TRANSIENT TABLE {target_schema_table} USING TEMPLATE (
                        SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                        FROM TABLE(
                            INFER_SCHEMA(
                                LOCATION=>'@{stage}', FILE_FORMAT=>'{file_format}', IGNORE_CASE => TRUE
                            )
                        )
                    );
                    """
                )
                print(f"Copying {target_table} parquet data to {target_schema_table}.")
                cur.execute(
                    f"""
                    COPY INTO {target_schema_table}
                    FROM @{stage} FILE_FORMAT = {file_format}
                    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                    PURGE=TRUE;
                    """
                )

        print(f"Removing {target_table} parquet file.")
        os.remove(parquet_file)
    except snowflake.connector.errors.DatabaseError as e:
        raise snowflake.connector.errors.DatabaseError(f"An error occurred while uploading data to Snowflake: {e}")
    except Exception as e:
        raise RuntimeError(f"An error occurred during the Snowflake operation: {e}")


def load_data_source(tdsx_data: Dict) -> None:
    """
    Orchestrate the process of handling a single Tableau data source.

    This function manages the end-to-end process of extracting data from a Tableau .tdsx file,
    converting it to .parquet format, and uploading it to a Snowflake table.

    Args:
        tdsx_data (Dict): A dictionary containing details about the downloaded .tdsx file,
        including its path, name, and data source ID.

    Raises:
        Exception: If any step in the process fails.
    """
    try:
        configs = get_configs()
        table_prefix = configs.get("target_table_prefix")
        target_schema_name = configs.get("target_schema")

        ds_name = tdsx_data.get("datasource_name").lower().replace(" ", "_")
        hyper_file_name = extract_hyper_files(tdsx_file_path=tdsx_data.get("tdsx_file"))
        new_parquet_file = f"{ds_name}.parquet"
        export_hyper_to_parquet(hyper_file=hyper_file_name, parquet_file=new_parquet_file)

        if table_prefix:
            target_table_name = f"{table_prefix}_{ds_name}"
        else:
            target_table_name = ds_name

        # Make database object names uppercase so any logged or printed names match final database objects
        target_table_name = target_table_name.upper()
        target_schema_name = target_schema_name.upper()

        # Upload the parquet file to Snowflake
        pq_to_snowflake(target_table=target_table_name, target_schema=target_schema_name, parquet_file=new_parquet_file)
    except Exception as e:
        raise RuntimeError(
            f"An error occurred while processing the data source {tdsx_data.get('datasource_name')}: {e}")


if __name__ == "__main__":
    try:
        load_dotenv()  # Load environment variables from .env file

        # Download Tableau data sources
        tdsx_files = download_data_source_file()

        # Process each data source using multiprocessing
        with Pool(5) as p:
            p.map(load_data_source, tdsx_files)
    except Exception as e:
        print(f"An error occurred during the main execution: {e}")
