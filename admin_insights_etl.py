from tableauserverclient import PersonalAccessTokenAuth, Server
from tableauhyperapi import HyperProcess, Connection, Telemetry, CreateMode
import snowflake.connector
import yaml
from dotenv import load_dotenv
import zipfile
import pathlib
import os
from typing import List, Dict


def get_configs() -> Dict:
    """
    Load and return configuration settings from the config.yml file.

    Returns:
        Dict: A dictionary containing configuration settings.
    """
    with open('config.yml', 'r') as file:
        config_yaml = yaml.safe_load(file)

    return config_yaml['config']


def download_data_source_file() -> List[Dict]:
    """
    Download Tableau data sources from the specified project and return information about each downloaded file.

    Returns:
        List[Dict]: A list of dictionaries containing data source ID, name, and the path to the downloaded file.
    """
    tdsx_ls = []

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

    return tdsx_ls


def extract_hyper_files(tdsx_file_path: str) -> str:
    """
    Extract .hyper files from a Tableau .tdsx file.

    Args:
        tdsx_file_path (str): The path to the .tdsx file.

    Returns:
        str: The path to the extracted .hyper file.
    """
    target_dir = pathlib.Path.cwd()
    tdsx_file_name = pathlib.Path(tdsx_file_path).name

    hyper_files = []
    with zipfile.ZipFile(tdsx_file_path, 'r') as zip_ref:
        for file in zip_ref.namelist():
            if file.endswith('.hyper'):
                target_path = os.path.join(target_dir, os.path.basename(file))

                # Extract the .hyper file to the current directory
                with zip_ref.open(file) as source_file:
                    with open(target_path, 'wb') as dest_file:
                        dest_file.write(source_file.read())
                        hyper_files.append(dest_file.name)

    print(f"Extracted hyper files from {tdsx_file_name}")

    os.remove(tdsx_file_path)  # Clean up the .tdsx file

    return hyper_files[0]


def export_hyper_to_parquet(hyper_file: str, parquet_file: str) -> None:
    """
    Export data from a .hyper file to a .parquet file.

    Args:
        hyper_file (str): The path to the .hyper file.
        parquet_file (str): The path to the output .parquet file.
    """
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


def pq_to_snowflake(target_table: str, target_schema: str, parquet_file: str) -> None:
    """
    Upload data from a .parquet file to a Snowflake table.

    Args:
        target_table (str): The name of the target table in Snowflake.
        target_schema (str): The name of the target schema in Snowflake.
        parquet_file (str): The path to the .parquet file.
    """
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


def main():
    """
    Main function to orchestrate the process of downloading Tableau data sources,
    extracting .hyper files, converting them to .parquet, and uploading the data to Snowflake.
    """
    load_dotenv()  # Load environment variables from .env file
    configs = get_configs()
    table_prefix = configs.get("target_table_prefix")
    target_schema_name = configs.get("target_schema")

    # Download Tableau data sources
    tdsx_files = download_data_source_file()

    for tdsx_data in tdsx_files:
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


if __name__ == "__main__":
    main()
