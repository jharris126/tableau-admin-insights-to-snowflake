# Tableau Cloud Admin Insights Data to Snowflake

This repo show an example of how to
leverage `tableauserverclient` and `tableauhyperapi`
in an ETL job that loads the data from the
Tableau Cloud Admin Insights data sources into
Snowflake tables where they can be enhanced
with other relevant data from your organization.

---

## Setting Environment Variables

To set environment variables for this repo,
create a `.env` file and populate the below
variables or otherwise set environment variables
with the below names.

```dotenv
# Tableau Cloud Login
TABLEAU_CLOUD_URI=
TABLEAU_CLOUD_SITE=
PAT_NAME=
PAT_SECRET=

# Snowflake Login
SNOWFLAKE_ACCOUNT=
SNOWFLAKE_WAREHOUSE=
SNOWFLAKE_EBI_DEV_DATABASE=
SNOWFLAKE_ROLE=
SNOWFLAKE_USER=
SNOWFLAKE_PASSWORD=
```

## Optional Configurations

You can optionally choose a different target
Snowflake schema than `TABLEAU_CLOUD` and/or
add a prefix to the table when it is
written to Snowflake by editing the properties
in `config.yml`. Default values are show below.

```yaml
config:
  target_schema: TABLEAU_CLOUD
  target_table_prefix: null
```

## ETL Strategy
Currently, this repo uses a daily full drop and
recreate ETL pattern which is not typically a
best practice, especially as the size and usage
of your Tableau Cloud environment grows. Below,
is a high level overview of the steps taken.

1. Use `tableauserverclient` python library
   to search the Tableau-generated "Admin Insights"
   project for all Published Data Sources
   and download each to the current working
   directory.
2. Open each `.tdsx` zip archive and extract
   the `.hyper` file in each data source to the
   current working directory
3. Use `tableauhyperapi` to open each .`hyper` file
   and use the [COPY TO](https://tableau.github.io/hyper-db/docs/sql/command/copy_to)
   SQL command to export Hyper data to a `.parquet` file.
4. Bulk copy parquet files to Snowflake tables. First,
   using the [PUT](https://docs.snowflake.com/en/sql-reference/sql/put)
   command to load files to a
   temporary [internal stage](https://docs.snowflake.com/en/user-guide/data-load-local-file-system-create-stage)
   then use [schema inference](https://docs.snowflake.com/en/sql-reference/functions/infer_schema)
   to unload to a
   [TRANSIENT TABLE](https://docs.snowflake.com/en/user-guide/tables-temp-transient#transient-tables)
   in Snowflake.

## Future Goals
1. More informative `README.md`
2. Look into incremental load strategies
