# Databricks notebook source
# MAGIC %md ### Define an external location to search for

# COMMAND ----------

# external_location = "abfss://files@oneenvadls.dfs.core.windows.net"
external_location = "abfss"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All Catalogs with a Default Location
# MAGIC This gets all catalogs that have the `external_location` as a part of their root path.

# COMMAND ----------

from external_locations_helper.catalogs import get_managed_catalogs

managed_catalogs_df = get_managed_catalogs(external_location)
display(managed_catalogs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All Schemas with a Default Location
# MAGIC This gets all schemas that have the `external_location` as a part of their root path.

# COMMAND ----------

from external_locations_helper.schemas import get_managed_schemas

managed_schemas_df = get_managed_schemas(
    catalog="gshen_external_catalog", external_loc=external_location
)
display(managed_schemas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All External Tables
# MAGIC This gets all external locations that have `external_location` as a part of their root path.

# COMMAND ----------

from external_locations_helper.tables import get_external_tables

# catalog and schema are optional, if left blank it will search across all catalogs and schemas
external_tables_df = get_external_tables(external_loc=external_location, catalog="main", schema="abedb")
display(external_tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All Managed Tables
# MAGIC This gets all Managed locations that have `external_location` as a part of their root path.

# COMMAND ----------


