# Databricks notebook source
# MAGIC %md ### Define an external location to search for.
# MAGIC
# MAGIC Leave as `external_location = ""` to search for all external assets.

# COMMAND ----------

external_location = "abfss

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All Catalogs with a Default Location that contains the External Location
# MAGIC This gets all Managed Catalogs that have `external_location` as a part of their root path.
# MAGIC
# MAGIC To run this function succesfully, the user running the function needs to have `USE CATALOG` permissions in UC for the  being checked, and `SELECT` permissions to the `system.information_schema.catalogs`.
# MAGIC
# MAGIC #### Examples
# MAGIC
# MAGIC ```
# MAGIC external_location = "abfss"
# MAGIC get_managed_catalogs(external_location)
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

from external_locations_helper.catalogs import get_managed_catalogs

external_location = "abfss"
managed_catalogs_df = get_managed_catalogs(external_location)
display(managed_catalogs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get All Schemas with a Default Location
# MAGIC This gets all Managed Schemas that have `external_location` as a part of their root path.
# MAGIC
# MAGIC To run this function succesfully, the user running the function needs to have `SELECT` permissions in UC for the Schemas being checked, and `SELECT` permissions to the `system.information_schema.schemata`.
# MAGIC
# MAGIC #### Examples
# MAGIC
# MAGIC __To check only `Managed` Schemas in a given Catalog:__
# MAGIC
# MAGIC ```
# MAGIC get_managed_schemas(external_loc=external_location, catalog="gshen_catalog")
# MAGIC ```
# MAGIC
# MAGIC __To check for `Managed` Schemas across all catalogs:__
# MAGIC
# MAGIC ```
# MAGIC get_managed_schemas(external_loc=external_location)
# MAGIC ```
# MAGIC </br>
# MAGIC
# MAGIC _If you do not have access to `system.information_schema.schemata` you can use the `system_table` parameter to switch to a `information_schema` the user does have access to:_
# MAGIC
# MAGIC </br>
# MAGIC
# MAGIC ```
# MAGIC get_table_paths(external_loc=external_location, catalog="gshen_catalog",system_table = "gshen_catalog.information_schema.schemata")
# MAGIC ```

# COMMAND ----------

from external_locations_helper.schemas import get_managed_schemas

external_location = "abfss"
managed_schemas_df = get_managed_schemas(
    external_loc=external_location
)
display(managed_schemas_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get Managed and External Tables Locations
# MAGIC This gets all Managed and External tables that have `external_location` as a part of their root path.
# MAGIC
# MAGIC To run this function succesfully, the user running the function needs to have `SELECT` permissions in UC for the tables being checked, and `SELECT` permissions to the `system.information_schema.tables`.
# MAGIC
# MAGIC #### Examples
# MAGIC
# MAGIC __To check only `External` Tables in a given catalog:__
# MAGIC
# MAGIC ```
# MAGIC get_table_paths(external_loc=external_location, catalog="gshen_catalog", table_type="EXTERNAL")
# MAGIC ```
# MAGIC
# MAGIC __To check only `Managed` Tables in a given catalog and schema:__
# MAGIC
# MAGIC ```
# MAGIC get_table_paths(external_loc=external_location, catalog="gshen_catalog", schema ="data_blending", table_type="MANAGED")
# MAGIC ```
# MAGIC
# MAGIC __To check both `External` and `Managed` Tables across all catalogs and schemas :__
# MAGIC
# MAGIC ```
# MAGIC get_table_paths(external_loc=external_location)
# MAGIC ```
# MAGIC </br>
# MAGIC If you do not have access to `system.information_schema.tables`, you can use the `system_table` parameter to switch to a `information_schema` the user does have access to:
# MAGIC
# MAGIC ```
# MAGIC get_table_paths(external_loc=external_location, catalog="gshen_catalog",system_table = "gshen_catalog.information_schema.tables")
# MAGIC ```

# COMMAND ----------

from external_locations_helper.tables import get_table_paths

external_location = "abfss://"
table_paths = get_table_paths(external_loc=external_location, catalog="gshen_catalog")
display(table_paths)

# COMMAND ----------


