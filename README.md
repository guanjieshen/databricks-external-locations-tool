# Databricks External Locations Tool
This repository enables users to search assets stored within Databricks Unity Catalog to see if those assets are stored within a specific location in cloud object storage. The assets supported include:

- Managed Catalogs
- Managed Schemas
- Managed Tables
- External Tables

__To export the storage locations for all assets, set `external_location = ""`__


### Get Catalogs with a Default Location that contain a specific External Location path
This gets all Managed Catalogs that have `external_location` as a part of their root path.

To run this function succesfully, the user running the function needs to have `USE CATALOG` permissions in UC for the catalogs being checked, and `SELECT` permissions to the `system.information_schema.catalogs`.

#### Examples

```
external_location = "abfss"
get_managed_catalogs(external_location)
```

### Get Schemas with a Default Location that contain a specific External Location path
This gets all Managed Schemas that have `external_location` as a part of their root path.

To run this function succesfully, the user running the function needs to have `USE SCHEMA` permissions in UC for the schemas being checked, and `SELECT` permissions to the `system.information_schema.schemata`.

#### Examples

__To check `Managed` Schemas in a given Catalog:__

```
get_managed_schemas(external_loc=external_location, catalog="gshen_catalog")
```

__To check `Managed` Schemas across all catalogs:__

```
get_managed_schemas(external_loc=external_location)
```
</br>

_If you do not have access to `system.information_schema.schemata` you can use the `system_table` parameter to switch to a `information_schema` the user does have access to:_

</br>

```
get_table_paths(external_loc=external_location, catalog="gshen_catalog",system_table = "gshen_catalog.information_schema.schemata")
```

### Get Managed and External Tables that contains a specific External Location path
This gets all Managed and External tables that have `external_location` as a part of their root path.

To run this function succesfully, the user running the function needs to have `SELECT` permissions in UC for the tables being checked, and `SELECT` permissions to the `system.information_schema.tables`.

#### Examples

__To check `External` Tables in a given catalog:__

```
get_table_paths(external_loc=external_location, catalog="gshen_catalog", table_type="EXTERNAL")
```

__To check `Managed` Tables in a given catalog and schema:__

```
get_table_paths(external_loc=external_location, catalog="gshen_catalog", schema ="data_blending", table_type="MANAGED")
```

__To check `External` and `Managed` Tables across all catalogs and schemas :__

```
get_table_paths(external_loc=external_location)
```
</br>
If you do not have access to `system.information_schema.tables`, you can use the `system_table` parameter to switch to a `information_schema` the user does have access to:

```
get_table_paths(external_loc=external_location, catalog="gshen_catalog",system_table = "gshen_catalog.information_schema.tables")
```
