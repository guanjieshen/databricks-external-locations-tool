from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("external_locations_helper_tables") \
    .getOrCreate()

def get_external_location_catalog(catalog: str) -> str:
    """
    Returns the storage location for a catalog.

    Args:
        schema_name (str):  Full schema path i.e. `catalog`

    Returns:
        str: Cloud storage location for the catalog
    """
    from pyspark.sql.functions import col
    try
        table_loc = (
            spark.sql(f"describe catalog extended `{catalog}`")
            .filter(col("info_name") == "Storage Root")
            .collect()
        )

        if table_loc:
            return table_loc[0]["info_value"]
    except Exception as e:
        print(f"Error Checking Catalog: {catalog}")
        return f"Error Checking Catalog: {catalog}"


def get_managed_catalogs(external_loc: str):
    """
    Checks to see which schemas root location contain a specific string.

    Args:
        external_loc (str): Filter tables that contain this str within their root location in cloud storage.
    Returns:
        DataFrame: DataFrame containing the catalog locations
    """
    catalogs = spark.sql(
        "SELECT catalog_name FROM system.information_schema.catalogs where catalog_owner != 'System user'"
    ).collect()

    catalog_arr = [catalog["catalog_name"] for catalog in catalogs]
    positive_catalogs = []
    for catalog in catalog_arr:
        print(f"Checking Catalog: {catalog}", flush=True)
        catalog_loc = get_external_location_catalog(catalog)
        if catalog_loc and external_loc in catalog_loc.lower():
            catalog_details = {"catalog_name": catalog, "path": catalog_loc}
            positive_catalogs.append(catalog_details)

    # Define the schema for the DataFrame
    schema = StructType([
        StructField('catalog_name', StringType(), nullable=False),
        StructField('path', StringType(), nullable=False)
    ])

    # Create the DataFrame
    return spark.createDataFrame(positive_catalogs, schema)