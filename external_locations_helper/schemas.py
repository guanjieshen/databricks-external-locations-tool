from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("external_locations_helper_schemas").getOrCreate()


def __get_managed_schemas_list(
    catalog: str = None,
    system_table: str = "system.information_schema.schemata",
):
    """
    Retrieves the list of schemas from system.information_schema.schemata optionally filtered by catalog

    Args:
        catalog (str): [Optional] Filter for specific catalog name
        system_table (str): [Optional] Specify which system_table to use
    Returns:
        [Row]: Array of schemas names
    """

    schemas = spark.sql(
        f"SELECT catalog_name, schema_name FROM {system_table} where created_by != 'System user'"
    ).withColumn(
        "full_schema_name",
        concat(
            lit("`"),
            col("catalog_name"),
            lit("`.`"),
            col("schema_name"),
            lit("`"),
        ),
    )

    # If catalog is populated
    if catalog:
        schemas = schemas.filter(col("catalog_name") == f"{catalog}")

    schemas_array = schemas.collect()
    return [schema["full_schema_name"] for schema in schemas_array]


def __get_managed_schema_location(schema_name: str):
    """
    Returns the storage location for a schema.

    Args:
        schema_name (str):  Full schema path i.e. `catalog`.`schema`

    Returns:
        str: Cloud storage location for the schema
    """
    print(f"Checking Schema: {schema_name}", end="\r", flush=True)

    try:
        schema_loc = (
            spark.sql(f"describe schema extended {schema_name}")
            .filter(col("database_description_item") == "RootLocation")
            .collect()
        )
        if schema_loc:
            return schema_loc[0]["database_description_value"]
    except Exception as e:
        print(f"Error Checking Schema: {schema_name}")
        return f"Error Checking Schema: {schema_name}"

def get_managed_schemas(
    external_loc: str,
    catalog: str = None,
    system_table: str = "system.information_schema.schemata",
):
    """
    Checks to see which schemas root location contain a specific string.

    Args:
        external_loc (str): Filter tables that contain this str within their root location in cloud storage.
        catalog (str): [Optional] Filter for specific catalog name
        system_table (str): [Optional] Specify which system_table to use
    Returns:
        DataFrame: DataFrame containing the schema location
    """
    managed_schema_list = __get_managed_schemas_list(catalog=catalog)
    positive_schemas = []

    for schema in managed_schema_list:
        location = __get_managed_schema_location(schema)
        if location:
            if external_loc in location.lower():
                schema_details = {"schema_name": schema, "path": location}
                positive_schemas.append(schema_details)

    # Define the schema for the DataFrame
    schema = StructType(
        [
            StructField("schema_name", StringType(), nullable=False),
            StructField("path", StringType(), nullable=False),
        ]
    )
    # Create the DataFrame
    return spark.createDataFrame(positive_schemas, schema)