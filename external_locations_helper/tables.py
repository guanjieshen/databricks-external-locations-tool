from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession, Row

spark = SparkSession.builder.appName("external_locations_helper_tables").getOrCreate()


def __get_tables_list(
    table_type: str = None,
    catalog: str = None,
    schema: str = None,
    system_table: str = "system.information_schema.tables",
):
    """
    Retrieves the list of tables from system.information_schema.tables filter by table_type, catalog, or schema.

    Args:
        table_type (str): [Optional] Filter for either MANAGED or EXTERNAL tables
        catalog (str): [Optional] Filter for specific catalog name
        schema (str): [Optional] Filter for specific schema name
        system_table (str): [Optional] Specify which system_table to use
    Returns:
        [Row]: Array of pyspark.sql.Row
    """

    # Generate SQL query
    sql_query = f"""
        SELECT * FROM {system_table} where data_source_format != 'UNITY_CATALOG'
    """
    if table_type == None:
        sql_query = sql_query + "and table_type in ('MANAGED','EXTERNAL')"
    elif table_type.upper() == "EXTERNAL":
        sql_query = sql_query + "and table_type = 'EXTERNAL'"

    elif table_type.upper() == "MANAGED":
        sql_query = sql_query + "and table_type = 'MANAGED'"

    tables = spark.sql(sql_query).withColumn(
        "full_table_name",
        concat(
            lit("`"),
            col("table_catalog"),
            lit("`.`"),
            col("table_schema"),
            lit("`.`"),
            col("table_name"),
            lit("`"),
        ),
    )

    # If catalog is populated
    if catalog:
        tables = tables.filter(col("table_catalog") == f"{catalog}")

    # If schema is populated
    if schema:
        tables = tables.filter(col("table_schema") == f"{schema}")

    # Select required columns
    tables_array = tables.select(
        "table_catalog",
        "table_schema",
        "table_name",
        "full_table_name",
        "table_type",
        "table_owner",
        "created",
        "created_by",
        "last_altered",
        "last_altered_by",
        "data_source_format",
    ).collect()

    return tables_array


def __get_external_location_for_table(table_name: str) -> str:
    """
    Returns the storage location for a table.

    Args:
        table_name (str):  Full table path i.e. `catalog`.`schema`.`table`

    Returns:
        str: Cloud storage location for the table
    """
    from pyspark.sql.functions import col
    try:
        table_loc = (
            spark.sql(f"describe table extended {table_name}")
            .filter(col("col_name") == "Location")
            .collect()
        )
        return table_loc[0]["data_type"]
    except Exception as e:
        print(f"Error Checking Table: {table_name}")
        return f"Error Checking Table: {table_name}"
        
def get_table_paths(
    external_loc: str,
    table_type: str = None,
    catalog: str = None,
    schema: str = None,
    system_table: str = "system.information_schema.tables",
):

    """
    Checks to see which tables root location contain a specific string.

    Args:
        external_loc (str): Filter tables that contain this str within their root location in cloud storage.
        table_type (str): [Optional] Filter for either MANAGED or EXTERNAL tables
        catalog (str): [Optional] Filter for specific catalog name
        schema (str): [Optional] Filter for specific schema name
        system_table (str): [Optional] Specify which system_table to use
    Returns:
        DataFrame: Array of pyspark.sql.Row
    """
    tables_list = __get_tables_list(catalog=catalog, schema=schema)

    positive_tables = []

    for table in tables_list:
        full_table_name = table["full_table_name"]
        print(f"Checking Table: {full_table_name}", end="\r", flush=True)

        table_location = __get_external_location_for_table(full_table_name)

        if external_loc.lower() in table_location.lower():
            table_row = Row(**table.asDict(), table_path=table_location)
            positive_tables.append(table_row)

    if len(positive_tables) > 0:
        return spark.createDataFrame(positive_tables)
    else:
        spark.createDataFrame([])