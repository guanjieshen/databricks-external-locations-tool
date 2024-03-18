from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("external_locations_helper_tables") \
    .getOrCreate()


def get_external_tables_list(catalog: str = None, schema: str = None):

    tables = spark.sql(
        """
    SELECT * FROM system.information_schema.tables where table_type = 'EXTERNAL' and data_source_format != 'UNITY_CATALOG'
    """
    ).withColumn(
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

    tables_array = tables.collect()

    return [table["full_table_name"] for table in tables_array]


def get_external_location_for_table(table_name: str) -> str:
    from pyspark.sql.functions import col

    table_loc = (
        spark.sql(f"describe table extended {table_name}")
        .filter(col("col_name") == "Location")
        .collect()
    )
    return table_loc[0]["data_type"]


def get_external_tables(external_loc: str, catalog: str = None, schema: str = None):
    external_tables_list = get_external_tables_list(catalog=catalog, schema=schema)

    positive_tables = []

    for table in external_tables_list:
        print(f"Checking External Table: {table}", end="\r", flush=True)
        location = get_external_location_for_table(table)
        if external_loc in location.lower():
            table_details = {"table_name": table, "path": location}
            positive_tables.append(table_details)

    # Define the schema for the DataFrame
    schema = StructType(
        [
            StructField("table_name", StringType(), nullable=False),
            StructField("path", StringType(), nullable=False),
        ]
    )

    # Create the DataFrame
    return spark.createDataFrame(positive_tables, schema)