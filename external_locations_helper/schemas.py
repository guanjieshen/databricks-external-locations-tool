from pyspark.sql.functions import concat, col, lit
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("external_locations_helper_schemas").getOrCreate()


def get_managed_schemas_list(catalog: str = None):
    schemas = spark.sql(
        "SELECT catalog_name, schema_name FROM system.information_schema.schemata where created_by != 'System user'"
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


def get_managed_schema_location(schema_name: str):
    print(f"Checking Schema: {schema_name}", end="\r", flush=True)

    schema_loc = (
        spark.sql(f"describe schema extended {schema_name}")
        .filter(col("database_description_item") == "RootLocation")
        .collect()
    )
    if schema_loc:
        return schema_loc[0]["database_description_value"]


def get_managed_schemas(external_loc: str, catalog: str = None):
    managed_schema_list = get_managed_schemas_list(catalog=catalog)
    positive_schemas = []

    for schema in managed_schema_list:
        location = get_managed_schema_location(schema)
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