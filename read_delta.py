try:
    import pyspark
    from delta import *
    from pyspark.sql.functions import parse_json
    from pyspark.sql.functions import try_variant_get
except Exception as e:
    print("Error ", e)



builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()


# Read data from the Delta table
delta_table_path = "/Users/sshah/IdeaProjects/poc-projects/lakehouse/data/delta-table"
delta_df = spark.read.format("delta").load(delta_table_path)

# Show the contents of the DataFrame
delta_df.show()

# Print the schema of the DataFrame
delta_df.printSchema()

# Extracting values from the JSON variant using try_variant_get
df3 = delta_df.select(
    try_variant_get("json_var", "$.user.name", "STRING").alias("name"),
    try_variant_get("json_var", "$.user.age", "INT").alias("age"),
    try_variant_get("json_var", "$.user.hobbies[0]", "STRING").alias("first_hobby"),
    try_variant_get("json_var", "$.status", "STRING").alias("status"),
    try_variant_get("json_var", "$.tags[1]", "STRING").alias("second_tag")
)

# Show the extracted values
df3.show()