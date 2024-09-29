try:
    import pyspark
    from delta import *
    from pyspark.sql.functions import parse_json
except Exception as e:
    print("Error ", e)

builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df1 = spark.createDataFrame([{
    'json_string': '''{
        "user": {
            "name": "Alice",
            "age": 30,
            "hobbies": ["reading", "swimming", "hiking"],
            "address": {
                "city": "Wonderland",
                "zip": "12345"
            }
        },
        "status": "active",
        "tags": ["admin", "user", "editor"]
    }'''
}])
df2 = df1.select(
    parse_json(df1.json_string).alias("json_var")
)

df2.show()
df2.printSchema()

path = "/Users/sshah/IdeaProjects/poc-projects/lakehouse/data/delta-table"
df2.write.format("delta").save(path)
