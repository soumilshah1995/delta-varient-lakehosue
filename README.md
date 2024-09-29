# delta-varient-lakehosue
delta-varient-lakehosue


```
pip install pyspark==4.0.0.dev1.
pip install delta-spark==4.0.0rc1

pyspark  \
--packages io.delta:delta-spark_2.13:4.0.0rc1   \
--conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension"  \
--conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"

```

https://docs.delta.io/4.0.0-preview/quick-start.html#language-python
