# Spark Status Store Connector

A Spark DSv2 connector for querying its runtime status


## Usage

```shell
bin/spark-sql \
  --jars /path/to/spark-status-store-connector-0.1.0-SNAPSHOT.jar \
  --conf spark.sql.catalog.live=org.apache.spark.yao.StatusStoreCatalog \
  --conf ...
```

```shell
spark-sql (default)> show tables in live;
hadoop_property
metrics_property
rdd
application
stage
active_stage
executor
spark_property
system_property
runtime
job

```

