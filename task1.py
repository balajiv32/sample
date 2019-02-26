from __future__ import print_function
import sys
from pyspark.sql import SparkSession
spark = SparkSession\
        .builder\
        .appName("PythonSort")\
        .enableHiveSupport()\
        .getOrCreate()

ddd_fact = spark.read.parquet("s3n://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/onboarding/training/bv20370/source/parquet/fact/")
ddd_prdgrp = spark.read.parquet("s3n://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/onboarding/training/bv20370/source/parquet/prdgrp/")
ddd_fact.registerTempTable("temp_fact_table")
ddd_prdgrp.registerTempTable("temp_prdgrp")
spark.sql("create table if not exists Fact_Table as SELECT * from temp_fact_table")
spark.sql("create table if not exists prdgrp as SELECT * from temp_prdgrp")
task1 =spark.sql("select Sales_fact.market_id as Market_id,Sales_fact.product_group_id as product_group, Sum (Sales_fact.volume_units) as Total_volume_units,sum(Sales_fact.dollars) as total_revenue  from Fact_Table Sales_fact left outer join   prdgrp prdgrp on Sales_fact.market_id=prdgrp.market_id and Sales_fact.product_group_id =prdgrp.product_group_id group by Sales_fact.market_id,Sales_fact.product_group_id")
task1.coalesce(1).write.mode("overwrite").format("csv").save("s3n://aws-a0049-use1-00-d-s3b-bpod-bdp-data01/onboarding/training/bv20370/results_from_airflow/")
spark.stop()


