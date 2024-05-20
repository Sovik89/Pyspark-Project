spark-submit --deploy-mode cluster \
--conf spark.yarn.appMasterEnv.SRC_BASE_DIR=/user/sovik/retail_db \
--conf spark.yarn.appMasterEnv.TGT_BASE_DIR=/user/sovik/retail_db \
--jars /tmp/jars/antlr4-runtime-4.9.3.jar,/tmp/jars/delta-storage-3.0.0.jar,/tmp/jars/delta-spark_2.12-3.0.0.jar \
--conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
--conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
/home/sovik/data-engineering-using-spark/pyspark_scripts/daily_revenue_computation_final.py
