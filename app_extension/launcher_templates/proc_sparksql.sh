#!/bin/bash

SUBMIT_APP_NAME="sparksql_shell_kaiyi"
echo SUBMIT_APP_NAME=${SUBMIT_APP_NAME}
echo TOTAL_EXECUTOR_CORES=$TOTAL_EXECUTOR_CORES
echo EXECUTOR_MEMORY=$EXECUTOR_MEMORY
echo DRIVER_MEMORY=$DRIVER_MEMORY
echo MAINARGS=$MAINARGS
SPARK_MASTER=local[4]

spark-sql --name ${SUBMIT_APP_NAME} \
 --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
 --executor-memory ${EXECUTOR_MEMORY} \
 --driver-memory ${DRIVER_MEMORY}


#CREATE TEMPORARY VIEW parquetTable
#USING org.apache.spark.sql.parquet
#OPTIONS (
#    path "/AAAA/BBBBB/DATA.X/2024/Jul/11/00/30"
#);
#SELECT count(*) FROM parquetTable;
#DESCRIBE TABLE parquetTable;
