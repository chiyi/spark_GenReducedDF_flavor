#!/bin/bash

SUBMIT_APP_NAME="pyspark_shell_kaiyi"
echo SUBMIT_APP_NAME=${SUBMIT_APP_NAME}
echo TOTAL_EXECUTOR_CORES=$TOTAL_EXECUTOR_CORES
echo EXECUTOR_MEMORY=$EXECUTOR_MEMORY
echo DRIVER_MEMORY=$DRIVER_MEMORY
SPARK_MASTER=local[4]
PY_FILES=`${WORKDIR}/scripts/gen_pyfiles.sh`

pyspark --name ${SUBMIT_APP_NAME} \
 --py-files ${PY_FILES} \
 --master ${SPARK_MASTER} \
 --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
 --executor-memory ${EXECUTOR_MEMORY} \
 --driver-memory ${DRIVER_MEMORY}

