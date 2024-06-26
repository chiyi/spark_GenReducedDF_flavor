#!/bin/bash

SUBMIT_APP_NAME="spark"
echo SUBMIT_APP_NAME=${SUBMIT_APP_NAME}
echo TOTAL_EXECUTOR_CORES=$TOTAL_EXECUTOR_CORES
echo EXECUTOR_MEMORY=$EXECUTOR_MEMORY
echo DRIVER_MEMORY=$DRIVER_MEMORY
LIB_JARS=`${WORKDIR}/scripts/gen_libjars.sh`
echo LIB_JARS=$LIB_JARS


spark-shell \
 --jars ${LIB_JARS} \
 --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
 --executor-memory ${EXECUTOR_MEMORY} \
 --driver-memory ${DRIVER_MEMORY}

