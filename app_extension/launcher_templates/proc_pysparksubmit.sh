#!/bin/bash

echo TOTAL_EXECUTOR_CORES=$TOTAL_EXECUTOR_CORES
echo EXECUTOR_MEMORY=$EXECUTOR_MEMORY
echo DRIVER_MEMORY=$DRIVER_MEMORY
echo ENTRY_PYFILE=$ENTRY_PYFILE
echo MAINARGS=$MAINARGS
PY_FILES=`${WORKDIR}/scripts/gen_pyfiles.sh`

spark-submit --py-files ${PY_FILES} \
 --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
 --executor-memory ${EXECUTOR_MEMORY} \
 --driver-memory ${DRIVER_MEMORY} \
 ${ENTRY_PYFILE} ${MAINARGS}

