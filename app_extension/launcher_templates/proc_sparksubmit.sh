#!/bin/bash

echo TOTAL_EXECUTOR_CORES=$TOTAL_EXECUTOR_CORES
echo EXECUTOR_MEMORY=$EXECUTOR_MEMORY
echo DRIVER_MEMORY=$DRIVER_MEMORY
echo JAR_FILE=$JAR_FILE
echo MAIN_CLASS=$MAIN_CLASS
echo MAINARGS=$MAINARGS

# reference to build jar file
# ./run_container.sh
#
# if we would like to seperate the runner name between spark and user
# chmod 777 projects/${PROJNAME}    
# cd projects/${PROJNAME}
# sbt compile
# sbt package
# ls target/scala-*/*.jar

spark-submit \
 --class ${MAIN_CLASS} \
 --total-executor-cores ${TOTAL_EXECUTOR_CORES} \
 --executor-memory ${EXECUTOR_MEMORY} \
 --driver-memory ${DRIVER_MEMORY} \
 ${JAR_FILE} ${MAINARGS}

