#!/bin/bash

SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
APP_JAR="projects/${PROJNAME}/target/scala-2.12/demo_nestedjson_2.12-0.0.jar"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="demo"




DTproc="20240620_1500"
MAINARGS="Demo_NestedJson "$DTproc" ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv 1"
 docker run \
  -e DOCKER_SPARK_ENTRY_SCRIPT="launcher_templates/proc_sparksubmit.sh" \
  -e TOTAL_EXECUTOR_CORES="4" \
  -e EXECUTOR_MEMORY="4G" \
  -e DRIVER_MEMORY="4G" \
  -e APP_JAR=${APP_JAR} \
  -e MAIN_CLASS="Demo_NestedJson" \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}
