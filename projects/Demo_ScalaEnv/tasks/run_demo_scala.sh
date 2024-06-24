#!/bin/bash

SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
JAR_FILE="projects/${PROJNAME}/target/scala-2.12/demo_2.12-0.0.jar"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="demo"




DTproc="20240620_1010"
MAINARGS="demo "$DTproc" ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e DOCKER_SPARK_ENTRY_SCRIPT="launcher_templates/proc_sparksubmit.sh" \
  -e TOTAL_EXECUTOR_CORES="2" \
  -e EXECUTOR_MEMORY="1G" \
  -e DRIVER_MEMORY="1G" \
  -e JAR_FILE=${JAR_FILE} \
  -e MAIN_CLASS="Demo" \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${REPOPATH}/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}


MAINARGS="demo2 "$DTproc" ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e DOCKER_SPARK_ENTRY_SCRIPT="launcher_templates/proc_sparksubmit.sh" \
  -e TOTAL_EXECUTOR_CORES="2" \
  -e EXECUTOR_MEMORY="1G" \
  -e DRIVER_MEMORY="1G" \
  -e JAR_FILE=${JAR_FILE} \
  -e MAIN_CLASS="Demo2" \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${REPOPATH}/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}

