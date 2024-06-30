#!/bin/bash

SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
APP_JAR="projects/${PROJNAME}/target/scala-2.12/datarichness_2.12-0.0.jar"
INPUTFMT="'projects/DataRichness/data/Fake/'yyyy/MM/dd/HH'H'/'Fake_'yyyy_MM_dd_HH'H.parquet'"
OUTPUTFILE="projects/DataRichness/table/Table_NonEmptyCols_fake.tsv"




DTproc="20240620_0800"
MAINARGS="schema1d "$DTproc" ${OUTPUTFILE} 3 ${INPUTFMT}"
 docker run \
  -e DOCKER_SPARK_ENTRY_SCRIPT="launcher_templates/proc_sparksubmit.sh" \
  -e TOTAL_EXECUTOR_CORES="1" \
  -e EXECUTOR_MEMORY="1G" \
  -e DRIVER_MEMORY="1G" \
  -e APP_JAR=${APP_JAR} \
  -e MAIN_CLASS="GenTable" \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}
