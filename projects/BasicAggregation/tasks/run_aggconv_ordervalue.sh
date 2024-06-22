#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
ENTRY_PYFILE="projects/${PROJNAME}/src/aggconv_ordervalue.py"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="aggconv_ordervalue"




ndays=1
dt_begin=20240620
val_DTproc=`date -d "$dt_begin 14:10:00" '+%s'`
for ((idx=0; $idx<$ndays; idx++))
do
 DTproc=`date -d '@'$val_DTproc '+%Y%m%d_%H%M'`
 echo $DTproc

 MAINARGS="${DTproc} ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e TOTAL_EXECUTOR_CORES="2" \
  -e EXECUTOR_MEMORY="1G" \
  -e DRIVER_MEMORY="1G" \
  -e ENTRY_PYFILE=${ENTRY_PYFILE} \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}

 val_DTproc=$(($val_DTproc+86400))
done



