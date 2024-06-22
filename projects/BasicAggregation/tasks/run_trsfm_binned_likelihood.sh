#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
ENTRY_PYFILE="projects/${PROJNAME}/src/trsfm_binned_likelihood.py"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="Trsfm_binnedLd_GroupingAna"




nhours=24
dt_begin=20240619
val_DTproc=`date -d "$dt_begin 16:00:00" '+%s'`
for ((idx=0; $idx<$nhours; idx++))
do
 DTproc=`date -d '@'$val_DTproc '+%Y%m%d_%H%M'`
 echo $DTproc

 MAINARGS="${DTproc} ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e TOTAL_EXECUTOR_CORES="4" \
  -e EXECUTOR_MEMORY="2G" \
  -e DRIVER_MEMORY="2G" \
  -e ENTRY_PYFILE=${ENTRY_PYFILE} \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}

 val_DTproc=$(($val_DTproc+3600))
done



