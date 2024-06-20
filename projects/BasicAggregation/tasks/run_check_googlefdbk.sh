#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
ENTRY_PYFILE="projects/${PROJNAME}/src/check_googlefdbk.py"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="check_googlefdbk"

arr_dtdata=("20240620_1010")
for DTproc in ${arr_dtdata[@]};
do
 echo $DTproc
 MAINARGS="${DTproc} ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e TOTAL_EXECUTOR_CORES="8" \
  -e EXECUTOR_MEMORY="4G" \
  -e DRIVER_MEMORY="4G" \
  -e ENTRY_PYFILE=${ENTRY_PYFILE} \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}
done



