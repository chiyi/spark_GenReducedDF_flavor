#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/../../..`
PROJNAME=`echo $SCPATH | awk -F "/" '{print $(NF-1)}'`
export TZ=Asia/Taipei
IMAGE_NAME=`$REPOPATH/scripts/get_image_name.sh`
ENTRY_PYFILE="projects/${PROJNAME}/src/create_dfparq.py"
OUTPUT_DIR="projects/${PROJNAME}/res"
OUTPUT_PREFIX="demo"

arr_dtdata=("20240619_1610" "20240619_1715")
for DTproc in ${arr_dtdata[@]};
do
 echo $DTproc
 MAINARGS="${DTproc} ${OUTPUT_DIR}/${OUTPUT_PREFIX}_${DTproc}.tsv"
 docker run \
  -e TOTAL_EXECUTOR_CORES="2" \
  -e EXECUTOR_MEMORY="2G" \
  -e DRIVER_MEMORY="2G" \
  -e ENTRY_PYFILE=${ENTRY_PYFILE} \
  -e MAINARGS="${MAINARGS}" \
  -v ${REPOPATH}/projects:/opt/spark/work-dir/projects \
  -v ${SCPATH}/spark.conf:/opt/spark/conf/spark-defaults.conf \
  --net host \
  --rm \
  ${IMAGE_NAME}
done



