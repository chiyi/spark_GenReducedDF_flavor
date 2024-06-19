#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
export TZ=Asia/Taipei
IMAGE_NAME=`$SCPATH/scripts/get_image_name.sh`

cd $SCPATH
docker run -it \
 -e TERM="xterm-256color" \
 -e TOTAL_EXECUTOR_CORES="16" \
 -e EXECUTOR_MEMORY="4G" \
 -e DRIVER_MEMORY="4G" \
 -e PY_FILES=${PY_FILES} \
 -v ${PWD}/ipython:/home/spark/.ipython \
 -v ${PWD}/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
 -v ${PWD}/projects:/opt/spark/work-dir/projects \
 --net host \
 --name ${IMAGE_NAME}-pyspark \
 -e "PYSPARK_DRIVER_PYTHON=ipython3" \
 --rm \
 --entrypoint launcher_templates/proc_pyspark.sh \
 ${IMAGE_NAME}
