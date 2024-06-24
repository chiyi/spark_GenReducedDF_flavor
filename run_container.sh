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
 -v ${PWD}/user_cache/ipython:/home/spark/.ipython \
 -v ${PWD}/user_cache/scala_history:/home/spark/.scala_history \
 -v ${PWD}/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
 -v ${PWD}/projects:/opt/spark/work-dir/projects \
 --net host \
 --name ${IMAGE_NAME}-bash \
 -e "PYSPARK_DRIVER_PYTHON=ipython3" \
 --rm \
 --entrypoint /bin/bash \
 ${IMAGE_NAME}
