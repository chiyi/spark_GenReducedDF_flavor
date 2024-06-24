#!/bin/bash
SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
export TZ=Asia/Taipei
IMAGE_NAME=`$SCPATH/scripts/get_image_name.sh`

cd $SCPATH
docker run -it \
 -e TERM="xterm-256color" \
 -e TOTAL_EXECUTOR_CORES="8" \
 -e EXECUTOR_MEMORY="2G" \
 -e DRIVER_MEMORY="2G" \
 -v ${PWD}/user_cache/ipython:/home/spark/.ipython \
 -v ${PWD}/spark-defaults.conf:/opt/spark/conf/spark-defaults.conf \
 -v ${PWD}/projects:/opt/spark/work-dir/projects \
 --net host \
 --name ${IMAGE_NAME}-sql \
 --rm \
 --entrypoint launcher_templates/proc_sparksql.sh \
 ${IMAGE_NAME}
