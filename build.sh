#!/bin/bash
IMAGE_NAME=`scripts/get_image_name.sh`

docker build -t ${IMAGE_NAME} .
