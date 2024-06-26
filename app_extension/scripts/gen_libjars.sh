#!/bin/bash

if [ -n "$LIB_JARS" ];
then
 LIB_JARS=$LIB_JARS","
fi

LIB_JARS=$LIB_JARS`envsubst < ${WORKDIR}/config/default_libjars.txt | awk '{printf "%s,", $0}'`

echo $LIB_JARS
