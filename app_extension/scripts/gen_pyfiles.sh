#!/bin/bash

if [ -z "$PY_FILES" ];
then
 PY_FILES=`envsubst < ${WORKDIR}/config/default_pyfiles.txt | awk '{printf "%s,", $0}'`
fi

echo $PY_FILES
