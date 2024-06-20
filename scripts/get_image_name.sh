#!/bin/bash

SCFULLNAME=`realpath $0`
SCPATH=`dirname $SCFULLNAME`
REPOPATH=`realpath $SCPATH/..`

echo $REPOPATH | awk -F "/" '{print tolower($NF)}'
