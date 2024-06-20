#!/bin/bash
set -ex
PYLIBDIR=${SPARK_HOME}/python

pip install --no-cache-dir -t $PYLIBDIR/ext_pypkgs -r config/pip_requirements.txt
zip -r $PYLIBDIR/lib/ext_pypkgs.zip $PYLIBDIR/ext_pypkgs

pip install --no-cache-dir -r config/pip_requirements.txt
