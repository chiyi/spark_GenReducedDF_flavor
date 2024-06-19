#!/bin/bash

DEFAULT_SCRIPT=launcher_templates/proc_pysparksubmit.sh

# trap ctrl-c and call ctrl_c()
trap ctrl_c INT

function ctrl_c() {
    echo "** Trapped CTRL-C"
    echo kill -9 $PID
    kill -9 $PID
}




echo "*** STARTING from main.sh ***"
#env

if [ -z $DOCKER_SPARK_ENTRY_SCRIPT ];
then
 echo default: ${DEFAULT_SCRIPT}
 ${DEFAULT_SCRIPT} &
 PID=$1
 wait
else
 echo from... $DOCKER_SPARK_ENTRY_SCRIPT
 $DOCKER_SPARK_ENTRY_SCRIPT &
 PID=$1
 wait
fi

echo "*** END of main.sh ***"
