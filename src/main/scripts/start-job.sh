#!/bin/bash
export JAVA_HOME=/opt/java/current
app_home=/software/etl/DataCollectionCenter
for f in ${app_home}/lib/*.jar;do
    RUN_PATH=${RUN_PATH}:$f
done
JAVA_OPTS="-Xmx2G -XX:MaxPermSize=256M -Dproperties.file=${app_home}/conf/igniteDynamicStringExample.properties"
${JAVA_HOME}/bin/java $JAVA_OPTS -cp $RUN_PATH com.pholema.job.server.Application
