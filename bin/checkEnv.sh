#!/usr/bin/env bash

LSDir=$(cd `dirname $0`/..;pwd)
#如果环境中未配置JAVA_HOME,可在下面单引号里添加jdk的路径
#export JAVA_HOME=${LSDir}'/jdk'
if [[ "x$JAVA_HOME" = "x" ]]; then
   echo "JAVA_HOME is not configured, please configure and then execute again!"
   exit 1
else
   export JAVA=${JAVA_HOME}/bin/java
fi
java_version=`${JAVA} -version 2>&1 |awk 'NR==1{gsub(/"/,""); print $3}'`
java_version=${java_version:0:3}
if [[ ${java_version//[_|.]/} -lt 18 ]]; then
   echo "java version need 1.8+"
   exit 1;
fi
if [[ ! -x ${JAVA:=''} ]]; then
   echo "java command error..."
   exit 1
fi
