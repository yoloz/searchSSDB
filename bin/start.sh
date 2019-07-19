#!/usr/bin/env bash

case $1 in
  -h |-help| --h | --help)
   echo "USAGE: $0 [daemon]"
   exit 1
  ;;
  *)
  ;;
esac

. `dirname $0`/checkEnv.sh

#if [[ -f `dirname $0`"/java" ]];then
#rm -f `dirname $0`"/java"
#fi
#$(ln -s "$JAVA_HOME/bin/java" `dirname $0`"/java")

test -d ${LSDir}"/logs" || mkdir -p ${LSDir}"/logs"

test -d ${LSDir}"/var" || mkdir -p ${LSDir}"/var"

if [[ "x$1" = "x-f" ]]; then
   exec ${JAVA} "-Xmx1G" "-Xms1G" "-DLSDir="${LSDir} "-DLogLevel=info" -cp ${LSDir}"/lib/*" SSServer "start"
else
   nohup ${JAVA} "-Xmx1G" "-Xms1G" "-DLSDir="${LSDir} "-DLogLevel=info" -cp ${LSDir}"/lib/*" SSServer "start" > ${LSDir}"/logs/server.out" 2>&1 < /dev/null &
fi