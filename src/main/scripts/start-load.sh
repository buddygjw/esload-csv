#!/bin/sh

SP_MIN_MEM=512m
SP_MAX_MEM=1024m

SCRIPT="$0"
# SCRIPT may be an arbitrarily deep series of symlinks. Loop until we have the concrete path.
while [ -h "$SCRIPT" ] ; do
  ls=`ls -ld "$SCRIPT"`
  # Drop everything prior to ->
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '/.*' > /dev/null; then
    SCRIPT="$link"
  else
    SCRIPT=`dirname "$SCRIPT"`/"$link"
  fi
done
RTW_SERVER_HOME=`dirname "$SCRIPT"`/..
RTW_SERVER_HOME=`cd "$RTW_SERVER_HOME"; pwd`

SP_CLASSPATH=$CLASSPATH:$RTW_SERVER_HOME/lib/*

#java opts config
SP_OPTS="-Dinstall.home=$RTW_SERVER_HOME"
SP_OPTS="$SP_OPTS -Dlog4j.configuration=file:$RTW_SERVER_HOME/config/log4j.properties"
SP_OPTS="$SP_OPTS -Xms${SP_MIN_MEM} -Xmx${SP_MAX_MEM}"

if [ ! -d $RTW_SERVER_HOME/logs ]
then
    mkdir -p $RTW_SERVER_HOME/logs
fi

#nohup java $SP_OPTS -cp $SP_CLASSPATH com.teligen.iis4g.service.ServerApp >/dev/null 2>&1 &
java $SP_OPTS -cp $SP_CLASSPATH com.teligen.bigdata.esload.ESLoadServer