#!/usr/bin/env bash

set -x
SRC_HOME=$(dirname "$0")
SRC_HOME=$(cd -P -- "${SRC_HOME}"; pwd -P)

SPARK_HOME=/opt/spark
PHOENIX_HOME=/opt/phoenix


ANYSTREAM_CONF="anystream-uxip.conf"
ANYSTREAM_CONF_PATH="${SRC_HOME}/${ANYSTREAM_CONF}"


ANYSTREAM_TRANSFORM_HQL="uxip_transform.hive.sql"
ANYSTREAM_LL_ACTION_HQL="uxip_ll_action.hive.sql"
ANYSTREAM_HL_ACTION_HQL="uxip_hl_action.hive.sql"

ANYSTREAM_HQL_PATH="${SRC_HOME}/${ANYSTREAM_TRANSFORM_HQL}"
ANYSTREAM_HQL_PATH="${ANYSTREAM_HQL_PATH},${SRC_HOME}/${ANYSTREAM_LL_ACTION_HQL}"
ANYSTREAM_HQL_PATH="${ANYSTREAM_HQL_PATH},${SRC_HOME}/${ANYSTREAM_HL_ACTION_HQL}"


ANYSTREAM_JARS="${SPARK_HOME}/lib/datanucleus-api-jdo-3.2.6.jar"
ANYSTREAM_JARS="${ANYSTREAM_JARS},${SPARK_HOME}/lib/datanucleus-rdbms-3.2.9.jar"
ANYSTREAM_JARS="${ANYSTREAM_JARS},${SPARK_HOME}/lib/datanucleus-core-3.2.10.jar"
ANYSTREAM_JARS="${ANYSTREAM_JARS},${SPARK_HOME}/extraclass/mysql-connector-java-5.1.22-bin.jar"
ANYSTREAM_JARS="${ANYSTREAM_JARS},${SPARK_HOME}/extraclass/hadoop-lzo-0.4.20-SNAPSHOT.jar"

# --conf spark.sql.shuffle.partitions=20 \
# local[*]  yarn-client
# --ll_action ${ANYSTREAM_LL_ACTION_HQL} \
# --conf spark.streaming.blockInterval=1000ms \
# --conf spark.streaming.concurrentJobs=2
# --conf HADOOP_USER_NAME=hadoop \
# --conf user.name=hadoop \
# --conf "spark.streaming.receiver.maxRate=1000ms" \
# --conf "spark.sql.codegen=true"

${SPARK_HOME}/bin/spark-submit \
	--verbose \
        --name "ANYSTREAM-UXIP" \
	--master yarn-cluster \
        --queue anystream \
        --num-executors 18 \
        --executor-cores 1 \
        --executor-memory 5G \
        --driver-cores 1   \
        --conf "spark.streaming.receiver.writeAheadLog.enable=true" \
        --conf "spark.streaming.receiver.maxRate=3000" \
        --conf "yarn.resourcemanager.am.max-attempts=3" \
        --conf "spark.storage.memoryFraction=0.4" \
        --conf "spark.shuffle.consolidateFiles=true" \
        --conf "spark.executor.extraJavaOptions=-XX:+UseG1GC -XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=/home/hadoop/" \
        --conf "spark.io.compression.codec=org.apache.spark.io.LZ4CompressionCodec" \
        --conf "spark.yarn.executor.memoryOverhead=1024" \
        --driver-java-options  "-XX:PermSize=128M -XX:MaxPermSize=256M" \
        --jars "${ANYSTREAM_JARS}" \
        --files "${ANYSTREAM_HQL_PATH},${ANYSTREAM_CONF_PATH}" \
	--class com.meizu.anystream.ETL \
	"${SRC_HOME}/anystream.jar" \
	--conf ${ANYSTREAM_CONF} \
	--transform ${ANYSTREAM_TRANSFORM_HQL} \
        --ll_action ${ANYSTREAM_LL_ACTION_HQL} \
        --hl_action ${ANYSTREAM_HL_ACTION_HQL}

