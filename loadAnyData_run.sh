#!/bin/bash

job_home="/root/LoadAnyData"

keytab_path=$(ls -1trd /var/run/cloudera-scm-agent/process/*impala-IMPALAD |tail -1);

cd $job_home;

exec >>loadAnyData.log 2>&1

kinit impala/node.domain@DOMAIN -k -t $keytab_path/impala.keytab;

spark2-submit --queue root.impala --driver-memory 2g --executor-memory 8g --master yarn  --deploy-mode cluster  loadAnyData.py

#spark2-submit  --queue root.impala --conf spark.network.timeout=800 --driver-memory 2g --executor-memory 2g  --master yarn  --deploy-mode client  loadAnyData.py

#impala-shell --ssl -k --quiet -B -d somedatabase -q "refresh history"
