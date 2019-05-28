#!/bin/bash

job_home="/root/updateIP2Location"

keytab_path=$(ls -1trd /var/run/cloudera-scm-agent/process/*impala-IMPALAD |tail -1);

cd $job_home;

exec >>updateIP2Location.log 2>&1

kinit impala/node10.domain.ru@DOMAIN.RU -k -t $keytab_path/impala.keytab;

#spark2-submit --queue root.impala --driver-memory 8g --executor-memory 4g --conf=spark.kryoserializer.buffer.max=512 --master yarn  --deploy-mode cluster  updateIP2Location.py
spark2-submit --queue root.impala --driver-memory 8g --executor-memory 4g --conf=spark.kryoserializer.buffer.max=512 --master yarn  --deploy-mode client  updateIP2Location.py
