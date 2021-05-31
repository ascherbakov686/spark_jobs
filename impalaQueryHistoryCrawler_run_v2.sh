#!/bin/bash

job_home="/root/impalaQueryHistoryCrawler/"

keytab_path=$(ls -1trd /var/run/cloudera-scm-agent/process/*impala-IMPALAD |tail -1);

cd $job_home;

exec >>impalaQueryHistoryCrawler_v2.log 2>&1

kinit impala/node10.avp.ru@DOMAIN.RU -k -t $keytab_path/impala.keytab;

#spark2-submit --queue root.impala --driver-memory 2g --executor-memory 2g --master yarn  --deploy-mode cluster  impalaQueryHistoryCrawler.py
spark2-submit  --queue root.impala --conf spark.network.timeout=800 --driver-memory 2g --executor-memory 2g  --master yarn  --deploy-mode client --py-files durationpy.zip impalaQueryHistoryCrawler_v2.py

impala-shell --ssl -k --quiet -B -d impala_queries -q "refresh history_profile"
