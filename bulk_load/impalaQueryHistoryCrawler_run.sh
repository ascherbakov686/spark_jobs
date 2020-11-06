#!/bin/bash

job_home="/root/impalaQueryHistoryCrawler/bulk_load"

keytab_path=$(ls -1trd /var/run/cloudera-scm-agent/process/*impala-IMPALAD |tail -1);

cd $job_home;

exec >>impalaQueryHistoryCrawler.log 2>&1

kinit impala/server.domain.ru@DOMAIN.RU -k -t $keytab_path/impala.keytab;

#spark2-submit --queue root.impala --driver-memory 2g --executor-memory 2g --master yarn  --deploy-mode cluster  impalaQueryHistoryCrawler.py
spark2-submit --queue root.impala --driver-memory 2g --executor-memory 2g  --master yarn  --deploy-mode client  impalaQueryHistoryCrawler.py

impala-shell --ssl -k --quiet -B -d impala_queries -q "refresh history_query;refresh history_dml;refresh history_ddl;"

