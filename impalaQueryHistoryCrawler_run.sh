#!/bin/bash

job_home="/root/impalaQueryHistoryCrawler"

keytab_path=$(ls -1trd /var/run/cloudera-scm-agent/process/*impala-IMPALAD |tail -1);

cd $job_home;

exec >>impalaQueryHistoryCrawler.log 2>&1

kinit impala/node10.avp.ru@DOMAIN.RU -k -t $keytab_path/impala.keytab;

spark2-submit --queue root.impala --driver-memory 2g --executor-memory 2g --master yarn  --deploy-mode cluster  impalaQueryHistoryCrawler.py
#spark2-submit --queue root.impala --driver-memory 2g --executor-memory 2g  --master yarn  --deploy-mode client  impalaQueryHistoryCrawler.py

impala-shell --ssl -k --quiet -B -d impala_queries -q "refresh history"
impala-shell --ssl -k -B -d impala_queries -q \
 "select concat(\"Yesterday Query History Records Saved Count = \",cast(count(*) as string)) from history where to_date(starttime) = to_date(now() - interval 1 days)"

tail -n 8 impalaQueryHistoryCrawler.log | mailx -s "impalaQueryHistoryCrawler" -S smtp=smtp://mailinternal.domain.ru \
 -S from="impalaQueryHistoryCrawler <impalaQueryHistoryCrawler@mailserver.com>" \
 alexey.shcherbakov@mailserver.com
