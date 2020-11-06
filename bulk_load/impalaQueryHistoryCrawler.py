import urllib,urllib2,json,base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pprint
import ssl

def getUrls():
 cdh="http://server.domain.ru:7180"
 l = []
 yesterday = datetime.today() - timedelta(1)
 f = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 1)
 to = datetime(yesterday.year, yesterday.month, yesterday.day, 23, 59, 59)
 delta = timedelta(minutes=30)
 f1=""
 f2=""

 while f < to:
    f1=urllib.quote_plus(f.isoformat())
    f += delta
    f2=urllib.quote_plus(f.isoformat())
    l.append("%s/api/v7/clusters/cluster/services/impala/impalaQueries?from=%s&to=%s&limit=1000&filter=" % (cdh,f1,f2))
 return l

def saveData(url):
 s="["
 login="cron"
 password="**********************"
 ssl._create_default_https_context = ssl._create_unverified_context
 r = urllib2.Request(url)
 base64string = base64.b64encode('%s:%s' % (login, password))
 r.add_header("Authorization", "Basic %s" % base64string)   
 j = json.loads(urllib2.urlopen(r).read())
 for jj in j['queries']:
   s+=str(json.dumps(jj))
   s+=","
 s+="{} ]"
 return s

def main():
 spark = SparkSession.builder.appName('ImpalaQueryHistoryCrawler').enableHiveSupport().getOrCreate()
 sc = spark.sparkContext
 log4j = sc._jvm.org.apache.log4j
 log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
 #rdd = sc.parallelize(getUrls())
 #j = spark.read.json(rdd.map(saveData))
 #j.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history")

 query_schema = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"attributes","nullable":true,"type":{"fields":[{"metadata":{},"name":"admission_result","nullable":true,"type":"string"},{"metadata":{},"name":"bytes_streamed","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"connected_user","nullable":true,"type":"string"},{"metadata":{},"name":"delegated_user","nullable":true,"type":"string"},{"metadata":{},"name":"estimated_per_node_peak_memory","nullable":true,"type":"string"},{"metadata":{},"name":"file_formats","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_average_scan_range","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_from_cache","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_from_cache_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_local","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_local_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_remote","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_remote_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_short_circuit","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_short_circuit_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_scanner_average_bytes_read_per_second","nullable":true,"type":"string"},{"metadata":{},"name":"impala_version","nullable":true,"type":"string"},{"metadata":{},"name":"memory_accrual","nullable":true,"type":"string"},{"metadata":{},"name":"memory_aggregate_peak","nullable":true,"type":"string"},{"metadata":{},"name":"memory_per_node_peak","nullable":true,"type":"string"},{"metadata":{},"name":"memory_per_node_peak_node","nullable":true,"type":"string"},{"metadata":{},"name":"memory_spilled","nullable":true,"type":"string"},{"metadata":{},"name":"network_address","nullable":true,"type":"string"},{"metadata":{},"name":"oom","nullable":true,"type":"string"},{"metadata":{},"name":"original_user","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"pool","nullable":true,"type":"string"},{"metadata":{},"name":"query_status","nullable":true,"type":"string"},{"metadata":{},"name":"session_id","nullable":true,"type":"string"},{"metadata":{},"name":"session_type","nullable":true,"type":"string"},{"metadata":{},"name":"stats_corrupt","nullable":true,"type":"string"},{"metadata":{},"name":"stats_missing","nullable":true,"type":"string"},{"metadata":{},"name":"thread_cpu_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_cpu_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_receive_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_receive_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_send_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_send_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_storage_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_storage_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_total_time","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"coordinator","nullable":true,"type":{"fields":[{"metadata":{},"name":"hostId","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"database","nullable":true,"type":"string"},{"metadata":{},"name":"detailsAvailable","nullable":true,"type":"boolean"},{"metadata":{},"name":"durationMillis","nullable":true,"type":"long"},{"metadata":{},"name":"endTime","nullable":true,"type":"string"},{"metadata":{},"name":"queryId","nullable":true,"type":"string"},{"metadata":{},"name":"queryState","nullable":true,"type":"string"},{"metadata":{},"name":"queryType","nullable":true,"type":"string"},{"metadata":{},"name":"rowsProduced","nullable":true,"type":"long"},{"metadata":{},"name":"startTime","nullable":true,"type":"string"},{"metadata":{},"name":"statement","nullable":true,"type":"string"},{"metadata":{},"name":"user","nullable":true,"type":"string"}],"type":"struct"}'))

 #j = spark.read.json("hdfs://nameservice1/tmp/impala_queries/impala_queries_from_PROD_2020-31-October.json",schema=query_schema).filter("querytype == 'QUERY'")

 query_ddl = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"attributes","nullable":true,"type":{"fields":[{"metadata":{},"name":"admission_result","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"connected_user","nullable":true,"type":"string"},{"metadata":{},"name":"ddl_type","nullable":true,"type":"string"},{"metadata":{},"name":"delegated_user","nullable":true,"type":"string"},{"metadata":{},"name":"file_formats","nullable":true,"type":"string"},{"metadata":{},"name":"impala_version","nullable":true,"type":"string"},{"metadata":{},"name":"network_address","nullable":true,"type":"string"},{"metadata":{},"name":"oom","nullable":true,"type":"string"},{"metadata":{},"name":"original_user","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"query_status","nullable":true,"type":"string"},{"metadata":{},"name":"session_id","nullable":true,"type":"string"},{"metadata":{},"name":"session_type","nullable":true,"type":"string"},{"metadata":{},"name":"stats_corrupt","nullable":true,"type":"string"},{"metadata":{},"name":"stats_missing","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"coordinator","nullable":true,"type":{"fields":[{"metadata":{},"name":"hostId","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"database","nullable":true,"type":"string"},{"metadata":{},"name":"detailsAvailable","nullable":true,"type":"boolean"},{"metadata":{},"name":"durationMillis","nullable":true,"type":"long"},{"metadata":{},"name":"endTime","nullable":true,"type":"string"},{"metadata":{},"name":"queryId","nullable":true,"type":"string"},{"metadata":{},"name":"queryState","nullable":true,"type":"string"},{"metadata":{},"name":"queryType","nullable":true,"type":"string"},{"metadata":{},"name":"rowsProduced","nullable":true,"type":"string"},{"metadata":{},"name":"startTime","nullable":true,"type":"string"},{"metadata":{},"name":"statement","nullable":true,"type":"string"},{"metadata":{},"name":"user","nullable":true,"type":"string"}],"type":"struct"}'))

 #j = spark.read.json("hdfs://nameservice1/tmp/impala_queries/impala_queries_from_PROD_2020-31-October.json",schema=ddl_schema).filter("querytype == 'DDL'")

 query_dml = StructType.fromJson(json.loads('{"fields":[{"metadata":{},"name":"attributes","nullable":true,"type":{"fields":[{"metadata":{},"name":"admission_result","nullable":true,"type":"string"},{"metadata":{},"name":"bytes_streamed","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"client_fetch_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"connected_user","nullable":true,"type":"string"},{"metadata":{},"name":"estimated_per_node_peak_memory","nullable":true,"type":"string"},{"metadata":{},"name":"file_formats","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_average_scan_range","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_from_cache","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_from_cache_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_local","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_local_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_remote","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_remote_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_short_circuit","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_read_short_circuit_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_bytes_written","nullable":true,"type":"string"},{"metadata":{},"name":"hdfs_scanner_average_bytes_read_per_second","nullable":true,"type":"string"},{"metadata":{},"name":"impala_version","nullable":true,"type":"string"},{"metadata":{},"name":"memory_accrual","nullable":true,"type":"string"},{"metadata":{},"name":"memory_aggregate_peak","nullable":true,"type":"string"},{"metadata":{},"name":"memory_per_node_peak","nullable":true,"type":"string"},{"metadata":{},"name":"memory_per_node_peak_node","nullable":true,"type":"string"},{"metadata":{},"name":"network_address","nullable":true,"type":"string"},{"metadata":{},"name":"oom","nullable":true,"type":"string"},{"metadata":{},"name":"original_user","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"planning_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"pool","nullable":true,"type":"string"},{"metadata":{},"name":"query_status","nullable":true,"type":"string"},{"metadata":{},"name":"rows_inserted","nullable":true,"type":"string"},{"metadata":{},"name":"session_id","nullable":true,"type":"string"},{"metadata":{},"name":"session_type","nullable":true,"type":"string"},{"metadata":{},"name":"stats_corrupt","nullable":true,"type":"string"},{"metadata":{},"name":"stats_missing","nullable":true,"type":"string"},{"metadata":{},"name":"thread_cpu_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_cpu_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_receive_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_receive_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_send_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_network_send_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_storage_wait_time","nullable":true,"type":"string"},{"metadata":{},"name":"thread_storage_wait_time_percentage","nullable":true,"type":"string"},{"metadata":{},"name":"thread_total_time","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"coordinator","nullable":true,"type":{"fields":[{"metadata":{},"name":"hostId","nullable":true,"type":"string"}],"type":"struct"}},{"metadata":{},"name":"database","nullable":true,"type":"string"},{"metadata":{},"name":"detailsAvailable","nullable":true,"type":"boolean"},{"metadata":{},"name":"durationMillis","nullable":true,"type":"long"},{"metadata":{},"name":"endTime","nullable":true,"type":"string"},{"metadata":{},"name":"queryId","nullable":true,"type":"string"},{"metadata":{},"name":"queryState","nullable":true,"type":"string"},{"metadata":{},"name":"queryType","nullable":true,"type":"string"},{"metadata":{},"name":"rowsProduced","nullable":true,"type":"long"},{"metadata":{},"name":"startTime","nullable":true,"type":"string"},{"metadata":{},"name":"statement","nullable":true,"type":"string"},{"metadata":{},"name":"user","nullable":true,"type":"string"}],"type":"struct"}'))

 #j = spark.read.json("hdfs://nameservice1/tmp/impala_queries/impala_queries_from_PROD_2020-31-October.json",schema=dml_schema).filter("querytype == 'DML'")

 files = "hdfs://nameservice1/tmp/impala_queries/*.json"

 j = spark.read.json(files,query_schema).filter("querytype == 'QUERY'")
 #print j.count()
 #j.printSchema()
 j.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history_query")

 j = spark.read.json(files,query_ddl).filter("querytype == 'DDL'")
 #print j.count()
 #j.printSchema()
 j.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history_ddl")

 j = spark.read.json(files,query_dml).filter("querytype == 'DML'")
 #print j.count()
 #j.printSchema()
 j.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history_dml")


 spark.stop()

if __name__ == '__main__':
 main()

