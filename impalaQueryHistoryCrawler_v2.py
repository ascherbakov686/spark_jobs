import urllib2,json,re
from pyspark.sql import SparkSession

def getUrls():
 impalad = "http://node2.domain.ru:25000"
 query = "queries?json"
 query_profile = "query_profile?query_id"
 request = "%s/%s" % (impalad,query)
 l = []
 r = urllib2.urlopen(request)
 j = json.load(r)

 x = j['in_flight_queries']
 for i in x:
  l.append("%s/%s=%s" % (impalad,query_profile,i['query_id']))
 
 x = j['completed_queries']
 for i in x:
  l.append("%s/%s=%s" % (impalad,query_profile,i['query_id']))

 return l

def saveData(url):
 r = urllib2.Request(url)

 #
 # Example
 #
 # Query (id=af45dc86e4b57fe9:c39654e200000000)
 # Session ID: 1e4ce02e6cbf959b:df9a6a4b30581b8c
 # Query submitted: 57.864us 
 # Planning finished: 2s540ms 
 # Ready to start on 49 backends: 2s802ms 
 # First row fetched: 5s948ms
 # ClientFetchWaitTimer: 1s955ms
 # RowMaterializationTimer: 2m46s

 rgxs = ['Query\ \(id\=(.+?)\)','Session\ ID\:\ (.+?)$',
         'Query\ submitted\:\ (.+?)$','Planning\ finished\:\ (.+?)$',
         'Ready\ to\ start\ on\ \d+\ backends\:\ (.+?)$','First\ row\ fetched\:\ (.+?)$',
         'ClientFetchWaitTimer\:\ (.+?)$','RowMaterializationTimer\:\ (.+?)$']
 content = urllib2.urlopen(r).read()
 s = ""
 for rgx in rgxs:
   m = re.findall(rgx, content, re.MULTILINE)
   if m:
    s += "%s\t" % str(m[-1])

 return s[:-1]

def main():

 # debug
 #print "query_id\tsession_id\tquery_submitted\tplanning_finished\tready_to_start_on_backends\tfirst_row_fetched\tclientfetchwaittimer\trowmaterializationtimer"
 #for url in getUrls():
 #  print saveData(url)

 spark = SparkSession.builder.appName('ImpalaQueryHistoryCrawler_v2').enableHiveSupport().getOrCreate()
 sc = spark.sparkContext
 log4j = sc._jvm.org.apache.log4j
 log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
 rdd = sc.parallelize(getUrls())
 c = spark.read.option("delimiter", "\t").csv(rdd.map(saveData))
 #c.printSchema()
 #c.show()
 c.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history_profile")
 spark.stop()

if __name__ == '__main__':
 main()

