import urllib2,json,re
from pyspark.sql import SparkSession
import durationpy

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

 rgxs = ['Query\ \(id\=(.+?)\)','Session\ ID\:\ (.+?)$',
         'Query\ submitted\:.+\((.+?)\)$','Planning\ finished\:.+\((.+?)\)$',
         'Ready\ to\ start\ on\ \d+\ backends\:.+\((.+?)\)$','First\ row\ fetched\:.+\((.+?)\)$',
         'ClientFetchWaitTimer\:\ (.+?)$','RowMaterializationTimer\:\ (.+?)$']

 content = urllib2.urlopen(r).read()
 s = ""
 val = ""
 for rgx in rgxs:
   m = re.findall(rgx, content, re.MULTILINE)
   if m:
 
        try:
         val = durationpy.from_str(str(m[-1])).total_seconds() * 1000
        except:
         val = str(m[-1])

        s += "%s\t" % val

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
