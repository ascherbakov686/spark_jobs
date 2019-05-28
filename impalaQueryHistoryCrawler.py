import urllib,urllib2,json,base64
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
import pprint
import ssl

def getUrls():
 cdh="http://cdh.domain:7180"
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
 password="*******************"
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
 rdd = sc.parallelize(getUrls())
 j = spark.read.json(rdd.map(saveData))
 j.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/impala_queries.db/history")
 spark.stop()

if __name__ == '__main__':
 main()
