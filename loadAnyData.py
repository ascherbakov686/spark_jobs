import urllib,urllib2
from urllib2 import urlopen
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import re

def getUrls():
 #url="http://host.domain/custom/testdata/"
 url="http://host.domain:38888/"
 l = []
 urlpath=urlopen(url)
 string=urlpath.read().decode('utf-8')
 #pattern=re.compile('data[1-4].csv"')
 pattern=re.compile('part0.log"')

 filelist=pattern.findall(string)

 for filename in filelist:
     filename=filename[:-1]
     l.append(url+filename)

 return l

def saveData(url):

 r = urllib2.Request(url)

 #if multiple space delimiter
 #s = ' '.join((urllib2.urlopen(r).read()).replace(","," ").split())

 # full file read
 s = (urllib2.urlopen(r).read()).replace(",",'\t')

 # limited file read
 #s = ""
 #for x in range(100):
 #         s += (urllib2.urlopen(r).readline()).replace(",",'\t')

 return s


def saveData2(line):
  s = line.replace(",",'\t')
  return s


def main():

 customSchema = StructType([ \
    StructField("md5", StringType(), True), \
    StructField("timestamp", StringType(), True), \
    StructField("scan_result", StringType(), True), \
    StructField("verdict", StringType(), True), \
    StructField("file_size", StringType(), True), \
    StructField("file_type", StringType(), True)])

 spark = SparkSession.builder.appName('loadAnyData').enableHiveSupport().getOrCreate()
 sc = spark.sparkContext
 log4j = sc._jvm.org.apache.log4j
 log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)

 #rdd = sc.parallelize(getUrls())
 rdd = sc.textFile("hdfs://nameservice1/tmp/1/*.log")
 #rdd = sc.textFile("hdfs://nameservice1/tmp/1/result.txt")
 #c = spark.read.option("delimiter",' ').csv(rdd.map(saveData),header=False,schema=customSchema)
 #c = spark.read.option("delimiter",'\t').csv(rdd.map(saveData),header=False,schema=customSchema)
 c = spark.read.option("delimiter",'\t').csv(rdd.map(saveData2),header=False,schema=customSchema)
 #c = spark.read.option("delimiter",',').csv(rdd,header=True)
 c.show()
 c.printSchema()
 #c.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/1.db/table")
 #c.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/1.db/table")
 #c.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/1.db/table")
 #c.write.format("parquet").mode("overwrite").option("compression","gzip").save("/user/hive/warehouse/1.db/table")
 #c.write.format("parquet").mode("append").option("compression","gzip").save("/user/hive/warehouse/1.db/table")
 spark.stop()

if __name__ == '__main__':
 main()


#########
# CREATE EXTERNAL TABLE 1.table (
# md5 STRING,
# timestamp STRING,
# s STRING,
# v STRING,
# file_size STRING,
# file_type STRING )
# STORED AS PARQUET LOCATION 'hdfs://nameservice1/user/hive/warehouse/1.db/table';
#########
