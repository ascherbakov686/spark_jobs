from StringIO import StringIO
from zipfile import ZipFile
from urllib import urlopen
from pyspark.sql import SparkSession
import subprocess
#import ssl

def getUrls():

 l = []

 #l.append("http://geolite.maxmind.com/download/geoip/database/GeoLite2-City-CSV.zip")
 #l.append("http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country-CSV.zip")
 #l.append("http://geolite.maxmind.com/download/geoip/database/GeoLite2-ASN-CSV.zip")

 l.append("http://infra-node1.domain.ru/custom/GeoLite2-City-CSV.zip")
 l.append("http://infra-node1.domain.ru/custom/GeoLite2-Country-CSV.zip")
 l.append("http://infra-node1.domain.ru/custom/GeoLite2-ASN-CSV.zip")

 #l.append("http://infra-node1.domain.ru/custom/IP2LOCATION-LITE-DB1.CSV.ZIP")
 #l.append("http://infra-node1.domain.ru/custom/IP2LOCATION-LITE-DB1.IPV6.CSV.ZIP")

 return l

def saveData(url):
 #proxies = {'http':'http://Av5r82B1oCUJWoiVyaXh.oc.domain.ru'}
 csvs = {}
 #ssl._create_default_https_context = ssl._create_unverified_context
 zf = ZipFile(StringIO(urlopen(url).read()))
 #zf = ZipFile(StringIO(urlopen(url,proxies=proxies).read()))

 for info in zf.infolist():
     if info.filename.lower().endswith('.csv'):
        csv = { info.filename:zf.read(info)  }
        csvs.update(csv)
 return csvs

def main():
 spark = SparkSession.builder.appName('updateIP2Location').enableHiveSupport().getOrCreate()
 sc = spark.sparkContext
 log4j = sc._jvm.org.apache.log4j
 log4j.LogManager.getRootLogger().setLevel(log4j.Level.WARN)
 rdd = sc.parallelize(getUrls())

 for csvs in rdd.map(saveData).collect():
    for filename,csv in csvs.items():
        tbl=filename[filename.find('/')+1:filename.find('.')].replace('-', '_')
        print tbl
        c = spark.read.option("header","true").csv(sc.parallelize(csv.splitlines()))
        # debug
        #c.printSchema()
        fields = c.schema.fields
        li = []
        for f in fields:
            li.append("%s %s" % (f.name,f.dataType.typeName()))

        q1 = "create external table if not exists %s (" % tbl
        q2 = ") stored as parquet location '/user/hive/warehouse/ip2location.db/%s'" % tbl
        fi = ','.join(li)
        q3 = q1+fi+q2

        # debug
        #print q3

        ret = subprocess.check_output("hdfs dfs -mkdir -p /user/hive/warehouse/ip2location.db/%s" % tbl,shell=True)
        print ret

        c.write.format("parquet").mode("overwrite").option("compression","gzip").save("/user/hive/warehouse/ip2location.db/%s" % tbl)

        ret = subprocess.check_output("impala-shell --ssl -k --quiet -B -d ip2location -q \"%s\"" % q3,shell=True)
        print ret

        ret = subprocess.check_output("impala-shell --ssl -k --quiet -B -d ip2location -q \"refresh %s\"" % tbl,shell=True)
        print ret

        # ret = subprocess.check_output("impala-shell --ssl -k --quiet -B -d statlake -q '%s'" % q3,shell=True)
        # print ret

        # ret = subprocess.check_output("impala-shell --ssl -k --quiet -B -d statlake -q 'refresh %s'" % tbl,shell=True)
        # print ret

 spark.stop()

if __name__ == '__main__':
 main()
