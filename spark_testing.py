from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import *
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import StringType, IntegerType
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc.stop()
sc = SparkContext("local",'app')
spark = SparkSession.builder.appName('name').config('spark.sql.shuffle.partitions',10).getOrCreate()

class DealDate():
    def pre_data(self):
        df = spark.read.csv('Demo_Data.csv', header=True, inferSchema=True) 
    def acquire_info(self):
        print("please enter you want information, eg: src >= 2010 and dst <= 2019 and name = 'lenovo' and btc>0")
            global conditions = int(input("enter all condition："))
    def fileters(self):
        df.filter(self.conditions)\  
                 .select(*)\
                 .show(truncate=False) 

if __name__ == '__main__':
    dd = DealData()
    dd.acquire_info()
    dd.filters()

# ----------------------
from pyspark import SparkContext
from pyspark.sql import SparkSession
sc.stop()
sc = SparkContext("local")
spark = SparkSession(sc)
from operator import add
import pandas as pd


class DealDate():
    def pre_data(self):
        data = pd.read_csv('Demo_Data.csv')
        data['src'] = pd.to_datetime(data['src'], unit='s')
        data['dst'] = pd.to_datetime(data['dst'], unit='s')
        gap = data['src'] - data['dst']
        data['dst_src_gap'] = gap
        data.head()#看前五行
        data.to_csv('rawData.csv')
        lines = sc.textFile('rawData.csv')
        counts = lines.flatMap(lambda x: x.split('\n')) \
                      .map(lambda x: (x, 1)) \
                      .reduceByKey(add) 
        print (counts.take(100000))        
    def acquire_info(self):
        print("please enter you want information, eg: src >= 2010 and dst <= 2019 and name = 'lenovo' and btc>0")
            conditions = int(input("enter all condition："))
            self.filter(conditions)        
    def fileters(self):
        df.filter(user_input_string)\  
                 .select(*)\
                 .show(truncate=False) 

if __name__ == '__main__':
    dd = DealData()
    dd.acquire_info()
    dd.filters()