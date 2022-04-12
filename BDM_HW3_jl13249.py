import csv
import json

import IPython

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import DoubleType


from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split


keyfood_products_1 = spark.read.load('/tmp/bdm/keyfood_products.csv',format='csv',header = True,inferSchema=False).select('store','upc','product','price')
keyfood_products_1 = keyfood_products_1.withColumn('upc', split(keyfood_products_1 ['upc'], '-').getItem(1))

keyfood_sample_items = spark.read.load('keyfood_sample_items.csv',format='csv',header = True,inferSchema=False)
keyfood_sample_items = keyfood_sample_items.withColumn('UPC code', split(keyfood_sample_items['UPC code'], '-').getItem(1))

cond1 = [keyfood_sample_items['UPC code']==keyfood_products_1['upc']]
df1 = keyfood_sample_items.join(keyfood_products_1, cond1, how = 'left')
df1 = df1.select('store','upc', 'product', 'price')

df_store = spark.read.load('keyfood_nyc_stores.json',format='json',header = True,inferSchema=False)
keyfood_nyc_stores = {x:df_store.select(x+'.foodInsecurity').first()['foodInsecurity'] for x in df_store.columns}
callnewColsUdf = F.udf(lambda x: keyfood_nyc_stores[x],T.DoubleType())
df  = df1.withColumn('foodInsecurity',callnewColsUdf(F.col('store')))

# price
df  = df.withColumn("price", regexp_extract("price", "\d+\.?\d*", 0))
df = df.withColumn("price",df["price"].cast(DoubleType()))

# foodInsecurity
outputTask1 = df.withColumn("foodInsecurity", 100*df["foodInsecurity"]).select('product', 'price','foodInsecurity')


# ## DO NOT EDIT BELOW
outputTask1 = outputTask1.cache()
outputTask1.count()