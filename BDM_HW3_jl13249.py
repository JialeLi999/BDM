import csv
import json
import numpy as np
# import matplotlib.pyplot as plt
# import seaborn as sns
import pandas as pd
import IPython

IPython.display.set_matplotlib_formats('svg')
pd.plotting.register_matplotlib_converters()
sns.set_style("whitegrid")

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

from pyspark.sql.session import SparkSession
from pyspark.sql.functions import split
spark = SparkSession(sc)

keyfood_products_1 = spark.read.load('/tmp/bdm/keyfood_products.csv',format='csv',header = True,inferSchema=False).select('store','upc','product','price')
keyfood_products_1 = keyfood_products_1.withColumn('upc', split(keyfood_products_1 ['upc'], '-').getItem(1))

keyfood_sample_items = spark.read.load('keyfood_sample_items.csv',format='csv',header = True,inferSchema=False)
keyfood_sample_items = keyfood_sample_items.withColumn('UPC code', split(keyfood_sample_items['UPC code'], '-').getItem(1))

cond1 = [keyfood_sample_items['UPC code']==keyfood_products_1['upc']]
df1 = keyfood_sample_items.join(keyfood_products_1, cond1, how = 'left')
df1 = df1.select('store','upc', 'product', 'price')

keyfood_nyc_stores = pd.read_json('keyfood_nyc_stores.json')
keyfood_nyc_stores = keyfood_nyc_stores.T[['name','communityDistrict','foodInsecurity']]
spark = SparkSession.builder.appName("Python Spark SQL Hive integration example").enableHiveSupport().getOrCreate()
keyfood_nyc_stores = spark.createDataFrame(keyfood_nyc_stores)
cond2 = [df1['store']==keyfood_nyc_stores['name']]
df_task1 = df1.join(keyfood_nyc_stores, cond2, how = 'left')
df_task1 = df_task1.select('product', 'price','foodInsecurity')


from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import DoubleType

# price
df_task1  = df_task1.withColumn("price", regexp_extract("price", "\d+\.?\d*", 0))
df_task1 = df_task1.withColumn("price",df_task1["price"].cast(DoubleType()))

# foodInsecurity
outputTask1 = df_task1.withColumn("foodInsecurity", 100*df_task1["foodInsecurity"])


## DO NOT EDIT BELOW
outputTask1 = outputTask1.cache()
print(outputTask1.count())

#
# def dfTask1(data):
#     rdd = data.rdd if hasattr(data, 'rdd') else data
#     if rdd.count()>10000:
#         raise Exception('`outputTask1` has too many rows')
#     rows = map(lambda x: (x[0], x[1], int(x[2])), rdd.collect())
#     return pd.DataFrame(data=rows, columns=['Item Name','Price ($)','% Food Insecurity'])
#
# def plotTask1(data, figsize=(8,8)):
#     itemNames = pd.read_csv('keyfood_sample_items.csv')['Item Name']
#     itemKey = dict(map(reversed,enumerate(itemNames)))
#     df = dfTask1(data).sort_values(
#         by = ['Item Name', '% Food Insecurity'],
#         key = lambda x: list(map(lambda y: itemKey.get(y,y), x)))
#     plt.figure(figsize=figsize)
#     ax = sns.violinplot(x="Price ($)", y="Item Name", data=df, linewidth=0,
#                         color='#ddd', scale='width', width=0.95)
#     idx = len(ax.collections)
#     sns.scatterplot(x="Price ($)", y="Item Name", hue='% Food Insecurity', data=df,
#                     s=24, linewidth=0.5, edgecolor='gray', palette='YlOrRd')
#     for h in ax.legend_.legendHandles:
#         h.set_edgecolor('gray')
#     pts = ax.collections[idx]
#     pts.set_offsets(pts.get_offsets() + np.c_[np.zeros(len(df)),
#                                             np.random.uniform(-.1, .1, len(df))])
#     ax.set_xlim(left=0)
#     ax.xaxis.grid(color='#eee')
#     ax.yaxis.grid(color='#999')
#     ax.set_title('Item Prices across KeyFood Stores in NYC')
#     return ax
#
# if 'outputTask1' not in locals():
#     raise Exception('There is no `outputTask1` produced in Task 1.A')
#
# plotTask1(outputTask1)