import csv
import pyspark
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import sys
from pyspark.sql.functions import year, month, dayofmonth
from pyspark.sql.functions import monotonically_increasing_id as mi
from pyspark.sql.functions import when, col
from pyspark.sql.functions import explode
from pyspark.sql.functions import udf, first
from pyspark.sql.types import ArrayType, FloatType, StringType, IntegerType
import json
from pyproj import Geod
sc = pyspark.SparkContext.getOrCreate()
spark = SparkSession(sc)

nyc_sup = spark.read.load('/content/drive/MyDrive/BDM/final/nyc_supermarkets.csv', format='csv', header=True, inferSchema=True).cache()
nyc_cbg_cen = spark.read.load('/content/drive/MyDrive/BDM/final/nyc_cbg_centroids.csv', format='csv', header=True, inferSchema=True).cache()
weekly_pattern = spark.read.option('escape','"').load('weekly-patterns-nyc-2019-2020-sample.csv', format='csv', header=True, inferSchema=True).select('placekey', 'poi_cbg', 'visitor_home_cbgs', 'date_range_start','date_range_end')\
      .withColumn('date_range_start_substr',col('date_range_start').substr(0,7))\
      .withColumn('date_range_end_substr',col('date_range_end').substr(0,7))
join_weeklyPattern_nycSup = weekly_pattern.join(nyc_sup, weekly_pattern.placekey==nyc_sup.safegraph_placekey, how='inner') 

def func_date(start,end):
  if start == '2019-03' or end == '2019-03':
    return '2019-03'
  elif start == '2019-10' or end == '2019-10':
    return '2019-10'
  elif start == '2020-03' or end == '2020-03':
    return '2020-03'
  elif start == '2020-10' or end == '2020-10':
    return '2020-10'
  else:
    return ''

UDF_date = udf(func_date,StringType())
df_time = join_weeklyPatterns_nycSup.withColumn("date", UDF_date('date_range_start_substr','date_range_end_substr'))
df_time = df_time.filter(col('date') != '').dropDuplicates().select('placekey', 'visitor_home_cbgs','latitude','longitude','date')

def func_vh_cbgs(vh_cbgs, lat, long):
    vhs = json.loads(vh_cbgs)
    distance_list = []
    for id in vhs:
      distance_list.append(id)
    return distance_list

UDF_number = udf(func_vh_cbgs,ArrayType(StringType()))
df_distance = df_time.withColumn("distance_list", UDF_number("visitor_home_cbgs",'latitude','longitude'))
df_distance = df_distance.select(df_distance.placekey,df_distance.latitude.alias('org_lat'),df_distance.longitude.alias('org_lon'),explode(df_distance.distance_list),'date')
df_distance = df_distance.join(nyc_cbg_cen,df_distance.col==nyc_cbg_cen.cbg_fips).dropDuplicates().repartition(1).cache()

wgs84_geod = Geod(ellps='WGS84')
def func_distance(org_lat, org_lon, lat, lon):
    az12, az21, dist = wgs84_geod.inv(org_lon, org_lat, lon, lat)
    distance = dist * 0.00062137
    return distance

UDF_distance = udf(func_distance,FloatType())
df_distance = df_distance.withColumn("distance", UDF_distance("org_lat",'org_lon','latitude','longitude'))
df_distance_date = df_distance.select('placekey','distance','date')

df_distance_date = df_distance_date.groupBy(['placekey','date']).pivot('date',['2019-03','2019-10','2020-03','2020-10']).agg({'distance': 'mean'}).drop('date')
df_final = df_distance_date.groupBy('placekey').sum()
df_final = df_final.select(col("placekey").alias("cbg_fips"),col("sum(2019-03)").alias("2019-03"),col("sum(2019-10)").alias("2019-10"),col("sum(2020-03)").alias("2020-03"),col("sum(2020-10)").alias("2020-10"))

df_final.rdd.saveAsTextFile(sys.argv[1])
