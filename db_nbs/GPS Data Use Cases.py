# Databricks notebook source
# MAGIC %md
# MAGIC # Interpolation of GPS Data
# MAGIC 
# MAGIC > This notebook is being developed to be semi easy to follow along or be able to come in and see what each piece is doing this might change in the future, but this will hopefully be a nice learning ground for those that want to get their hands dirty in Databricks, and yes that means you are cheating on snowflake 

# COMMAND ----------

! python -m pip install --upgrade pip
! pip install shapely
! pip install pyproj
! pip install geopandas
! pip install descartes

# COMMAND ----------

from pyspark.sql.types import * 
from pyspark.sql.functions import *
from pyspark.sql import functions, Window
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt
from matplotlib import pyplot as plt
from functools import reduce

import pandas as pd
import pyspark.sql.functions as func

# COMMAND ----------

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, Polygon, shape
from shapely import wkb, wkt
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType,DecimalType
from pyspark.sql.functions import pandas_udf, PandasUDFType
import shapely.speedups
shapely.speedups.enable() # this makes some spatial queries run faster

# COMMAND ----------

# MAGIC %md ## Create a Mount for Bronze Level Data

# COMMAND ----------

ACCOUNT_KEY = "EzJJ19jNqJfABvCc/39ygXONjvpjZYdoQJ9XtpGpoy/Mcv22lWDCCeKkiD8wXUJYtCnTMbOyVz2Q6SkUyoX4aA=="
BLOB_CONTAINER = "lumiplan-data"
BLOB_ACCOUNT = "vaildtscadls"
MOUNT_PATH = "/mnt/lumiplan-data/"

# COMMAND ----------

# dbutils.fs.unmount(MOUNT_PATH)

# dbutils.fs.mount(
#   source = f"wasbs://{BLOB_CONTAINER}@{BLOB_ACCOUNT}.blob.core.windows.net/",
#   mount_point = MOUNT_PATH,
#   extra_configs = {
#     f"fs.azure.account.key.{BLOB_ACCOUNT}.blob.core.windows.net":ACCOUNT_KEY
#   }
# )

# COMMAND ----------

dbutils.fs.ls('/mnt/lumiplan-data/bronze/')

# COMMAND ----------

# MAGIC %md ## Eventual API Configuration (Streaming Redis, Spark, other Apache Frame Works)

# COMMAND ----------

# TODO: Begin to Test API when Credentials Come Out

# COMMAND ----------

# Originally this data was given in a google drive we will eventually be adding most like
# path = 'https://drive.google.com/u/0/uc?export=download&confirm=rBxJ&id=1iEoMSN4IGpFGUonX7tEumOR37qbUwT2x'
# geo = pd.read_csv(path)
# for col in geo.columns:
#     geo[col] = pd.to_numeric(geo[col], errors='coerce')
# geo.dropna(inplace=True)
# geo['skier_id'] = geo['skier_id'].astype(int)
# geo['timestamp'] = geo['timestamp'].astype(int)
# geo['timestamp'] = pd.to_datetime(geo['timestamp'],unit='s')
# geo.dtypes
# df = spark.createDataFrame(geo)
# df.write.parquet(f'{MOUNT_PATH}sample-extract/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Pull From Data Lake

# COMMAND ----------

geo_schema = StructType([
    StructField('skier_id', IntegerType()),
    StructField('latitude', DoubleType()),
    StructField('longitude', DoubleType()),
    StructField('timestamp', TimestampType()),
])

df = spark.read.parquet(
    '/mnt/lumiplan-data/sample-extract/',
    schema=geo_schema,
)

( df
    .write
    .format('delta')
    .mode('overwrite')
    .save('/mnt/lumiplan-data/bronze/sample-extract')
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md ## Create DB Database

# COMMAND ----------

# MAGIC %md ### Skier Data From Lumiplan
# MAGIC 
# MAGIC > This data is in the most raw form that we can get and no preprocessing has been done to this data yet in the future this would be the table where we are doing merge update statement using delta calls to continously update these tables and then push to a silver location where interpolation could be completed with a gold level of data that is use in production level applications a True DEV, UAT, PROD with some expections as we begin to build this data asset out

# COMMAND ----------

# _ = spark.sql('CREATE DATABASE lumiplan_data')
# _ = spark.sql('''
#   CREATE TABLE lumiplan_data.bronze_sample_extract
#   USING DELTA 
#   LOCATION '/mnt/lumiplan-data/bronze/sample-extract'
#   ''')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating a DIMDATEMINUTE Data Source

# COMMAND ----------

# MAGIC %md > This is a simple POC so note that we would change this to be created for many years out so that we wouldn't need to make this again until we got to 2025 or something like that with a simple inner join on the date and minutes similar to what we do for grid like problems where missing data can appear. One thing about time data or day level data is that it's very easy to ensure or see where we are going to be missing data as we can create a grid to join to. This is something RDE does an extremely good job of

# COMMAND ----------

# one of the powerful things about spark is that you can use sql and spark
# the idea behind snowpark and spark also support UDFs asa well just different syntax
dim_time = (
    spark.sql('''SELECT sequence(to_timestamp('2022-01-31T01:00:00'), to_timestamp('2022-02-05T23:59:00'), interval 1 minute) as timestamp_list''')
        .select("*", explode("timestamp_list").alias("timestamp"))
        .drop('timestamp_list')
)
dim_time = (
    dim_time
        .withColumn('date', to_date(dim_time.timestamp))
)
display(dim_time)

# COMMAND ----------

( dim_time
    .write
    .format('delta')
    .partitionBy('date')
    .mode('overwrite')
    .save('/mnt/lumiplan-data/dims/dim-time')
  )
( dim_time
    .write
    .format('delta')
    .partitionBy('date')
    .mode('overwrite')
 # Note this isn't a bronze needed database because this data
 # is already at a stage where it's being created you could
 # just create a new row each time, but some things are better done
 # in bulk so that there is less that can go wrong
 # in the future this might also get written to gold as well if we
 # think that this project needs it
    .save('/mnt/lumiplan-data/bronze/dims/dim-time')
  )
( dim_time
    .write
    .format('delta')
    .partitionBy('date')
    .mode('overwrite')
 # Note this isn't a bronze needed database because this data
 # is already at a stage where it's being created you could
 # just create a new row each time, but some things are better done
 # in bulk so that there is less that can go wrong
 # in the future this might also get written to gold as well if we
 # think that this project needs it
    .save('/mnt/lumiplan-data/silver/dims/dim-time')
  )
spark.sql('''
  CREATE TABLE lumiplan_data.DIM_DATE_MINUTE
  USING DELTA 
  LOCATION '/mnt/lumiplan-data/bronze/dims/dim-time'
  ''')

# COMMAND ----------

# MAGIC %md # Taking a Look At One Day Dealing with Interpolation
# MAGIC 
# MAGIC > Take our word for it we are going to have missing data and we are going to have to interpolate you can see this by plotting a time series by minute, but we are going to skip that part and work on the interpolation method. Please feel free to clone this notebook and create better ways there is really never one way to an answer

# COMMAND ----------

day_1 = spark.sql("""
select 
      DISTINCT geo.skier_id
    , geo.latitude
    , geo.longitude
    , from_utc_timestamp(geo.timestamp, 'MST') as skier_timestamp
  from lumiplan_data.bronze_sample_extract geo
  where
        date(from_utc_timestamp(geo.timestamp, 'MST')) = '2022-02-05'
--     and skier_id in (3114)
    and skier_id in (314430, 313607, 3114)
    and date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') >= '07:00:00'
    and date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') <= '17:00:00'
""")
display(day_1)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Time Join

# COMMAND ----------

df = spark.sql('''
/*
Creating the grid from dim_time
*/
with cross_time_skiers as
(
  select
      dt.timestamp
    , dt.date
    , geo.skier_id
  from lumiplan_data.DIM_DATE_MINUTE dt
--   cross join (select distinct skier_id from lumiplan_data.bronze_sample_extract WHERE SKIER_ID in (3114)) geo
  cross join (select distinct skier_id from lumiplan_data.bronze_sample_extract WHERE SKIER_ID in (314430, 313607, 3114)) geo
--   cross join (select distinct skier_id from lumiplan_data.bronze_sample_extract) geo
  where dt.date = '2022-02-05'
    and date_format(dt.timestamp, 'HH:mm:ss') >= '09:00:00'
    and date_format(dt.timestamp, 'HH:mm:ss') <= '17:00:00'
),
/*
This table is going to grab all the data we have on
every skier, but right now it's only on one skier so
we most likely will have room to improve this sql moving forward
*/
raw_geo as
(
  select 
      geo.skier_id
    , geo.latitude
    , geo.longitude
    , from_utc_timestamp(geo.timestamp, 'MST') as skier_timestamp
  from lumiplan_data.bronze_sample_extract geo
  where
        date(from_utc_timestamp(geo.timestamp, 'MST')) = '2022-02-05'
--     and skier_id in (3114)
    and skier_id in (314430, 313607, 3114)
    and date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') >= '07:00:00'
    and date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') <= '17:00:00'
    
 
)
/*
We are going to create the now create avg 
SkierID, LAT, LOG, Total GPS Pings, TimeStamp by minute

This is where we know missing data will come about and
why we will need to create a window function to iterploate
where they might be or where they went.

As we move forward we want to begin to think about where they are
actually stopping if it's next to a lift line it ~ means they
are waiting in line and will need to be in the wait time 
calculation with a reasonable amount of time as other lumiplan
does this now but has very little control/understanding of 
how they can adjust this for us so having the data like this 
could lead to huge % increase in accuarcy of a true waitime at
each lift

Note: that we know the data comes in in the miliseconds to seconds
so this is a decision to round up to the minute. For a 
health and saftey perspective a second data model might be helpful
*/
select
    skier_id
  , avg(latitude) as latitude
  , avg(longitude) as longitude
  , sum(case when longitude is null then 0 else 1 end) as total_gps_points
  , agg_timestamp as timestamp
  , date
from
(
  select 
      dt.skier_id
    , geo.latitude
    , geo.longitude
    , geo.skier_timestamp
    , dt.timestamp as agg_timestamp
    , dt.date
  from cross_time_skiers dt
  left join raw_geo geo
    on  (unix_timestamp(dt.timestamp) - unix_timestamp(geo.skier_timestamp)) > -30
    and (unix_timestamp(dt.timestamp) - unix_timestamp(geo.skier_timestamp)) <= 30
    and dt.date = date(geo.skier_timestamp)
    and dt.skier_id = geo.skier_id
)
group by
    skier_id
  , agg_timestamp
  , date
order by 
    skier_id
  , agg_timestamp
''')
# There we go we see the nulls now some are okay because some the person just isn't at the mountain
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Windowing Functions

# COMMAND ----------

# lets create a column that is going to anchor our data for a first time seen
df = df.withColumn("time_hit", when(df.latitude.isNull(), None).otherwise(df.timestamp))
display(df.filter(df.time_hit.isNotNull()))

# COMMAND ----------

# create forward and backward windows standard look forward and backwards
window_ff = Window.partitionBy(['skier_id', 'date'])\
                  .orderBy('timestamp')\
                  .rowsBetween(-120, 0)
window_bf = Window.partitionBy(['skier_id', 'date'])\
                  .orderBy('timestamp')\
                  .rowsBetween(0, 120)

# COMMAND ----------

# get latitude difference
lat_last = func.last(df['latitude'], ignorenulls=True).over(window_ff)
lat_next = func.first(df['latitude'],ignorenulls=True).over(window_bf)
long_last = func.last(df['longitude'], ignorenulls=True).over(window_ff)
long_next = func.first(df['longitude'], ignorenulls=True).over(window_bf)

# get timing difference
time_last = func.last(df['time_hit'], ignorenulls=True).over(window_ff)
time_next = func.first(df['time_hit'], ignorenulls=True).over(window_bf)

# COMMAND ----------

# I want everyone to undestand that spark is sql and sql is spark it's all syntax
# If you know sql you can do spark or if you know pandas you can do spark
# if you dyplr you can do spark 
lat_last

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution

# COMMAND ----------

# add columns to the dataframe
df_filled = df.withColumn('time_value_ff', time_last)\
              .withColumn('time_value_bf', time_next)\
              .withColumn('lat_value_ff', lat_last)  \
              .withColumn('lat_value_bf', lat_next)  \
              .withColumn('long_value_ff', long_last)\
              .withColumn('long_value_bf', long_next)
display(df_filled.filter(df_filled.time_value_ff.isNotNull()))

# COMMAND ----------

# finalize gaps and LAT and long we want to use
df_filled = df_filled.withColumn('total_gap', (unix_timestamp('time_value_bf') - unix_timestamp('time_value_ff')) / 60)
df_filled = df_filled.withColumn('current_gap', (unix_timestamp('time_value_bf') - unix_timestamp('timestamp')) / 60)
df_filled = df_filled.withColumn('latitude_int',
    coalesce(
        df_filled.latitude,
        df_filled.lat_value_ff + (df_filled.total_gap - df_filled.current_gap) * (df_filled.lat_value_bf - df_filled.lat_value_ff) / (df_filled.total_gap),
    )
)
df_filled = df_filled.withColumn('longitude_int',
    coalesce(
        df_filled.longitude,
        df_filled.long_value_ff + (df_filled.total_gap - df_filled.current_gap) * (df_filled.long_value_bf - df_filled.long_value_ff) / (df_filled.total_gap),
    )
)

# COMMAND ----------

display(df_filled.filter(
    ((hour(df.timestamp) > 9) & (hour(df.timestamp) <= 11))
)
 )

# COMMAND ----------

display(
    df_filled[[
        'skier_id',
        'timestamp',
        'latitude',
        'longitude',
        'latitude_int',
        'longitude_int',
        'lat_value_ff',
        'lat_value_bf',
        'time_value_ff',
        'time_value_bf',
        'total_gap',
        'current_gap',
    ]].filter(
    ((hour(df.timestamp) > 9) & (hour(df.timestamp) <= 11))
)
)

# COMMAND ----------

# MAGIC %md ## Bringing in Lift Information
# MAGIC 
# MAGIC > Using Snowflake table at the moment a Data Asset Created by RDE

# COMMAND ----------

# Load Libraries
import logging 
logging.getLogger("py4j").setLevel(logging.ERROR)

# COMMAND ----------

import os
os.environ['sfAccount']='vailresorts.east-us-2.azure'
os.environ['sfDatabase']='MACHINELEARNINGOUTPUTS'
os.environ['sfPswd']='UEKF8ph4wbDP!'
os.environ['sfRole']='snowflake_ds_user'
os.environ['sfSchema']='DEV'
os.environ['sfUser']='SVC_DS_MANAGER_SF@vailresorts.com'
os.environ['sfWarehouse']='DATASCI_DEV'

# COMMAND ----------

from data_system_utilities.snowflake.query import Snowflake
import os

sf = Snowflake(
    sfAccount=os.environ['sfAccount'],
    sfUser=os.environ['sfUser'],
    sfPswd=os.environ['sfPswd'],
    sfWarehouse=os.environ['sfWarehouse'],
    sfDatabase=os.environ['sfDatabase'],
    sfSchema=os.environ['sfSchema'],
    sfRole=os.environ['sfRole']
)

df_lifts = sf.run_sql_str('''SELECT * 
    FROM ONMOUNTAIN.PROD.LIFTDIMENSIONS
    WHERE LATITUDE IS NOT NULL AND LONGITUDE IS NOT NULL
'''
)
df_lifts.columns = [x.lower() for x in df_lifts.columns]
display(df_lifts.head())
# df_lifts = spark.createDataFrame(df_lifts[['resortname', 'resortkey', 'liftname', 'latitude', 'longitude']]).write.parquet(f'{MOUNT_PATH}lift-data/')

lift_schema =StructType([
    StructField('resortname', StringType()),
    StructField('resortkey', IntegerType()),
    StructField('liftname', StringType()),
    StructField('latitude', FloatType()),
    StructField('longitude', FloatType()),
])
df_lifts = spark.read.parquet(
    '/mnt/lumiplan-data/lift-data/',
    schema=lift_schema,
)
( 
    df_lifts
    .write
    .format('delta')
    .mode('overwrite')
    .save('/mnt/lumiplan-data/bronze/lift-data/')
)

# COMMAND ----------

# _ = spark.sql('CREATE DATABASE lumiplan_data')
_ = spark.sql('''
  CREATE TABLE lumiplan_data.lift_data
  USING DELTA 
  LOCATION '/mnt/lumiplan-data/bronze/lift-data/'
  ''')

# COMMAND ----------

lift_data = spark.sql('''
SELECT *, (latitude, longitude) as lat_long
FROM lumiplan_data.lift_data
WHERE RESORTNAME = 'Park City' 
''')
display(lift_data)

# COMMAND ----------

# test = pd_lift_data.set_index('resortkey')[['lat_long']].T
# test.columns = [col[0] for col in lift_data.select('liftname').distinct().collect()]

# COMMAND ----------

# MAGIC %md ## Grabbing Geo Circles Created In Github Repo
# MAGIC 
# MAGIC [How Geo Circles Are Being Created](https://github.com/JeremyDemlow/EpicMixGuestGPSData/blob/main/nbs/00_GPS_POC.ipynb)
# MAGIC 
# MAGIC Eventually we would like to get out of this db env allowing for better documentation processes and being more iterative

# COMMAND ----------

from data_system_utilities.snowflake.query import Snowflake
from data_system_utilities.snowflake.utils import create_table_query_from_df
from matplotlib import pyplot as plt

from shapely.geometry import Point
from shapely.geometry.polygon import Polygon

# create new snowflake data asset
sf = Snowflake(
    sfAccount=os.environ['sfAccount'],
    sfUser=os.environ['sfUser'],
    sfPswd=os.environ['sfPswd'],
    sfWarehouse=os.environ['sfWarehouse'],
    sfDatabase=os.environ['sfDatabase'],
    sfSchema='WAITTIMES',
    sfRole=os.environ['sfRole']
)
geo_cirle_sp = sf.run_sql_str('''SELECT * FROM lat_lon_lift_circle_waitime_qa''')
geo_cirles

# COMMAND ----------

geo_cirles_sp = sf.run_sql_str('''SELECT * FROM lat_lon_lift_circle_waitime_qa_shapely''')
geo_cirles_sp

# COMMAND ----------

def lat_lon_in_cirle(geo_cricle_df, liftname, lat, lon):
    lift_name = 'Saddleback Express'
    lons_lats_vect = geo_cirles_sp[geo_cirles_sp.LIFTNAME == lift_name][['LAT', 'LON']].values
    polygon = Polygon(lons_lats_vect)
    point = Point(lat, lon) # create point
    if polygon.contains(point) or point.within(polygon) or polygon.touches(point):
        return True
    return False

# COMMAND ----------

test.shape

# COMMAND ----------

lift_name = 'Saddleback Express'

# COMMAND ----------

test[lift_name.replace(' ', '_')] = test.apply(lambda x: lat_lon_in_cirle(geo_cirles_sp, lift_name, x['latitude_int'], x['longitude_int']), axis=1)

# COMMAND ----------

test[['skier_id','latitude','longitude','total_gps_points', 'timestamp', 'latitude_int','longitude_int', 'distance','speed','km_from_Saddleback_Express','Saddleback_Express', 'with_in_Saddleback_Express']].head()

# COMMAND ----------

plt.rcParams["figure.figsize"] = (10,10)
plt.plot(test[~(test.latitude_int_next.isna())].loc[180:215]['km_from_Saddleback_Express'].reset_index(drop=True), label='distance')
plt.plot(test[~(test.latitude_int_next.isna())].loc[180:215].speed.reset_index(drop=True), label = 'speed')
plt.axhline(y=50, color = 'r', linestyle = '-')
plt.axhline(y=5, color = 'g', linestyle = '-')
plt.axhline(y=0, color = 'black', linestyle = '-')
plt.ylim(-10, 150)
plt.legend()
plt.show()

# COMMAND ----------

test_sample = test[test.with_in_Saddleback_Express]
test_sample.shape

# COMMAND ----------

sample_geo = geo_cirle_sp[geo_cirle_sp.LIFTNAME == lift_name]
sample_lift = pd_lift[pd_lift.liftname == lift_name]
plt.plot(test_sample.latitude_int, test_sample.longitude_int, 'x', color='g')
plt.plot(test_sample[test.with_in_Saddleback_Express].latitude_int, test_sample[test.with_in_Saddleback_Express].longitude_int, 'o', color='r')
plt.plot(test_sample[test.Saddleback_Express].latitude_int, test_sample[test.Saddleback_Express].longitude_int, 'o', color='b')
plt.plot(sample_lift.latitude, sample_lift.longitude, '^', label=lift_name+'lift_location' )
plt.plot(sample_geo.LAT, sample_geo.LON, label=lift_name+'current_poc_maze')
plt.ylim(-111.579092, -111.576092)
plt.xlim(40.676, 40.678)
plt.legend()
plt.show()

# COMMAND ----------

plt.plot(create_cirlce(pd_lift_data[pd_lift_data.liftname==x].latitude.values[0], pd_lift_data[pd_lift_data.liftname==x].longitude.values[0], 0.0009).set_index('lat').long, label=x)

# COMMAND ----------


plt.rcParams["figure.figsize"] = (10,10)
plt.legend(loc="upper left")
pd_lift_data = lift_data.toPandas()
plt.plot(df_speed.filter(df_filled.skier_id == 3114).toPandas().set_index('latitude_int').longitude_int, label='interpolated')
# for x in pd_lift_data[pd_lift_data.resortkey == 16].liftname.tolist():
for x in ['Sunrise', 'Saddleback Express', 'Peak 5',  'DreamScape', 'Motherlode Express', 'Sweet Pea']:
    plt.plot(create_cirlce(pd_lift_data[pd_lift_data.liftname==x].latitude.values[0], 
                           pd_lift_data[pd_lift_data.liftname==x].longitude.values[0], 0.0009).set_index('lat').long, label=x)
plt.legend()
plt.show()

# COMMAND ----------

df.apply(lambda x: get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094, axis=1)

# COMMAND ----------

# Think about saving the python object inside of snowflake might eliminate the need to recreate the polygon
# Quick attempt didn't seem to work, but not meaning it can't be done
lifts = geo_cirles_sp.LIFTNAME.unique()
for l in lifts:
    lons_lats_vect = geo_cirles_sp[geo_cirles_sp.LIFTNAME == l][['LAT', 'LON']].values
    


for idx, lat_long in enumerate([(lat, lon) for lat, long in zip(geo_cirles_sp.LAT, geo_cirles_sp.LON)]):
    print(lat_long)
#     lift_name = 'Saddleback Express'
#     lons_lats_vect = geo_cirles_sp[geo_cirles_sp.LIFTNAME == lift_name][['LAT', 'LON']].values
#     df.apply(lambda x: get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094, axis=1)
#     polygon = Polygon(lons_lats_vect) # create polygon
#     polygon.contains(point) # check if polygon contains point
#     point.within(polygon) # check if a point is in the polygon 
#     polygon.touches(point) # check if point lies on border of polygon 

# COMMAND ----------

for idx, lat_long in enumerate([(lat, long) for lat, long in zip(pd_lift.latitude, pd_lift.longitude)]):
    # 3281 ~ km --> feet # with in 150 feet
    test[f"km_from_{pd_lift.loc[idx].liftname.replace(' ', '_')}"] = df.apply(lambda x: get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094, axis=1)
    test[f"with_in_{pd_lift.loc[idx].liftname.replace(' ', '_')}"] = df.apply(lambda x: False if (get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094) > 50 else True, axis=1)

# COMMAND ----------

# Think about saving the python object inside of snowflake might eliminate the need to recreate the polygon
# Quick attempt didn't seem to work, but not meaning it can't be done
lift_name = 'Saddleback Express'
lons_lats_vect = geo_cirles_sp[geo_cirles_sp.LIFTNAME == lift_name][['LAT', 'LON']].values
polygon = Polygon(lons_lats_vect) # create polygon
print(polygon.contains(point)) # check if polygon contains point
print(point.within(polygon)) # check if a point is in the polygon 
print(polygon.touches(point)) # check if point lies on border of polygon 

# COMMAND ----------

# MAGIC %md ## Grabbing Next Data To allow For Simpler Feature Creation
# MAGIC 
# MAGIC > Could Creat a Bunch of Window Funtions For this, but this seems easy enough for right now most likely will need to be a silver or gold type of appraoch as this will need to thought about more as we go from and API perspective we know that we are going to be getting data constantly, but we also know that we need to save the raw data and then make features on it so need to think about what makes sense here, but for now this seems creative enough. 
# MAGIC 
# MAGIC With streaming data I will want to do research into these kind of things maybe it's a feature that isn't allowed for bronze or we need to keep it cached some how and then release the cache like with a stack or a heap/
# MAGIC queue like arch for this which would be pretty cool.

# COMMAND ----------

df_filled = df_filled.withColumn("timestamp_before", df_filled.timestamp + expr('INTERVAL -1 minutes')).withColumn('date', to_date(df_filled.timestamp))
display(df_filled.filter(((hour(df_filled.timestamp) > 9) & (hour(df_filled.timestamp) <= 11))))

# COMMAND ----------

# This will be like what would come into this dataframe
oldColumns = df_filled['skier_id', 'timestamp', 'date', 'latitude_int', 'longitude_int'].schema.names
newColumns = [f'{i}_next' for i in oldColumns]

# COMMAND ----------

df_next = reduce(lambda df_filled, idx: df_filled.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df_filled)
display(df_next[newColumns])

# COMMAND ----------

df_next = df_next[newColumns]

# COMMAND ----------

df_speed = df_filled.join(
    df_next,
    on = (
          (df_filled.timestamp_before == df_next.timestamp_next)
        & (df_filled.skier_id == df_next.skier_id_next)
    ),
    how = 'inner'
)
display(df_speed['skier_id', 'timestamp', 'latitude', 'longitude', 'latitude_int', 'longitude_int',
                 'total_gap', 'current_gap', 'timestamp_before', 
                 'latitude_int_next', 'longitude_int_next'])

# COMMAND ----------

# MAGIC %md ## Creating Distance Per Minute and Speed 

# COMMAND ----------

# def get_distance(lon_1, lat_1, lon_2, lat_2):
#     if lon_1 is None or lat_1 is None or lon_2 is None or lat_2 is None:
#         return 0
#     else:
#         R=6371000 # radius of Earth in meters
#         phi_1=radians(lat_1)
#         phi_2=radians(lat_2)

#         delta_phi=radians(lat_2-lat_1)
#         delta_lambda=radians(lon_2-lon_1)

#         a=sin(delta_phi/2.0)**2+\
#            cos(phi_1)*cos(phi_2)*\
#            sin(delta_lambda/2.0)**2
#         c=2*atan2(sqrt(a),sqrt(1-a))

#         meters=R*c                    # output distance in meters
#         miles=meters*0.000621371      # output distance in miles
#         return miles
# udf_get_distance = func.udf(get_distance, DoubleType())

# COMMAND ----------

def get_distance(longit_a, latit_a, longit_b, latit_b):
    if longit_a is None or latit_a is None or longit_b is None or latit_b is None:
        return None
    else:
        # Transform to radians
        longit_a = radians(longit_a)
        latit_a = radians(latit_a)
        longit_b = radians(longit_b)
        latit_b = radians(latit_b)
        dist_longit = longit_b - longit_a
        dist_latit = latit_b - latit_a
        # Calculate area
        area = sin(dist_latit/2)**2 + cos(latit_a) * cos(latit_b) * sin(dist_longit/2)**2
        # Calculate the central angle
        central_angle = 2 * asin(sqrt(area))
        radius = 6371000
        # Calculate Distance
        distance = central_angle * radius
        return distance * 0.000621371
udf_get_distance = func.udf(get_distance, DoubleType())

# COMMAND ----------

df_speed = df_speed.withColumn(
    'distance',
    when(func.col('longitude_int').isNotNull(), udf_get_distance(
        func.col('longitude_int'), func.col('latitude_int'), 
        func.col('longitude_int_next'), func.col('latitude_int_next')))
    .otherwise(0)
    
)
display(df_speed)

# COMMAND ----------

df_speed = df_speed.withColumn(
    'speed', when(df_speed.distance.isNotNull(), df_speed.distance * int(60))
            .otherwise(0)
)
display(df_speed)

# COMMAND ----------

from math import radians, cos, sin, asin, sqrt

def within_range(lon1, lat1):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Radius of earth in kilometers. Use 3956 for miles
    if c * r <= distance:
        return True
    else:
        return False
udf_within_range = func.udf(within_range, DoubleType())

# COMMAND ----------

pd_lift = lift_data.toPandas()
df = df_speed.toPandas()

# COMMAND ----------

test = df.copy()

# COMMAND ----------

for idx, lat_long in enumerate([(lat, long) for lat, long in zip(pd_lift.latitude, pd_lift.longitude)]):
    # 3281 ~ km --> feet # with in 150 feet
    test[f"km_from_{pd_lift.loc[idx].liftname.replace(' ', '_')}"] = df.apply(lambda x: get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094, axis=1)
    test[f"with_in_{pd_lift.loc[idx].liftname.replace(' ', '_')}"] = df.apply(lambda x: False if (get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 1094) > 50 else True, axis=1)

# COMMAND ----------

test.shape

# COMMAND ----------

test[~(test.latitude_int_next.isna())]

# COMMAND ----------

wait = test[['skier_id', 'timestamp', 'latitude_int', 'total_gap', 'current_gap',
             'longitude_int', 'distance', 'speed','with_in_Saddleback_Express',
             'with_in_Iron_Mountain_Express']][~(test.latitude_int_next.isna()) & (test['with_in_Saddleback_Express'] | test['with_in_Iron_Mountain_Express'])]
wait

# COMMAND ----------

# (wait['timestamp'].shift(-1) - wait['timestamp'].dt.total_seconds() / 60)
wait['test'] = ((pd.to_datetime(wait['timestamp'].shift(-1)) - pd.to_datetime(wait['timestamp'])).dt.total_seconds() / 60)
wait['test2'] = wait['test'].shift(1)
import numpy as np
wait['valid'] = np.where((wait.test == 1) | plo
                             (wait.test.isnull() & wait.test2 == 2)
                             , 1, 0)
wait['total_wait_time']=wait.groupby((wait['valid'] != wait['valid'].shift(1)).cumsum()).cumcount()+1
wait

# COMMAND ----------

test_spark = spark.createDataFrame(wait)
display(test_spark)

# COMMAND ----------

test_spark.createOrReplaceTempView("temp_view")
display(spark.sql(
"""
SELECT *
FROM temp_view
WHERE with_in_Saddleback_Express
"""))

# COMMAND ----------


display(spark.sql(
"""
SELECT timestamp
, AVG(total_wait_time) Avg_WaitTime_SaddleBack_Express
, AVG(total_wait_time) Avg_WaitTime_Iron_Mountain_Express
FROM temp_view
WHERE with_in_Saddleback_Express OR with_in_Iron_Mountain_Express
GROUP BY timestamp,  with_in_Saddleback_Express, with_in_Iron_Mountain_Express
"""))

# COMMAND ----------

display(spark.sql(
"""
SELECT skier_id, SUM(valid) as TotalWait, DATE(timestamp) date
FROM temp_view
WHERE with_in_Saddleback_Express
GROUP BY skier_id,  DATE(timestamp)
"""))

# COMMAND ----------

test[['skier_id', 'timestamp', 'latitude_int', 'total_gap', 'current_gap',
      'longitude_int', 'distance', 'speed','with_in_Saddleback_Express', 'with_in_Short_Cut', 'km_from_Short_Cut',
      'km_from_Saddleback_Express']][~(test.latitude_int_next.isna()) & (test['with_in_Saddleback_Express'] == True | test['with_in_Short_Cut']) == True].head(50)

# COMMAND ----------

test[~(test.latitude_int_next.isna()) & test['with_in_Saddleback_Express']]

# COMMAND ----------

plt.plot(test[~(test.latitude_int_next.isna()) & test['with_in_Saddleback_Express']]['km_from_Saddleback_Express'].reset_index(drop=True))
plt.plot(test[~(test.latitude_int_next.isna()) & test['with_in_Saddleback_Express']].speed.reset_index(drop=True))

# COMMAND ----------

test[~(test.latitude_int_next.isna())].loc[300:350]

# COMMAND ----------



# COMMAND ----------

plt.hist(test[~(test.latitude_int_next.isna())]['km_from_Saddleback_Express'].unique())

# COMMAND ----------

for idx, lat_long in enumerate([(lat, long) for lat, long in zip(pd_lift.latitude, pd_lift.longitude)]):
    df[f"km_from_{pd_lift.loc[idx].liftname.replace(' ', '_')}"] = df.apply(lambda x: (get_distance(x['longitude_int_next'], x['latitude_int_next'], lat_long[1], lat_long[0]) * 3281), axis=1)
    

# COMMAND ----------

results = []
for row in lift_data.rdd.toLocalIterator():
    lat1, lon1 = row[3], row[4]
    for row1 in df_speed.filter(((hour(df_filled.timestamp) > 9) & (hour(df_filled.timestamp) <= 11))).rdd.toLocalIterator():
        lat2, lon1 = row1[12], row1[13]
        results.append(get_distance(lon1, lat1, lon1, lat2))

# COMMAND ----------

results = []
for row in lift_data.rdd.toLocalIterator():
    lat1, lon1 = row[3], row[4]
    for row1 in df_speed.filter(((hour(df_filled.timestamp) > 9) & (hour(df_filled.timestamp) <= 11))).rdd.toLocalIterator():
        lat2, lon1 = row1[12], row1[13]
        results.append(get_distance(lon1, lat1, lon1, lat2))

# COMMAND ----------

from math import radians, cos, sin, asin, sqrt

def within_range(lon1, lat1, lon2, lat2, distance):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Radius of earth in kilometers. Use 3956 for miles
    print((c * r))
    if (c * r) <= distance:
        return True
    else:
        return False

center_point = [{'lat': 40.67555, 'lng': -111.561}]
test_point = [{'lat': -7.79457, 'lng': 110.36563}]

lat1 = 40.65291
lon1 = -111.51032
lat2 = 40.686392823764365
lon2 = -111.55302163931083
# lat2 = 40.68650289353013
# lon2 = -111.55949629086884

distance = 0.1524 # in kilometer

within_range(lon1, lat1, lon2, lat2, distance)

# COMMAND ----------

! pip install geopy

# COMMAND ----------

display(df_speed)

# COMMAND ----------

from geopy.distance import geodesic
import pyspark.sql.functions as F
import geopy
# geodesic(a, b).m

# COMMAND ----------

from pyspark.sql.functions import UserDefinedFunction

# COMMAND ----------

display(df_speed.withColumn('check', F.("latitude_int", "longitude_int")))

# COMMAND ----------

# udf_geo_distance = UserDefinedFunction(geopy.distance.geodesic, DoubleType())
# # WIP
# @F.udf(returnType=FloatType())
# def geodesic_udf(a, b):
#     return udf_geo_distance(a, b).miles

# @F.udf(returnType=FloatType())
# def haversine(lat1, lon1, lat2, lon2):
#     if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
#             return None
#     return 2*6371000*sqrt(pow(sin((lat2-lat1)/2),2) + cos(lat1)*cos(lat2)*pow(sin((lon2-lon1)/2),2))


# # df_speed = df_speed.withColumn('length_feet', geodesic_udf(F.array("latitude_int", "longitude_int"), F.array("latitude_int_next", "longitude_int_next")))

# display(df_speed.withColumn('test', haversine(F.col("latitude_int"), F.col("longitude_int"), F.col("latitude_int_next"), F.col("longitude_int_next"))))

# COMMAND ----------

lift_data.show()

# COMMAND ----------

display(lift_data)

# COMMAND ----------

watch = df_speed.toPandas()
pd_lift_data = lift_data.toPandas()
# .filter(((hour(df_filled.timestamp) > 9) & (hour(df_filled.timestamp) <= 11))).toPandas()

# COMMAND ----------

import numpy as np
def create_cirlce(x, y, bufferLength=0.001, polygonSides=180):
    """0.001 km ~ 32ft"""
    angles = np.linspace(0, 2 * np.pi, polygonSides, endpoint=False)
    # found online probably not right ha but it's neat on plot but need to work on the length formula
    points_list = [(x + np.sin(a) * bufferLength,
                    y + np.cos(a) * bufferLength)
                   for a in angles]
    return pd.DataFrame(points_list, columns=['lat', 'long'])

# COMMAND ----------

plt.rcParams["figure.figsize"] = (20,20)
plt.scatter(x=watch.latitude_int.values, y=watch.longitude_int.values)
# for lift in pd_lift_data[pd_lift_data.resortkey == 16].liftname.tolist():
for lift in [ 'Sunrise', 'Saddleback Express', 'Peak 5',  'DreamScape', 'Sweet Pea', 'Short Cut', 'Quicksilver Gondola - North', 'Iron Mountain Express', ]:
    x = pd_lift_data[pd_lift_data.liftname==lift].latitude.values[0]
    y = pd_lift_data[pd_lift_data.liftname==lift].longitude.values[0]
    plt.scatter(x=x,
                y=y, marker='x', lw=10, color='r', label=lift)
    cir_df = create_cirlce(pd_lift_data[pd_lift_data.liftname==lift].latitude.values[0], 
                              pd_lift_data[pd_lift_data.liftname==lift].longitude.values[0], 0.0009)
    plt.scatter(x=cir_df.lat.values, y=cir_df.long.values, color='g')
    plt.text(x, y, lift, fontsize=10)
    plt.legend()
#     plt.scatter(x=40.68307, y=-111.55659, marker='x', lw=10, color='g')

# COMMAND ----------

|
from matplotlib import pyplot as plt
import numpy as np
from matplotlib.animation import FuncAnimation 
  
# initializing a figure in 
# which the graph will be plotted
fig = plt.figure() 
   
# marking the x-axis and y-axis
axis = plt.axes(xlim =(39.0, 41.0), 
                ylim =(111.0, 115.0)) 
  
# initializing a line variable
line, = axis.plot([], [], lw = 10) 
   
# data which the line will 
# contain (x, y)
def init(): 
    line.set_data([], [])
    return line,
   
def animate(i):
    df = watch
    line.set_data(df.latitude_int.values, df.latitude_int.values)
      
    return line,
   
anim = FuncAnimation(fig, animate, init_func = init, frames = 10, interval = 1000, blit = True)
displayHTML(anim.to_jshtml())

# COMMAND ----------

from math import radians, cos, sin, asin, sqrt

def within_range(lon1, lat1, lon2, lat2, distance):
    # convert decimal degrees to radians 
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula 
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a)) 
    r = 6371000 # Radius of earth in kilometers. Use 3956 for miles
    if c * r <= distance:
        return True
    else:
        return False

center_point = [{'lat': 40.67555, 'lng': -111.561}]
test_point = [{'lat': -7.79457, 'lng': 110.36563}]

lat1 = 40.65291
lon1 = -111.51032
lat2 = 40.68650289353013
lon2 = -111.55949629086884

radius = 10000.00 # in kilometer

within_range(lon1, lat1, lon2, lat2, radius)

# COMMAND ----------

plt.rcParams["figure.figsize"] = (10,10)
plt.legend(loc="upper left")
pd_lift_data = lift_data.toPandas()
plt.plot(df_speed.filter(df_filled.skier_id == 3114).toPandas().set_index('latitude_int').longitude_int, label='interpolated')
# for x in pd_lift_data[pd_lift_data.resortkey == 16].liftname.tolist():
for x in ['Sunrise', 'Saddleback Express', 'Peak 5',  'DreamScape', 'Motherlode Express', 'Sweet Pea']:
    plt.plot(create_cirlce(pd_lift_data[pd_lift_data.liftname==x].latitude.values[0], 
                           pd_lift_data[pd_lift_data.liftname==x].longitude.values[0], 0.0009).set_index('lat').long, label=x)
plt.legend()
plt.show()

# COMMAND ----------

lat_long_lift = pd_lift_data[['lat_long']].T.reset_index(drop=True)
lat_long_lift.columns = [col[0] for col in lift_data.select('liftname').distinct().collect()]
lat_long_lift[['Sunrise', 'Saddleback Express']]

# COMMAND ----------

from math import sin, cos, sqrt, atan2, radians, pi
import numpy as np

bufferLength = 0.01  # ~32 feet
polygonSides = 360

create_circle(lift_data[lift_data.liftname=='3 Kings'].latitude, lift_data[lift_data.liftname=='3 Kings'].longitude)
x = 40.6507
y = -111.51032
x = lift_data[lift_data.liftname=='3 Kings'].latitude[0]
y = lift_data[lift_data.liftname=='3 Kings'].longitude[0]
# 40.65291, -111.51032
angles = np.linspace(0, 2 * np.pi, polygonSides, endpoint=False)
points_list = [(x + np.sin(a) * bufferLength,
                y + np.cos(a) * bufferLength)
               for a in angles]

# circle = spark.createDataFrame(pd.DataFrame(points_list, columns=columns))
circle = pd.DataFrame(points_list, columns=columns)
circle

# COMMAND ----------

import matplotlib.pyplot as plt
plt.rcParams['figure.figsize'] = [10, 10]

# COMMAND ----------

display(df_speed)

# COMMAND ----------

! pip install streamlit

# COMMAND ----------

st.map(df)

# COMMAND ----------

import numpy as np
def create_cirlce(x, y, bufferLength=0.01, polygonSides=360):
    """0.01 km ~ 32ft"""
    angles = np.linspace(0, 2 * np.pi, polygonSides, endpoint=False)
    points_list = [(x + np.sin(a) * bufferLength,
                    y + np.cos(a) * bufferLength)
                   for a in angles]
    return pd.DataFrame(points_list, columns=['lat', 'long'])

# COMMAND ----------

from math import sin, cos, sqrt, atan2, radians, pi
import numpy as np

bufferLength = 0.01  # ~32 feet
polygonSides = 360

x = lift_data[lift_data.liftname=='3 Kings'].latitude[0]
y = lift_data[lift_data.liftname=='3 Kings'].longitude[0]
# 40.65291, -111.51032
angles = np.linspace(0, 2 * np.pi, polygonSides, endpoint=False)
points_list = [(x + np.sin(a) * bufferLength,
                y + np.cos(a) * bufferLength)
               for a in angles]

# circle = spark.createDataFrame(pd.DataFrame(points_list, columns=columns))
circle = pd.DataFrame(points_list, columns=columns)
circle

# COMMAND ----------

plt.rcParams["figure.figsize"] = (10,10)
plt.legend(loc="upper left")
pd_lift_data = lift_data.toPandas()
plt.plot(df_speed.toPandas().set_index('latitude_int').longitude_int, label='interpolated')
# for x in pd_lift_data[pd_lift_data.resortkey == 16].liftname.tolist():
for x in [ 'Sunrise', 'Saddleback Express', 'Peak 5',  'DreamScape', 'Motherlode Express', 'Sweet Pea']:
    plt.plot(create_cirlce(pd_lift_data[pd_lift_data.liftname==x].latitude.values[0], 
                           pd_lift_data[pd_lift_data.liftname==x].longitude.values[0], 0.0009).set_index('lat').long, label=x)
plt.legend()
plt.show()

# COMMAND ----------

test = df_speed.toPandas()


# COMMAND ----------

for id in list(test.skier_id.unique()):
    plt.plot(test[test.skier_id==id].set_index('latitude').longitude, label='Actual')
    plt.plot(test[(test.skier_id==id )& (test.latitude.isnull())].set_index('latitude_int').longitude_int, 'x', label='Actual')
    plt.show()

# COMMAND ----------



# COMMAND ----------

test

# COMMAND ----------

plt.plot(testing.set_index('latitude_int').longitude_int, label='interpolated')

# COMMAND ----------

testing.set_index('latitude_int')[['longitude_int']]

# COMMAND ----------

circle.set_index('lat')

# COMMAND ----------

testing.latitude_int

# COMMAND ----------

fig = plt.figure()
# plt.xlim(0, 10)
# plt.ylim(0, 1)
graph, = plt.plot([], [], 'o')

def animate(i):
    graph.set_data(x[:i+1], y[:i+1])
    return graph

ani = FuncAnimation(fig, animate, frames=1, interval=5)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Writing Full Interpolation To Bronze

# COMMAND ----------

df_filled = df_filled[[
        'skier_id',
        'timestamp',
        'latitude',
        'longitude',
        'latitude_int',
        'longitude_int',
        'lat_value_ff',
        'lat_value_bf',
        'time_value_ff',
        'time_value_bf',
        'total_gap',
        'current_gap',
    ]].write.parquet('/mnt/lumiplan-data/bronze/sample/full-interpolation/', overwrite=True)

# COMMAND ----------

# MAGIC %md # Mosaic Use Case?

# COMMAND ----------

# MAGIC %pip install databricks-mosaic

# COMMAND ----------

import mosaic as mos

mos.enable_mosaic(spark, dbutils)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # Full Run

# COMMAND ----------

df = spark.sql('''
with raw_geo as
(
  select 
      geo.skier_id
    , geo.latitude
    , geo.longitude
    , from_utc_timestamp(geo.timestamp, 'MST') as skier_timestamp
  from lumiplan_data.sample_extract geo
  where
        date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') >= '07:00:00'
    and date_format(from_utc_timestamp(geo.timestamp, 'MST'), 'HH:mm:ss') <= '17:00:00'
),

cross_time_skiers as
(
  select
      dt.timestamp
    , dt.date
    , geo.skier_id
  from lumiplan_data.dim_time dt
  cross join (select distinct skier_id from raw_geo) geo
  where
          date_format(dt.timestamp, 'HH:mm:ss') >= '07:00:00'
      and date_format(dt.timestamp, 'HH:mm:ss') <= '17:00:00'
)

select
    skier_id
  , avg(latitude) as latitude
  , avg(longitude) as longitude
  , sum(case when longitude is null then 0 else 1 end) as total_gps_points
  , agg_timestamp as timestamp
  , date
from
(
  select 
      dt.skier_id
    , geo.latitude
    , geo.longitude
    , geo.skier_timestamp
    , dt.timestamp as agg_timestamp
    , dt.date
  from cross_time_skiers dt
  left join raw_geo geo
    on  (unix_timestamp(dt.timestamp) - unix_timestamp(geo.skier_timestamp)) > -30
    and (unix_timestamp(dt.timestamp) - unix_timestamp(geo.skier_timestamp)) <= 30
    and dt.date = date(geo.skier_timestamp)
    and dt.skier_id = geo.skier_id
)
group by
    skier_id
  , agg_timestamp
  , date
order by 
    skier_id
  , agg_timestamp
''')
df = df.withColumn("time_hit", when(df.latitude.isNull(), None).otherwise(df.timestamp))

# COMMAND ----------

# create forward and backward windows
window_ff = Window.partitionBy(['skier_id', 'date'])\
                  .orderBy('timestamp')\
                  .rowsBetween(-15, 0)
window_bf = Window.partitionBy(['skier_id', 'date'])\
                  .orderBy('timestamp')\
                  .rowsBetween(0, 15)

# get latitude difference
lat_last = func.last(df['latitude'],
                      ignorenulls=True)\
               .over(window_ff)
lat_next = func.first(df['latitude'],
                       ignorenulls=True)\
               .over(window_bf)
long_last = func.last(df['longitude'],
                      ignorenulls=True)\
               .over(window_ff)
long_next = func.first(df['longitude'],
                       ignorenulls=True)\
               .over(window_bf)

# get timing difference
time_last = func.last(df['time_hit'],
                      ignorenulls=True)\
                .over(window_ff)
time_next = func.first(df['time_hit'],
                       ignorenulls=True)\
                .over(window_bf)

# COMMAND ----------

# add columns to the dataframe
df_filled = df.withColumn('time_value_ff', time_last)\
              .withColumn('time_value_bf', time_next)\
              .withColumn('lat_value_ff', lat_last)\
              .withColumn('lat_value_bf', lat_next)\
              .withColumn('long_value_ff', long_last)\
              .withColumn('long_value_bf', long_next)
df_filled = df_filled.withColumn('total_gap', (unix_timestamp('time_value_bf') - unix_timestamp('time_value_ff')) / 60)
df_filled = df_filled.withColumn('current_gap', (unix_timestamp('time_value_bf') - unix_timestamp('timestamp')) / 60)
df_filled = df_filled.withColumn('latitude_int',
    coalesce(
        df_filled.latitude,
        df_filled.lat_value_ff + (df_filled.total_gap - df_filled.current_gap) * (df_filled.lat_value_bf - df_filled.lat_value_ff) / (df_filled.total_gap),
    )
)
df_filled = df_filled.withColumn('longitude_int',
    coalesce(
        df_filled.longitude,
        df_filled.long_value_ff + (df_filled.total_gap - df_filled.current_gap) * (df_filled.long_value_bf - df_filled.long_value_ff) / (df_filled.total_gap),
    )
)

# COMMAND ----------

display(
    df_filled[[
        'skier_id',
        'timestamp',
        'latitude',
        'latitude_int',
        'longitude',
        'longitude_int',
        'lat_value_ff',
        'lat_value_bf',
        'time_value_ff',
        'time_value_bf',
        'total_gap',
        'current_gap',
    ]]
)

# COMMAND ----------

display(df_filled.filter(df_filled.skier_id == 3114))

# COMMAND ----------

display(
    df_filled[[
        'skier_id',
        'timestamp',
        'latitude',
        'latitude_int',
        'longitude',
        'longitude_int',
        'lat_value_ff',
        'lat_value_bf',
        'time_value_ff',
        'time_value_bf',
        'total_gap',
        'current_gap',
    ]]
)

# COMMAND ----------

df_filled[[
        'skier_id',
        'timestamp',
        'latitude',
        'latitude_int',
        'longitude',
        'longitude_int',
        'lat_value_ff',
        'lat_value_bf',
        'time_value_ff',
        'time_value_bf',
        'total_gap',
        'current_gap',
    ]].write.parquet('/mnt/lumiplan-data/full-interpolation/', overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Interpolation Check

# COMMAND ----------

df_base = spark.read.parquet('/mnt/lumiplan-data/full-interpolation/')
df_next = spark.read.parquet('/mnt/lumiplan-data/full-interpolation/')
df_next = df_next.withColumn("timestamp_after", df_next.timestamp + expr('INTERVAL -1 minutes'))

# COMMAND ----------

oldColumns = df_next.schema.names
newColumns = [f'{i}_next' for i in oldColumns]
df_next = reduce(lambda df_next, idx: df_next.withColumnRenamed(oldColumns[idx], newColumns[idx]), range(len(oldColumns)), df_next)

# COMMAND ----------

dft = df_base.join(
    df_next,
    on = (
          (df_base.timestamp == df_next.timestamp_after_next)
        & (df_base.skier_id == df_next.skier_id_next)
    ),
    how = 'inner'
)
dft = dft[
    [
        'skier_id',
        'timestamp_after_next',
        'timestamp',
        'timestamp_next',
        'latitude_int',
        'longitude_int',
        'latitude_int_next',
        'longitude_int_next',
        'total_gap',
        'current_gap',
    ]
].orderBy(['skier_id', 'timestamp'])

# COMMAND ----------

display(dft)

# COMMAND ----------

# MAGIC %md # Get Distance Between Two Lat and Long Points
# MAGIC 
# MAGIC ```python
# MAGIC from math import sin, cos, sqrt, atan2, radians
# MAGIC 'https://stackoverflow.com/questions/19412462/getting-distance-between-two-points-based-on-latitude-longitude'
# MAGIC # approximate radius of earth in km
# MAGIC R = 6373.0
# MAGIC 
# MAGIC lat1 = radians(52.2296756)
# MAGIC lon1 = radians(21.0122287)
# MAGIC lat2 = radians(52.406374)
# MAGIC lon2 = radians(16.9251681)
# MAGIC 
# MAGIC dlon = lon2 - lon1
# MAGIC dlat = lat2 - lat1
# MAGIC 
# MAGIC a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
# MAGIC c = 2 * atan2(sqrt(a), sqrt(1 - a))
# MAGIC 
# MAGIC distance = R * c
# MAGIC 
# MAGIC print("Result:", distance)
# MAGIC print("Should be:", 278.546, "km")
# MAGIC ```

# COMMAND ----------

lat_1 = 40.68613629581475
lon_1 = -111.56148446193038
lat_2 = 40.68650289353013
lon_1 = -111.55949629086884

R = 6373.0
lat1 = radians(lat_1)
lon1 = radians(lon_1)
lat2 = radians(lat_2)
lon2 = radians(lon_1)
lon1, lat1, long2, lat2 = map(radians, [lon_1, lat_1, lon_2, lat_2])
dlon = lon2 - lon1
dlat = lat2 - lat1

a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
c = 2 * atan2(sqrt(a), sqrt(1 - a))

distance = R * c

print("Result:", distance)

# COMMAND ----------

from math import sin, cos, sqrt, atan2, radians

# COMMAND ----------

def get_distance(lon_1, lat_1, long_2, lat_2):
    radius = 6373.0
    if longit_a is None or latit_a is None or longit_b is None or latit_b is None:
        return None
    else:
        
        # Transform to radians
        lon_1, lat_1, long_2, lat_2 = map(radians, [lon_1, lat_1, long_2, lat_2])
        dist_longit = longit_b - lon_1
        dist_latit = latit_b - latit_a
        # Calculate area
        a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
        # central point
        c = 2 * atan2(sqrt(a), sqrt(1 - a))
        # Calculate Distance
        distance = c * radius
        return distance * 0.621371 # change to miles
udf_get_distance = func.udf(get_distance, FloatType())
dft = dft.filter(
    dft.latitude_int.isNotNull()
    & dft.latitude_int_next.isNotNull()
    & dft.longitude_int.isNotNull()
    & dft.longitude_int_next.isNotNull()
)
dft = dft.withColumn(
    'distance',
    udf_get_distance(
        func.col('longitude_int'), func.col('latitude_int'), 
        func.col('longitude_int_next'), func.col('latitude_int_next')
    )
)

# COMMAND ----------

display(dft.filter(dft.total_gap > 0))

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   *
# MAGIC   , from_utc_timestamp(geo.timestamp, 'MST') as skier_timestamp
# MAGIC from lumiplan_data.sample_extract geo
# MAGIC where skier_id = 3114
# MAGIC order by timestamp

# COMMAND ----------

get_distance(-111.55684142922496, 40.68685558545444, -111.5569572873134, 40.68702332978732)

# COMMAND ----------


3114
40.68685558545444
-111.55684142922496
2022-02-05T11:34:22.000+0000
        
3114
40.68702332978732
-111.5569572873134
2022-02-05T11:36:43.000+0000
