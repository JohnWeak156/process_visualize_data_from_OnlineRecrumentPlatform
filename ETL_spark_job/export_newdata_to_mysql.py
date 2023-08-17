# Import library
import findspark
findspark.init()

import pyspark.sql as pyspark_sql
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from pyspark.sql.functions import udf

import pandas as pd
import uuid
import time_uuid
from datetime import datetime

from cassandra_mysql_info import MY_HOST, MY_PORT, MY_DBNAME, MY_URL, MY_DRIVER, MY_USER, MY_PASSWORD, CAS_TABLE, CAS_KEYSPACE 

# Create Spark environment
from pyspark.sql import SparkSession

# Define a function coverting uuid time -> timestamp
def uuid2ts(uuid_str):
    my_uuid = uuid.UUID(uuid_str)
    ts_long = time_uuid.TimeUUID(bytes=my_uuid.bytes).get_timestamp()
    return float(ts_long)

uuid2ts_udf = udf(uuid2ts, FloatType())

def process_df(df):
    df_processed = df\
    .withColumn('ts', from_unixtime(uuid2ts_udf('create_time')))\
    .select('create_time', 'ts', 'job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    return df_processed
    
## From table `output``
## Calculate table `click`
def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.createOrReplaceTempView('clicks')
    clicks_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , avg(bid) as bid_set, count(*) as clicks , sum(bid) as spend_hour from clicks
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return clicks_output 

## Calculate table `coversion`
def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.createOrReplaceTempView('conversion')
    conversion_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as conversions  from conversion
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return conversion_output 

## Calculate table `qualifed`
def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.createOrReplaceTempView('qualified')
    qualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as qualified  from qualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return qualified_output

## Calculate table `ununqualified`
def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.createOrReplaceTempView('unqualified')
    unqualified_output = spark.sql("""select job_id , date(ts) as date , hour(ts) as hour , publisher_id , campaign_id , group_id , count(*) as unqualified  from unqualified
    group by job_id , date(ts) , hour(ts) , publisher_id , campaign_id , group_id """)
    return unqualified_output

# Join mutiple table above
def process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output):
    keys = ['job_id','date','hour','publisher_id','campaign_id','group_id']
    final_data = clicks_output\
    .join(conversion_output, keys,'full')\
    .join(qualified_output, keys,'full')\
    .join(unqualified_output, keys,'full')
    return final_data 

def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output,conversion_output,qualified_output,unqualified_output)
    return final_data

# Get data from table `company` (to get job_id)
def retrieve_company_data():
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=MY_URL, driver=MY_DRIVER, dbtable=sql, user=MY_USER, password=MY_PASSWORD).load()
    return company

def import_to_mysql(output, db_table):
    final_output = output.select('job_id','date','hour','publisher_id','company_id','campaign_id','group_id'\
        ,'unqualified','qualified','conversions','clicks','bid_set','spend_hour', 'latest_update_time')
    final_output = final_output\
        .withColumnRenamed('date','dates')\
        .withColumnRenamed('hour','hours')\
        .withColumnRenamed('qualified','qualified_application')\
        .withColumnRenamed('unqualified','disqualified_application')\
        .withColumnRenamed('conversions','conversion')\
        .withColumn('sources', lit('Cassandra'))
    
    # Import to MySQL db
    final_output.write.format('jdbc')\
    .option('url', MY_URL)\
    .option('driver', MY_DRIVER)\
    .option('dbtable', db_table)\
    .option('user', MY_USER)\
    .option('password', MY_PASSWORD)\
    .mode('append').save()
    return print('Data imported successfully')

# Get latest time from table `events` in MySQL db
def get_mysql_latest_time():    
    sql = """(select max(latest_update_time) from events) data"""
    mysql_time = spark.read.format('jdbc').options(url=MY_URL, driver=MY_DRIVER, dbtable=sql, user=MY_USER, password=MY_PASSWORD).load()
    mysql_time = mysql_time.take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest 

# Main task
def main_task(mysql_time):
    print('The host is ' ,MY_HOST)
    print('The port using is ',MY_PORT)
    print('The db using is ',MY_DBNAME)
    print('-----------------------------')
    print('Retrieving and selecting data from Cassandra')
    print('-----------------------------')
    data = spark.read.format("org.apache.spark.sql.cassandra")\
    .options(table=CAS_TABLE, keyspace=CAS_KEYSPACE)\
    .load()\
    .select('create_time', col('job_id').cast(IntegerType()).cast(StringType()), 'custom_track','bid','campaign_id'\
            ,col('group_id').cast(IntegerType()).cast(StringType()), 'publisher_id', 'ts')\
    .where(col('ts')>= mysql_time)
    data.printSchema()
    print('-----------------------------')
    print('Processing data from Cassandra')
    print('-----------------------------')
    df = process_df(data)
    print('-----------------------------')
    print('Processing Cassandra Output')
    print('-----------------------------')
    cassandra_output = process_cassandra_data(df)
    print('-----------------------------')
    print('Merge Company Data')
    print('-----------------------------')
    company = retrieve_company_data()
    print('-----------------------------')
    print('Finalizing Output')
    print('-----------------------------')
    final_output = cassandra_output\
        .join(company,'job_id','full')\
        .drop(company.group_id)\
        .drop(company.campaign_id)\
        .withColumn('latest_update_time', current_timestamp())
    print('-----------------------------')
    print('Import Output to MySQL')
    print('-----------------------------')
    import_to_mysql(final_output, db_table='events')
    return print('Task Finished')

if __name__ == "__main__":
    spark = SparkSession.builder\
    .config("spark.jars", "mysql-connector-java-8.0.30.jar")\
    .config('spark.jars', 'spark-cassandra-connector-assembly_2.12-3.3.0.jar')\
    .getOrCreate()

    mysql_time = get_mysql_latest_time()
    main_task(mysql_time)
    spark.stop()