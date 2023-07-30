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

# Create Spark environment
from pyspark.sql import SparkSession

from cassandra_mysql_info import MY_HOST, MY_PORT, MY_DBNAME, MY_URL, MY_DRIVER, MY_USER, MY_PASSWORD, CAS_TABLE, CAS_KEYSPACE 

# Get latest time from table `log_tracking` in Cassandra db
def get_latest_time_cassandra():
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table=CAS_TABLE,keyspace=CAS_KEYSPACE).load()
    cassandra_latest_time = data.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_latest_time

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
def main():
    cassandra_time = get_latest_time_cassandra()
    print('Cassandra latest time is {}'.format(cassandra_time))
    mysql_time = get_mysql_latest_time()
    print('MySQL latest time is {}'.format(mysql_time))
    if cassandra_time > mysql_time: 
        print('Have new data, continue task')
        spark.stop()
        return True
    else:
        print('No new data found')
        spark.stop()
        return False
    
if __name__ == "__main__":
    spark = SparkSession.builder\
    .config("spark.jars", "mysql-connector-java-8.0.30.jar")\
    .config('spark.jars', 'spark-cassandra-connector-assembly_2.12-3.3.0.jar')\
    .getOrCreate()

    main()