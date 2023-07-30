from cassandra.cluster import Cluster
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import  Model
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine import connection
from cassandra.query import dict_factory
from datetime import datetime, timedelta
import time
import cassandra
import random
import uuid
import math
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)
import datetime
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import mysql.connector
from kafka_mysql_info import MY_HOST, MY_PORT, MY_DBNAME, MY_URL, MY_DRIVER, MY_USER, MY_PASSWORD 

keyspace = 'study_de'
cassandra_login = 'cassandra'
cassandra_password = 'cassandra'
cluster = Cluster()
session = cluster.connect(keyspace)

def get_data_from_job():
    cnx = mysql.connector.connect(user=MY_USER, password=MY_PASSWORD,
                                         host=MY_HOST,
                                      database=MY_DBNAME)
    query = """select id as job_id,campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data


def get_data_from_publisher():
    cnx = mysql.connector.connect(user=MY_USER, password=MY_PASSWORD,
                                         host=MY_HOST,
                                      database=MY_DBNAME)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def generating_dummy_data(n_records,session):
    publisher = get_data_from_publisher()
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job()
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    company_list = jobs_data['company_id'].to_list()
    group_list = jobs_data[jobs_data['group_id']!='']['group_id'].astype(int).to_list()
    i = 0
    fake_records = n_records
    while i <= fake_records:
        create_time = cassandra.util.uuid_from_time(datetime.datetime.now())
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = f""" INSERT INTO log_tracking (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES ('{create_time}',{bid},{campaign_id},'{custom_track}',{group_id},{job_id},{publisher_id},'{ts}')"""
        print(sql)
        session.execute(sql)
        i+=1 
    print("Data Generated Successfully")
    
status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,20),session = session)
    time.sleep(10)