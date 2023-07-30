from datetime import datetime, timedelta
import datetime
import time
import random
import cassandra
from cassandra.query import dict_factory # for create_time

import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)

import mysql.connector

from kafka.producer import KafkaProducer
import json

from kafka_mysql_info import MY_HOST, MY_PORT, MY_DBNAME, MY_URL, MY_DRIVER, MY_USER, MY_PASSWORD, K_HOST, K_PORT, K_TOPIC 

# Import data to Kafka topic
# Create a Producer
producer = KafkaProducer(bootstrap_servers=f'{K_HOST}:{K_PORT}',
                    value_serializer=lambda x: json.dumps(x).encode('utf-8'))

# Get example publisher_id
def get_data_from_publisher():
    cnx = mysql.connector.connect(user=MY_USER, password=MY_PASSWORD,
                                         host=MY_HOST,
                                      database=MY_DBNAME)
    query = """select distinct(id) as publisher_id from master_publisher"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

# Get example job_id, campaign_id, group_id, company_id
def get_data_from_job():
    cnx = mysql.connector.connect(user=MY_USER, password=MY_PASSWORD,
                                         host=MY_HOST,
                                      database=MY_DBNAME)
    query = """select id as job_id, campaign_id , group_id , company_id from job"""
    mysql_data = pd.read_sql(query,cnx)
    return mysql_data

def generating_dummy_data(n_records):
    publisher = get_data_from_publisher()
    publisher = publisher['publisher_id'].to_list()
    jobs_data = get_data_from_job()
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    company_list = jobs_data['company_id'].to_list()
    group_list = jobs_data[jobs_data['group_id']!='']['group_id'].astype(int).to_list()
        
    for _ in range(n_records):
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interact = ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interact,weights=(70,10,10,10))[0]
        job_id = random.choice(job_list)
        publisher_id = random.choice(publisher)
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        data = {'create_time':f'{create_time}', 'bid':f'{bid}'\
            , 'campaign_id':f'{campaign_id}', 'custom_track':f'{custom_track}', 'group_id':f'{group_id}'\
            , 'job_id':f'{job_id}', 'publisher_id':f'{publisher_id}', 'ts':f'{ts}'}
        print(f'Sending data: {data}')
        producer.send(f'{K_TOPIC}', value=data)
    print('Data Generated Successfully!')

status = "ON"
while status == "ON":
    generating_dummy_data(n_records = random.randint(1,5))
    time.sleep(1)
producer.close()