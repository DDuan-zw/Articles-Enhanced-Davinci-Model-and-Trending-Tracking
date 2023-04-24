from confluent_kafka import Consumer
import os
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
from time import sleep, time, ctime
from random import random, randint, choice
import logging
from ksql import KSQLAPI

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf


# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ziwei',
    'start_date': days_ago(0),
    #'email': ['ziwei@somemail.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'provide_context': True,   # this is set to True as we want to pass variables on from one task to another
    'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

# defining the DAG
with DAG(
    'twitter_consumer_dag',
    default_args=default_args,
    description='Save stream tweets into ksql',
    #schedule_interval='@hourly',
    catchup=False) as dag:

    @task
    def consume_tweets():

        props = read_ccloud_config("/home/airflow/gcs/dags/client.properties")
        props["group.id"] = "python-group-1"
        props["auto.offset.reset"] = "earliest"
        client = KSQLAPI(Variable.get('ksql_address'), 
                         api_key=Variable.get('ksql_API_key'), 
                         secret=Variable.get('ksql_API_secret'))
        tables = client.ksql('show tables')
        if 'tweets_table' not in tables[0]['tables']['tables']:
            client.create_stream(table_name='tweets_table',
                     columns_type=['publish_date TIMESTAMP', 'User_id INT','Content VARCHAR', 'hashtag VARCHAR', 'tweet_id VARCHAR'],
                     topic='store_tweets',
                     value_format='DELIMITED')


        consumer = Consumer(props)
        consumer.subscribe(["twitter_topic"])
        try:
            while True:
                msg = consumer.poll(1.0)
                if msg is not None and msg.error() is None:
                    key=msg.key().decode('utf-8')
                    value=msg.value().decode('utf-8')
                    (publish_date, User_id, hashtag, Content, tweet_id) = value.split(",")
                    row = [{"publish_date": publish_date, 
                            "User_id": User_id, 
                            "Content": Content,
                            'hashtag': hashtag,
                            'tweet_id': tweet_id}]
                    client.inserts_stream("store_tweets", key, row)
                    logging.info("key = {key:12} value = {value:12}".format(key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
        except KeyboardInterrupt:
            pass
        finally:
            consumer.close()
    
    consume_tweets()
