import os
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
from time import sleep, time, ctime
from random import random, randint, choice
import logging
from confluent_kafka import Producer
import tweepy

def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    return conf

class stream_twitter(tweepy.StreamingClient):
    def __init__(self, producer):
        super().__init__()
        self.producer = producer
    def on_connect(self):
        logging.info('Successful Connect to Twitter.')
    
    def on_tweet(self, tweet):
        # do not search re-tweets
        if tweet.referenced_tweets == None:
            self.producer.produce("twitter_topic", key=tweet.id, value=tweet.text)
            
            sleep(0.2)

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
    'twitter_stream_dag',
    default_args=default_args,
    description='Get related tweets from tweepy stream',
    schedule_interval='@hourly',
    catchup=False) as dag:

    @task
    def generate_tweets():      
        # initial producer
        producer = Producer(read_ccloud_config("/home/airflow/gcs/dags/client.properties"))
        '''I have facing issue to use twitter stream API, it only allowed enterprise
          user since API V2 release. Very sad so I have to generate dummy tweets here.
          If you have valid key, you can uncomment following code'''
        # get tweets from twitter API
        # search_terms = ['chatgpt', 'midjourney','stable-diffusion']
        # stream = stream_twitter(bearer_token, producer)
        # for term in search_terms:
        #     stream.add_rules(tweepy.StreamRule(term))
        
        # stream.filter(tweet_fields = ['referenced_tweets'])


        # generate dummy tweets
        hashtag_word = ("openai", "chatgpt", "alpaca", "llama", "langchain", 
                        "autogpt", "cachegpt", "midjourney", "stable diffusion")
        sentences = (' is awesome, I like to use it.',
                    ' is so good, I\'m happy to share with you all!',
                    ' make me feel uncomfortable and it looks not safe.')
        for _ in range(100):
            user_id = randint(1000, 10000)
            tweet_id = randint(10000, 10000000)
            key_word = choice(hashtag_word)
            sentence = choice(sentences)
            now = ctime(time())
            message = f"{now},{user_id},{key_word},{key_word+sentence},{tweet_id}"
            message = message.encode("utf-8")
            producer.produce("twitter_topic", key=str(user_id), value=message)
            sleep(random() * 4)
        
        
    
    generate_tweets()
        
