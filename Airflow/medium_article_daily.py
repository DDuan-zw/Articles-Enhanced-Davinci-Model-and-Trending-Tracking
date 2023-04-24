# import the libraries
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.utils.dates import days_ago
from medium_api import Medium
import openai
import pinecone
from time import sleep
import json
import itertools
#from src.MediumHelper import get_recent_article, processing_article, get_hot_articles
import logging

# medium_key = Variable.get('medium_key')
# pinecone_key = Variable.get('pinecone_key')
# pinecone_region = Variable.get('pinecone_region')
# openai_key = Variable.get('openai_key')
#defining DAG arguments

def initial_pinecone():
    index_name = 'openai-embedding-data'

    # initialize connection to pinecone (get API key at app.pinecone.io)
    pinecone.init(
        api_key=Variable.get('pinecone_key'),
        environment=Variable.get('pinecone_region')  # may be different, check at app.pinecone.io
    )

    # check if index already exists (it shouldn't if this is first time)
    if index_name not in pinecone.list_indexes():
        # if does not exist, create index
        pinecone.create_index(
            index_name,
            dimension=1536,
            metric='cosine',
            metadata_config={'indexed': ['source']}
        )
    # connect to index
    index = pinecone.Index(index_name)
    # view index stats
    index.describe_index_stats()
    return index

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ziwei',
    'start_date': days_ago(0),
    #'email': ['ziwei@somemail.com'],
    #'email_on_failure': False,
    #'email_on_retry': False,
    'provide_context': True,   # this is set to True as we want to pass variables on from one task to another
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# defining the DAG
with DAG(
    'medium_daily_article_dag',
    default_args=default_args,
    description='Get most recent articles from medium (last day) and upload embeddings to Ponecone.',
    schedule_interval=timedelta(days=1),) as dag:

    # define the first task

    @task
    def get_recent_article(**kwargs):
        medium_key = Variable.get('medium_key')
        medium = Medium(medium_key)
        result = []
        publication = medium.publication(publication_id="440100e76000", save_info=False)
        last_day_articles = publication.get_articles_between(
                                    _from=datetime.now(), 
                                    _to=datetime.now() - timedelta(days=1)
                                )
        for article in last_day_articles:
            result.append({
                'text': str(article.content),
                'id': str(article.article_id),
                'date': str(article.last_modified_at),
                'title': str(article.title),
                'url': str(article.url) 
                })
        return [json.dumps(res) for res in result]

    @task
    def processing_article(arti):
        result = []
        article = json.loads(arti)
        logging.info(f'successfully fetched from previous task, article title is {article["title"]}')
        content = article['text']
        sentence = ''
        for ind,t in enumerate(content.split('\n\n')):
            sentence += t+'\n'
            if len(sentence)>1000:
                result.append({
                    'text': sentence,
                    'id': article['id']+'_'+str(ind),
                    'date': article['date'],
                    'title': article['title'],
                    'url':article['url'] 
                })
                sentence = ''
        return json.dumps(result)

    @task
    def load2pinecone(sentences):
        sentences = [json.loads(s) for s in sentences]
        sentences = list(itertools.chain.from_iterable(sentences))
        openai.api_key = Variable.get('openai_key')
        index = initial_pinecone()
        embed_model = "text-embedding-ada-002"
        batch_size = 50  # how many embeddings we create and insert at once

        for i in range(0, len(sentences), batch_size):
            # find end of batch
            i_end = min(len(sentences), i+batch_size)
            meta_batch = sentences[i:i_end]
            # get ids
            ids_batch = [x['id'] for x in meta_batch]
            # get texts to encode
            texts = [x['text'] for x in meta_batch]
            # create embeddings (try-except added to avoid RateLimitError)
            try:
                res = openai.Embedding.create(input=texts, engine=embed_model)
            except:
                done = False
                while not done:
                    sleep(5)
                    try:
                        res = openai.Embedding.create(input=texts, engine=embed_model)
                        done = True
                    except:
                        pass
            embeds = [record['embedding'] for record in res['data']]
            # cleanup metadata
            meta_batch = [{
                'text': x.get('text','NaN'),
                'publish_date': x.get('date','NaN'),
                'title': x.get('title','NaN'),
                'url':x.get('url','NaN') 
            } for x in meta_batch]
            to_upsert = list(zip(ids_batch, embeds, meta_batch))
            # upsert to Pinecone
            index.upsert(vectors=to_upsert)


    # task pipeline 
    load2pinecone(processing_article.partial().expand(arti = get_recent_article()))