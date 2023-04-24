from medium_api import Medium
import os
from datetime import datetime, timedelta
from airflow.models import Variable
import logging

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
        result.append(article)
    return result


def get_hot_articles(**kwargs):
    medium_key = Variable.get('medium_key')
    medium = Medium(medium_key)
    result = []
    # Generative Ai airticles
    topfeeds_hot = medium.topfeeds(
                    tag = "generative-ai", 
                    mode = "hot",
                    count = 1
                )
    topfeeds_month = medium.topfeeds(
                    tag = "generative-ai", 
                    mode = "top_month",
                    count = 1
                )
    # Fetch all the articles information
    topfeeds_hot.fetch_articles()
    topfeeds_month.fetch_articles()
    # Iterate over topfeeds articles and print their title
    for article in topfeeds_hot.articles:
        result.append(article)
    for article in topfeeds_month.articles:
        result.append(article)
    return result

def processing_article(article, **kwargs):
    result = []
    logging.info(f'uccessfully fetched from previous task, article title is {article.title}')
    content = article.content
    sentence = ''
    for ind,t in enumerate(content.split('\n\n')):
        sentence += t+'\n'
        if len(sentence)>1000:
            result.append({
                'text': sentence,
                'id': article.article_id+'_'+str(ind),
                'date': str(article.last_modified_at),
                'title': article.title,
                'url':article.url 
            })
            sentence = ''
    return result