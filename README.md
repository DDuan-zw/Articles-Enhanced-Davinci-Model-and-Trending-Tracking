# Articles-Enhanced-Davinci-Model-and-Trending-Tracking
Airflow Pipeline to read ariticles from Medium python API, tracking key words trending on Twitter using Confluent Kafka

[Streamlit Page](https://dduan-zw-visualize-trends-app-gpc1ct.streamlit.app/)

## Project outline 
![alt text](https://github.com/DDuan-zw/Articles-Enhanced-Davinci-Model-and-Trending-Tracking/blob/main/src/outline.jpg)

## Steps to set up the pipeline 
1. Set up [Medium-python API](https://pypi.org/project/medium-api/), [Twitter Developer API](https://developer.twitter.com/en/portal/dashboard) and [Openai API](https://openai.com/blog/openai-api). Keep your keys and secrets.    
2. Register [Google Cloud](https://cloud.google.com/), [Pinecone](https://www.pinecone.io/), and [Confluent Kafka](https://www.confluent.io/) account. Keep your Pinecone keys and download `client.properties` from confluent cloud. 
3. Create enviroment in composer, set cluster, topics, ksqlDB on confluent cloud.
4. Add following packages to your GCP composer:
```
medium-api
pinecone-client
openai
confluent-kafka
tweepy
ksql
```
5. Upload all dags in Airflow folder to composer Buckets.
6. Set all keys and secrets Variables in Airflow by Admin-->Variables.



## Streamlit Page
![alt text](https://github.com/DDuan-zw/Articles-Enhanced-Davinci-Model-and-Trending-Tracking/blob/main/src/streamlit_UI.png)
