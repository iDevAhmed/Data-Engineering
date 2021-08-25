import json
import csv
import re
import tweepy as tw
from tweepy import OAuthHandler 
from textblob import TextBlob 
import pandas as pd
from textblob.sentiments import NaiveBayesAnalyzer
from datetime import datetime
from datetime import date
from airflow import DAG

from airflow.operators.python_operator import PythonOperator

default_args = {
		'owner': 'airflow',
		'depends_on_past': False,
		'start_date': datetime(2021, 1, 1)
		}

dag = DAG(
		'tweets-DAG',
		default_args=default_args,
		description='Fetch happiness data from API',
		schedule_interval='@hourly',
	)

def fetch_data(**kwargs):
	consumerKey = 'PSFNqjQ7vlekOlMlCusutTxky'
	consumerSecret = 'VpyKj7IEQFOzgqmr0FtKIVC8vBIrN98GDY574wRIYYOK4ZPJ9k'
	accessToken = '947064703334912000-dFx773rzq4R0eQydcM3XWmdilXAhaQf'
	accessTokenSecret = 'qfAlla1A9WkhS6465ozv6wa6bl1apC7rbswc6v4eXL8h9'

	auth = OAuthHandler(consumerKey, consumerSecret)
	auth.set_access_token(accessToken, accessTokenSecret)
	api = tw.API(auth)
	#Sweden
	happy_tweets = tw.Cursor(api.search,geocode="57.7118,11.98868,200km" , q= "-filter:retweets, safe", lang ="en", since='2021-01-01' ).items(20)
	#Egypt
	sad_tweets = tw.Cursor(api.search,geocode="27.19595,33.82958,200km" , q= "-filter:retweets, safe", lang ="en", since='2021-01-01' ).items(20)

	happy_users_locs = [[datetime.now(),tweet.user.screen_name, tweet.text ,tweet.user.location] for tweet in happy_tweets]
	sad_users_locs = [[datetime.now(),tweet.user.screen_name, tweet.text ,tweet.user.location] for tweet in sad_tweets]

	happy_tweets_df = pd.DataFrame(data=happy_users_locs, 
                    columns=['Timestamp','User','Text','Location'])
	sad_tweets_df = pd.DataFrame(data=sad_users_locs, 
                    columns=['Timestamp','User','Text','Location'])

	happy_tweets_json = happy_tweets_df.to_json()
	sad_tweets_json = sad_tweets_df.to_json()
	
	return happy_tweets_json, sad_tweets_json

def sentiment_analysis(**context):
		tweets_Happy, tweets_Sad = context['task_instance'].xcom_pull(task_ids='fetch')
		tweets_Happy_df = pd.DataFrame(json.loads(tweets_Happy))
		tweets_Sad_df = pd.DataFrame(json.loads(tweets_Sad))

		sentiment_sweden = 0
		sentiment_egypt = 0
		average_sentiment_sweden = 0
		average_sentiment_egypt = 0

		texts_sweden = tweets_Happy_df["Text"]
		texts_egypt = tweets_Sad_df["Text"]
		sentiment_series_sweden = []

		for text in texts_sweden:
			analysis = TextBlob(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split()))
			sentiment_sweden +=  analysis.sentiment.polarity
			average_sentiment_sweden = sentiment_sweden / len(tweets_Happy)
			sentiment_series_sweden.append(analysis.sentiment.polarity)
		tweets_Happy_df['Sentiment'] = sentiment_series_sweden

		sentiment_series_egypt = []
		for text in texts_egypt:
			analysis = TextBlob(' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", str(text)).split()))
			sentiment_egypt +=  analysis.sentiment.polarity
			average_sentiment_egypt = sentiment_egypt / len(tweets_Happy)
			sentiment_series_egypt.append(analysis.sentiment.polarity)
		tweets_Sad_df['Sentiment'] = sentiment_series_egypt

		return average_sentiment_sweden, average_sentiment_egypt, tweets_Happy_df.to_json(),tweets_Sad_df.to_json()

def store_data(**context):
    average_sentiment_sweden, average_sentiment_egypt, sentiment_sweden_df, sentiment_egypt_df = context['task_instance'].xcom_pull(task_ids='analyze_tweets')
    sweden_csv = pd.DataFrame(json.loads(sentiment_sweden_df))
    sweden_csv.to_csv("/root/airflow/dags/sweden-sent.csv")
    egypt_csv = pd.DataFrame(json.loads(sentiment_egypt_df))
    egypt_csv.to_csv("/root/airflow/dags/egypt-sent.csv")



fetch = PythonOperator(
    task_id='fetch',
    python_callable=fetch_data,
    provide_context=True,
    dag=dag
)

analyze_tweets = PythonOperator(
    task_id='analyze_tweets',
    python_callable=sentiment_analysis,
    provide_context=True,
    dag=dag
)

store = PythonOperator(
    task_id='store',
    python_callable=store_data,
    provide_context=True,
    dag=dag
)

fetch >> analyze_tweets

analyze_tweets >> store
