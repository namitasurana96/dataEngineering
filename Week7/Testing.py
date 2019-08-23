import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

import pandas as pd
import numpy as np
import pyparsing as pp
import matplotlib.pyplot as plt
import seaborn as sns
import nltk

from google.cloud import storage

from nltk.stem import PorterStemmer
from nltk.tokenize import TweetTokenizer
from wordcloud import WordCloud, STOPWORDS
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.pipeline import make_pipeline
import warnings


import string
import re
from nltk.corpus import stopwords

warnings.filterwarnings("ignore")
import pickle

def clean_tweets(data):
    stopwords_english = stopwords.words('english')
    stemmer = PorterStemmer()
    data = data.split(',')
    tweet = " ".join(data[1:])
    # remove stock market tickers like $GE
    tweet = ' '.join([word for word in tweet.split() if not word.startswith('@')])
    # remove stock market tickers like $GE
    tweet = re.sub(r'\$\w*', '', tweet)
    # remove old style retweet text "RT"
    tweet = re.sub(r'^RT[\s]+', '', tweet)
    # remove hyperlinks
    tweet = re.sub(r'https?:\/\/.*[\r\n]*', '', tweet)
    # remove hashtags
    # only removing the hash # sign from the word
    tweet = re.sub(r'#', '', tweet)
    # remove Special characters
    tweet = re.sub(r"[^a-zA-Z]+", ' ', tweet)
    # tokenize tweets
    tokenizer = TweetTokenizer(preserve_case=False, strip_handles=True, reduce_len=True)
    tweet_tokens = tokenizer.tokenize(tweet)
    tweets_clean = []
    for word in tweet_tokens:
        if (word not in stopwords_english and word not in string.punctuation):
            stem_word = stemmer.stem(word)
            tweets_clean.append(stem_word)

        tweet_str = " ".join(tweets_clean)
        result = ",".join([data[0], tweet_str])
        print(result)
    return [result]

def Mlmodel(data):
    # df = pd.DataFrame(data[1][1:], columns=('Date', 'Open', 'High','Low', 'Close', 'Volume' ))
    vs = [v for v in data[1]]
    df = pd.DataFrame(vs)
    df = df.iloc[:, 0:2]
    df.columns = ['id', 'cleaned_tweet']
    print(df)

    '''storage_client = storage.Client()
    bucket = storage_client.get_bucket("first_project1")
    blob = bucket.blob(storage_client=storage.Client())
    bucket = storage_client.get_bucket("first_project1")
    blob = bucket.blob("TwitterSA_model.pkl")
    model_local = "TwitterSA_model.pkl"
    blob.download_to_filename(model_local)'''

    # cv = CountVectorizer()
    # vect = cv.fit(df['cleaned_tweet'])

    pickle_in = open("TwitterSA_model.pkl", "rb")
    print(pickle_in)
    model = pickle.load(pickle_in)
    cv = pickle.load(pickle_in)
    preds = model.predict(cv.transform(df['cleaned_tweet']))
    l1 = []
    i = 0
    for val in preds:
        temp = {}
        temp['Tweets'] = df.ix[i, 'cleaned_tweet']
        temp['Predicted_Value'] = str(val)
        l1.append(temp)
    print(l1)
    # return [{'Years':'hjk', 'salary':'gvj'},{'Years':'hjk', 'salary':'gvj'}]
    # return [{'Predicted_Value': [op],}]
    return l1


options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'job30'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).runner = 'DirectRunner'

p = beam.Pipeline(options=options)
lines = \
(p | 'ReadFileFromCSV' >> beam.io.ReadFromText('gs://first_project1/test_tweets_anuFYb8.csv', skip_header_lines=1)
    |'map record'>> beam.Map(lambda record:('record', pp.commaSeparatedList.parseString(record).asList()))
    | 'Cleaning' >> beam.ParDo(clean_tweets)
    | 'map' >> beam.Map(lambda record:('data', record))
    | 'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(Mlmodel)
   # | 'database' >> beam.io.WriteToBigQuery(schema=table_schema, table="gcp1-248309:my_dataset_id.StockPrice")
 #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
  #  | 'Write output to file' >> beam.io.WriteToText('StockPricePrediction.csv')
)
p.run()