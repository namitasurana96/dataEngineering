import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from google.cloud import storage

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import nltk
import pyparsing as pp
from wordcloud import WordCloud, STOPWORDS
from sklearn.model_selection import train_test_split, GridSearchCV
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer, ENGLISH_STOP_WORDS
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import MultinomialNB
from sklearn.metrics import f1_score, roc_auc_score
from sklearn.pipeline import make_pipeline
import warnings

import tensorflow as tf

warnings.filterwarnings("ignore")
import pickle

def Mlmodel(data):
    vs = [v for v in data[1]]
    train = pd.DataFrame(vs, columns=['id','label', 'tweet'])
    print(type(train))
    #train.sample(2)
    print(train.head())


    train['cleaned_tweet'] = train.tweet.apply(lambda x: ' '.join([word for word in x.split() if not word.startswith('@')]))
    print(train.head())
    #Select all words from normal tweet
    normal_words = ' '.join([word for word in train['cleaned_tweet'][train['label'] == 0]])
    #Collect all hashtags
    pos_htag = [htag for htag in normal_words.split() if htag.startswith('#')]
    #Remove hashtag symbol (#)
    pos_htag = [pos_htag[i][1:] for i in range(len(pos_htag))]
    #Count frequency of each word
    pos_htag_freqcount = nltk.FreqDist(pos_htag)
    pos_htag_df = pd.DataFrame({'Hashtag' : list(pos_htag_freqcount.keys()),'Count' : list(pos_htag_freqcount.values())})

    #Repeat same steps for negative tweets
    negative_words = ' '.join([word for word in train['cleaned_tweet'][train['label'] == 1]])
    neg_htag = [htag for htag in negative_words.split() if htag.startswith('#')]
    neg_htag = [neg_htag[i][1:] for i in range(len(neg_htag))]
    neg_htag_freqcount = nltk.FreqDist(neg_htag)
    neg_htag_df = pd.DataFrame({'Hashtag' : list(neg_htag_freqcount.keys()), 'Count' : list(neg_htag_freqcount.values())})

    X_train, X_val, y_train, y_val = train_test_split(train['cleaned_tweet'], train['label'], random_state=0)
    X_train.shape, X_val.shape

    # cv = CountVectorizer()
    # X_train_vectorized = cv.fit_transform(train['cleaned_tweet'])

    cv = CountVectorizer()
    vect = cv.fit(X_train)
    X_train_vectorized = vect.transform(X_train)
    X_train_vectorized

    logistic_model_cv = LogisticRegression()
    logistic_model_cv.fit(X_train_vectorized, y_train)
    pred = logistic_model_cv.predict(vect.transform(X_val))
    # print('F1 :', f1_score(y_val, pred))

    # Save the preprocessing object to pickle file
    filename = "gs://first_project1/TwitterSA_model.pkl"
    with tf.io.gfile.GFile(filename, 'wb') as file:
        pickle.dump(logistic_model_cv, file)


'''def upload_blob(bucket_name, source_file_name, destination_blob_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    print('File {} uploaded to {}.'.format(source_file_name, destination_blob_name))
'''

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'batchjob'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).runner = 'DirectRunner'

p = beam.Pipeline(options=options)
lines = \
(p  | 'read' >> beam.io.ReadFromText('gs://first_project1/train_E6oV3lV.csv', skip_header_lines=1)
    #| 'map' >> beam.Map(lambda record: ('train', record.split(',')))
    |'map record'>> beam.Map(lambda record:('record', pp.commaSeparatedList.parseString(record).asList()))
    | 'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(Mlmodel)
)
p.run()