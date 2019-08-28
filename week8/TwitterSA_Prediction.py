import apache_beam as beam
import pandas
import numpy as np
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions, WorkerOptions
from google.cloud import bigquery
# import TwitterSA_Preprocessing as pp


def printdata(value):
    # print(type(value))
    # tweet = value.split(',')
    print(value)


def clean_tweets(data):

    import string
    import re
    from nltk.corpus import stopwords
    stopwords_english = stopwords.words('english')
    from nltk.stem import PorterStemmer
    stemmer = PorterStemmer()
    from nltk.tokenize import TweetTokenizer

    # print(data)
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
        if (word not in stopwords_english and  # remove stopwords
        # word not in emoticons and  # remove emoticons
            word not in string.punctuation):  # remove punctuation
            # tweets_clean.append(word)
            stem_word = stemmer.stem(word)  # stemming word
            tweets_clean.append(stem_word)

    tweet_str = " ".join(tweets_clean)
    result = ",".join([data[0], tweet_str])
    # print(result)
    return [result]


def MLmodel(data):

    import pandas as pd
    import pickle

    from sklearn.metrics import f1_score
    from sklearn.feature_extraction.text import CountVectorizer
    from google.cloud import storage
    # import google.cloud as storage
    # import lib.cloudstorage as storage
    # df = pd.DataFrame(data[1][1:], columns=('Date', 'Open', 'High','Low', 'Close', 'Volume' ))
    vs = [v for v in data[1]]
    df = pd.DataFrame(vs)
    df = df.iloc[:, 0:2]
    df.columns = ['id', 'cleaned_tweet']

    # df['cleaned_tweet'] = ''
    # df = pp.preprocessing(df)
    # print(df)

    storage_client = storage.Client()
    bucket = storage_client.get_bucket("gcp_first_bucket")
    blob = bucket.blob("Model1/TwitterSA_model.pkl")
    model_local = "TwitterSA_model.pkl"
    blob.download_to_filename(model_local)

    # cv = CountVectorizer()
    # vect = cv.fit(df['cleaned_tweet'])

    pickle_in = open(model_local, "rb")
    model = pickle.load(pickle_in)
    cv = pickle.load(pickle_in)
    # print(new_data)
    # # make predictions and find the rmse
    preds = model.predict(cv.transform(df['cleaned_tweet']))
    # print('predicted values', (preds))
    # print('F1 :', f1_score(y_test, preds))
    # print(preds)
    l1 =[]
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
google_cloud_options.project = 'probable-net-247610'
google_cloud_options.job_name = 'myjob'
google_cloud_options.staging_location = 'gs://gcp_first_bucket/staging'
google_cloud_options.temp_location = 'gs://gcp_first_bucket/temp'

worker_options = options.view_as(WorkerOptions)
worker_options.max_num_workers = 15 # default:15

options.view_as(StandardOptions).runner = 'Dataflow'
options.view_as(SetupOptions).save_main_session = True

# setup_options.setup_file = "/home/admin1/PycharmProjects/GCP_Project/setup.py" # set fullpath
p = beam.Pipeline(options=options)
lines = \
    (p | 'ReadFileFromCSV' >> beam.io.ReadFromText('test_tweets_anuFYb8.csv', skip_header_lines=1)
       # | "print data" >> beam.ParDo(printdata)
       | "Clean data" >> beam.ParDo(clean_tweets)

       | 'Splitter using beam.Map' >> beam.Map(lambda record: record.split(','))
       | 'Map record to key' >> beam.Map(lambda record: ('data', record))
       | 'GroupBy data' >> beam.GroupByKey( )
       | 'Build Model' >> beam.ParDo(MLmodel)
       # | 'Create table in bq' >> beam.ParDo(export_items_to_bigquery)
       # | 'Write output to file' >> beam.io.WriteToText('gs://gcp_first_bucket/StockMarket/output.txt')
       | 'write to big query' >> beam.io.WriteToBigQuery(
            'my_first_dataset.Twitter_pred', schema='Tweets:STRING,Predicted_Value:STRING')
     )
p.run()





# table_spec = bigquery.TableReference(project='probable-net-247610',
#     dataset_id='my_first_dataset',
#     table_id='test_table')
# table_schema = {'fields': [
#     {'name': 'source', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'quote', 'type': 'STRING', 'mode': 'REQUIRED'}
# ]}
# quotes = p | beam.Create([
#     {'source': 'Mahatma Gandhi', 'quote': 'My life is my message.'},
#     {'source': 'Yoda', 'quote': "Do, or do not. There is no 'try'."},
# ])
# quotes | beam.io.WriteToBigQuery(
#     table_spec,
#     schema=table_schema,
#     write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
#     create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
# quotes.run()


