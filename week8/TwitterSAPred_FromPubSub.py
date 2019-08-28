import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import pickle
import pandas as pd

def clean_tweets(tweet):

    import string
    import re
    from nltk.corpus import stopwords
    stopwords_english = stopwords.words('english')
    from nltk.stem import PorterStemmer
    stemmer = PorterStemmer()
    from nltk.tokenize import TweetTokenizer

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
    # result = ",".join([data[0], tweet_str])
    print(tweet_str)
    return [tweet_str]


def printdata(value):
    print(value)


def MLmodel(data):

    import pandas as pd
    import tensorflow as tf
    # import google.cloud as storage
    print('ML model starts........')
    # dict = eval(data)
    #
    df = pd.DataFrame([data])
    df.columns = ['tweet']
    print(df)
    #
    # storage_client = storage.Client()
    # bucket = storage_client.get_bucket("gcp_first_bucket")
    # blob = bucket.blob("Model1/TwitterSA_model.pkl")
    # model_local = "TwitterSA_model.pkl"
    # blob.download_to_filename(model_local)

    # cv = CountVectorizer()
    # vect = cv.fit(df['cleaned_tweet'])

    pickle_in = open("TwitterSA_model.pkl", "rb")
    model = pickle.load(pickle_in)
    cv = pickle.load(pickle_in)
    # print(new_data)
    # # make predictions and find the rmse
    preds = model.predict(cv.transform(df['tweet']))
    # print('predicted values', (preds))
    # print('F1 :', f1_score(y_test, preds))
    # print(preds)
    l1 = []
    i = 0
    for val in preds:
        temp = {}
        temp['Tweets'] = df.ix[i, 'tweet']
        temp['Predicted_Value'] = str(val)
        l1.append(temp)
    print(l1)
    # return [{'Years':'hjk', 'salary':'gvj'},{'Years':'hjk', 'salary':'gvj'}]
    # return [{'Predicted_Value': [op],}]
    return l1


options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'probable-net-247610'
google_cloud_options.job_name = 'myjob2'
google_cloud_options.staging_location = 'gs://gcp_first_bucket/staging'
google_cloud_options.temp_location = 'gs://gcp_first_bucket/temp'

worker_options = options.view_as(WorkerOptions)
worker_options.max_num_workers = 15 # default:15

options.view_as(StandardOptions).runner = 'Dataflow'
options.view_as(StandardOptions).streaming = True
options.view_as(SetupOptions).save_main_session = True
p = beam.Pipeline(options=options)

project_id = 'probable-net-247610'
topic_name = 'MyTopic_tweets'
subscription_name = 'MySub_tweets'
# op = rec.receive_messages(project_id, subscription_name)
lines = \
    (p | 'Read Data From PubSub' >> beam.io.ReadFromPubSub(subscription='projects/probable-net-247610/subscriptions/MySub_tweets').with_output_types(bytes)
       | 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
       | "Clean data" >> beam.ParDo(clean_tweets)
       # | "print data" >> beam.ParDo(printdata)
       # | 'Splitter using beam.Map' >> beam.Map(lambda record: record.split(','))
       # | 'Map record to key' >> beam.Map(lambda record: ('data', record))
       # | 'GroupBy data' >> beam.GroupByKey()
       | 'Build Model' >> beam.ParDo(MLmodel)
       # | "print data" >> beam.ParDo(printdata)
       | 'write to big query' >> beam.io.WriteToBigQuery(
          'my_first_dataset.Twitter_pred', schema='Tweets:STRING,Predicted_Value:STRING')
     # | 'Write output to file' >> beam.io.WriteToText('gs://gcp_first_bucket/StockMarket')
     )

p.run().wait_until_finish()






