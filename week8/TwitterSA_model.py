
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, WorkerOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
# import TwitterSA_Preprocessing as pp

def printdata(value):
    # print(type(value))
    # tweet = value.split(',')
    print(value)

# def slplt(value):
#     value= (value.split(','))
#     return value


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
    from sklearn.model_selection import train_test_split
    from sklearn.linear_model import LogisticRegression
    from sklearn.feature_extraction.text import CountVectorizer
    import TwitterSA_Preprocessing as pp
    import tensorflow as tf

    vs = [v for v in data[1]]
    df = pd.DataFrame(vs)
    # print(df)
    df = df.iloc[:, 0:3]
    df.columns = ['id', 'label', 'cleaned_tweet']

    # df['cleaned_tweet'] = ''
    # df = pp.preprocessing(df)

    # X_train, X_test, y_train, y_test = train_test_split(df['cleaned_tweet'], df['label'], random_state=0)

    cv = CountVectorizer()
    vect = cv.fit(df['cleaned_tweet'])
    X_train_vectorized = cv.transform(df['cleaned_tweet'])

    logistic_model_cv = LogisticRegression()
    logistic_model_cv.fit(X_train_vectorized, df['label'])
    # pred = logistic_model_cv.predict(vect.transform(X_test))
    # print(pred)
    # print('F1 :', f1_score(y_test, pred))
    filename = "gs://gcp_first_bucket/Model1/TwitterSA_model.pkl"
    with tf.io.gfile.GFile(filename, 'wb') as file:
        pickle.dump(logistic_model_cv, file)
        pickle.dump(cv, file)

    # with open('TwitterSA_model.pkl', 'wb') as file:
    #     pickle.dump(logistic_model_cv, file)
    #     pickle.dump(cv, file)

    print('success')

# p = beam.Pipeline('Directrunner')
# lines = \
#     (p | 'ReadFileFromCSV' >> beam.io.ReadFromText('Google_Stock_Price_Train.csv')
#      | 'Splitter using beam.Map' >> beam.Map(lambda record: record.split(','))
#      | 'Map record to key' >> beam.Map(lambda record: ('data', record))
#      | 'GroupBy data' >> beam.GroupByKey()
#      # | "print data" >> beam.ParDo(printdata)
#      | 'Build Model' >> beam.ParDo(MLmodel)
#      | 'Write output to file' >> beam.io.WriteToText('StockMarketoutput.csv')
#      )
# p.run()


options = PipelineOptions()

google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'probable-net-247610'
google_cloud_options.job_name = 'myjob2'
google_cloud_options.staging_location = 'gs://gcp_first_bucket/staging'
google_cloud_options.temp_location = 'gs://gcp_first_bucket/temp'

worker_options = options.view_as(WorkerOptions)
worker_options.max_num_workers = 15 # default:15

options.view_as(StandardOptions).runner = 'Dataflow'
options.view_as(SetupOptions).save_main_session = True
p = beam.Pipeline(options=options)
lines = \
    (p | 'ReadFileFromCSV' >> beam.io.ReadFromText('train_E6oV3lV.csv', skip_header_lines=1)
     # | "print data" >> beam.ParDo(printdata)
       | "Clean data" >> beam.ParDo(clean_tweets)
       # | "print data" >> beam.ParDo(printdata)
     | 'Splitter using beam.Map' >> beam.Map(lambda record: record.split(','))
     | 'Map record to key' >> beam.Map(lambda record: ('data', record))
     | 'GroupBy data' >> beam.GroupByKey()
     | 'Build Model' >> beam.ParDo(MLmodel)
     # | 'Write output to file' >> beam.io.WriteToText('gs://gcp_first_bucket/StockMarket')
     )
p.run()

