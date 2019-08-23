import tweepy as tw
import pandas as pd
from tweepy.parsers import JSONParser
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json

import apache_beam as beam
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from google.cloud import pubsub_v1

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions


options = PipelineOptions()
p = beam.Pipeline(options=options)

def MLmodel(data):
    dict={}
    dict = eval(data)
    print(dict)
    print(type(dict))
    #for x in dict.keys():
    #    print(x)

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'job30'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).streaming = True
options.view_as(StandardOptions).runner = 'DirectRunner'

p = beam.Pipeline(options=options)
lines = \
(p  #| 'Build Model' >> beam.ParDo(publish_messages)
    | 'Read' >> beam.io.ReadFromPubSub(subscription="projects/gcp1-248309/subscriptions/Twitter")
    |'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
    #|'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(MLmodel)
    #| 'Write ' >> beam.io.WriteToPubSub(topic="projects/gcp1-248309/topics/MyPubSub")
    #| 'database' >> beam.io.WriteToBigQuery(schema=table_schema, table="gcp1-248309:my_dataset_id.StockPrice")
 #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    #| 'Write output to file' >> beam.io.WriteToText('StockPricePrediction.csv')
)
p.run().wait_until_finish()
