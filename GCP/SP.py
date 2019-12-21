import pandas as pd
import apache_beam as beam
import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
# for normalizing data
from sklearn.preprocessing import MinMaxScaler
import numpy as np
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
import pickle
import pyparsing as pp
from google.cloud import bigquery
options = PipelineOptions()
p = beam.Pipeline(options=options)

def printdata(value):
    print(value[1])

def MLmodel(data):
    vs = [v for v in data[1]]
    df = pd.DataFrame(vs)

    features = [0,5,6,7]
    df = df.drop(features, axis=1)
    X = df.drop([4], axis=1)
    y = df.drop([1,2,3], axis=1)

    #y = np.array(y).astype(np.float)
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=0)
    # implement linear regression
    model = LinearRegression()
    model.fit(x_train, y_train)
    y_pred=model.predict(x_test)
    print(y_pred)
   # print(x_train.shape)
    '''pickle_out = open("stockMarket_model.pickle", "wb")
    pickle.dump(model, pickle_out)
    pickle_out.close()'''

    # # make predictions and find the rmse
    # preds = model.predict(x_test)
    # print('predicted values', (preds))
    # print model.score(x_test,y_test,preds)

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'job30'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(SetupOptions).save_main_session = True
options.view_as(StandardOptions).runner = 'DataflowRunner'


p = beam.Pipeline(options=options)
lines = \
(p | 'ReadFileFromCSV' >> beam.io.ReadFromText('gs://first_project1/GoogleSPT.csv', skip_header_lines=1)
   | 'Map record to key' >> beam.Map(lambda record: ('record', record.split(',')))
   #| 'map record' >> beam.Map(lambda record: ('record', re.split(r',(?=")', record)))
   # |'map record'>> beam.Map(lambda record:('record', pp.commaSeparatedList.parseString(record).asList()))
    | 'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(MLmodel)
   # | 'database' >> beam.io.WriteToBigQuery(schema=table_schema, table="gcp1-248309:my_dataset_id.StockPrice")
 #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    | 'Write output to file' >> beam.io.WriteToText('StockPricePrediction.csv')
)
p.run()


