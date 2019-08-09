import argparse
import logging
import re

from past.builtins import unicode

from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
import apache_beam as beam
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
from apache_beam.options.pipeline_options import PipelineOptions
import numpy as np
import pandas as pd
import pyparsing as pp
from google.cloud import bigquery

'''
options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.project = 'gcp1-248309'
google_cloud_options.job_name = 'testingjob'
google_cloud_options.staging_location = 'gs://first_project1/staging'
google_cloud_options.temp_location = 'gs://first_project1/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'
'''
def MLmodel(data):
    df = pd.DataFrame(data[1][1:])
    X = df.iloc[:,1:2].values
    y = df.iloc[:, 4:5].values
    print(df.head(), df.info())
    print(X)
    print(X.shape)
    print(y.shape)

    X_train, X_test, y_train, y_test=train_test_split(X, y, test_size=0.2, random_state=0)
    regressor=DecisionTreeRegressor(random_state=0)
    regressor.fit(X_train,y_train)
    print(X_test.shape)
    y_pred=regressor.predict(X_test)
    print(y_pred.shape)
    return y_pred


def export_items_to_bigquery(y_pred):
    # Instantiates a client
    bigquery_client = bigquery.Client()

    # Prepares a reference to the dataset
    dataset_ref = bigquery_client.dataset('my_dataset_id')

    table_ref = dataset_ref.table('StockPrice')
    table = bigquery_client.get_table(table_ref)  # API call

    rows_to_insert = [(y_pred)]
    errors = bigquery_client.insert_rows(table, rows_to_insert)  # API request
    assert errors == []

#export_items_to_bigquery()


p = beam.Pipeline('Directrunner')
lines = \
(p  | 'ReadFileFromCSV' >> beam.io.ReadFromText('GoogleSPT.csv')
    #| 'Map record to key' >> beam.Map(lambda record: ('record', record.split(" ")))
    #| 'map record' >> beam.Map(lambda record: ('record', re.split(r',(?=")', record)))
    |'map record'>> beam.Map(lambda record:('record' , pp.commaSeparatedList.parseString(record).asList()))
    | 'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(MLmodel)
 #   | 'db' >> beam.ParDo(export_items_to_bigquery)
    | 'Write output to file' >> beam.io.WriteToText('StockPricePrediction.csv')
)
p.run()