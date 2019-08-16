import argparse
import apache_beam as beam
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from google.cloud import pubsub_v1

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions

import MSG as m
options = PipelineOptions()
p = beam.Pipeline(options=options)

def list_topics(project_id):
    """Lists all Pub/Sub topics in the given project."""
    # [START pubsub_list_topics]
    from google.cloud import pubsub_v1

    # TODO project_id = "Your Google Cloud Project ID"

    publisher = pubsub_v1.PublisherClient()
    project_path = publisher.project_path(project_id)

    for topic in publisher.list_topics(project_path):
        print(topic)

# list_topics("gcp1-248309")


def create_topic(project_id, topic_name):
    from google.cloud import pubsub_v1

    project_id = project_id
    topic_name = topic_name

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)

    topic = publisher.create_topic(topic_path)

    print('Topic created: {}'.format(topic))

#create_topic("gcp1-248309", "MyPubSub")

def publish_messages(project_id, topic_name):
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    data= str(m.Message())
    data = data.encode('utf-8')
    print(data)
    future = publisher.publish(topic_path, data=data)
    print(future.result())

    print('Published messages.')

#publish_messages("gcp1-248309","MyPubSub")

def MLmodel(data):
    dict={}
    list=[]
    dict=eval(data)
    for x in dict['Time Series (1min)'].items():
        d=x[1]
        list.append(d)

    df=pd.DataFrame(list)
    #print(df)

    X=df.iloc[1:, 0:1].values
    #print(X)
    y=df.iloc[1: , 3:4].values
    #print(y)
    x_train, x_test, y_train, y_test = train_test_split(X, y, test_size=0.25, random_state=0)
    model = LinearRegression()
    model.fit(x_train, y_train)
    y_pred=model.predict(x_test)
    #print("Y_Pred",y_pred)

   # print("X-Train", x_train)
    dict_db={}
   # xlist = list(x_train)
   # ylist = list(y_pred)

    l1 = []
    i = 0
    # temp=''
    for xval in x_train:
        temp = {}
        try:
            temp['open'] = str(xval[0])
            temp['close'] = str(y_pred[0])
            l1.append(temp)
        except Exception:
            temp['open'] = ' '
            temp['close'] = ' '
            l1.append(temp)
    print(l1)
    return l1

table_schema = {'fields': [
{'name': 'open', 'type': 'STRING'},
{'name': 'close', 'type': 'STRING'}
]}

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
    | 'Read' >> beam.io.ReadFromPubSub(subscription="projects/gcp1-248309/subscriptions/Stock")
    |'Decode' >> beam.Map(lambda b: b.decode('utf-8'))
    #|'GroupBy data' >> beam.GroupByKey()
    | 'Build Model' >> beam.ParDo(MLmodel)
    #| 'Write ' >> beam.io.WriteToPubSub(topic="projects/gcp1-248309/topics/MyPubSub")
    | 'database' >> beam.io.WriteToBigQuery(schema=table_schema, table="gcp1-248309:my_dataset_id.StockPrice")
 #write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    #| 'Write output to file' >> beam.io.WriteToText('StockPricePrediction.csv')
)
p.run().wait_until_finish()
