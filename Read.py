import MSG as m  

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

publish_messages("gcp1-248309","MyPubSub")

