import tweepy as tw
from tweepy.parsers import JSONParser
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
#consumer key, consumer secret, access token, access secret.
ckey="YvRJym9qa8faOv69e9bzlX0WI"
csecret="ZfpjP2afD4n5pYUAf75xrRAkJpMWVeVlFJmJxXjwkBy9dtCePq"
atoken="1163752501767573504-RbM7QNOlMbearI4pd516ThTMg9BmYp"
asecret="oYtlRDNZ4G1m6XiwvAlba461ctVNkg6B51VyJAuLkylDV"
'''class listener(StreamListener):
    def on_data(self, data):
        all=json.loads(data)
        tweet=all['text']
        print((tweet))
        return True
    def on_error(self, status):
        print(status)
'''
def twt():
    auth = OAuthHandler(ckey, csecret)
    auth.set_access_token(atoken, asecret)
    api = tw.API(auth, wait_on_rate_limit=True)
    search_words='#wild'
    #date_since='2018-15-05'
    tweets = tw.Cursor(api.search, q=search_words, lang="en").items(15)

    max_tweets = 50
    query = 'Ipython'
    searched_tweets = [status._json for status in tw.Cursor(api.search, q=query).items(max_tweets)]
    json_strings = [json.dumps(json_obj) for json_obj in searched_tweets]

    #print(json_strings)
    return json_strings


def publish_messages(project_id, topic_name):
    from google.cloud import pubsub_v1

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_name)
    data= str(twt())
    data = data.encode('utf-8')
    #print(data)
    future = publisher.publish(topic_path, data=data)
    print(future.result())

    print('Published messages.')

publish_messages("gcp1-248309","MyPubSub")