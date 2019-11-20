from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "2332816866-9UP75baLXJbGIGqeOHPxyQ1cyufb5KBF9tTuHeZ"
access_token_secret =  "ivyOoMm4556sHo0Eois2Tzi9nwNPsl00SBYXsqNjjEHDR"
consumer_key =  "nhV8wu1ye7YQlvcHWqTL3eMnq"
consumer_secret =  "OhPXsSqKpvZmtpaZRzYq3j3PvnFd3luBjz6YmMUWa11fFIrfG4"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("twitter", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track="twitter")
