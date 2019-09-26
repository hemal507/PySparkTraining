from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient

access_token = "<access_token needs to added here>"
access_token_secret =  "<access_token_secret needs to be added here>"
consumer_key =  "consumer_key needs to be added here"
consumer_secret =  "consumer_secret needs to be added here"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("twitter_topic", data.encode('utf-8'))
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
stream.filter(track=["Microsoft", "Google", "microsoft", "google"])