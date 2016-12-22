from elasticsearch import Elasticsearch, RequestsHttpConnection
from gender_detector import GenderDetector
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream, API
from textblob import TextBlob
from geolocation.main import GoogleMaps
import datetime
from dateutil import parser
import string
import json
import re
import time
import config


from nltk.corpus import stopwords, wordnet
from nltk import bigrams, trigrams


class TweetAnalyzer():
    def __init__(self, index_name='presidents', doc_type='tweet', create_new_index=False):
        self.gender_detector = GenderDetector()
        self.index = index_name
        self.doc_type = doc_type
        self.elastic = Elasticsearch(connection_class=RequestsHttpConnection)
        self._setup_elastic_index(create_new_index)
        self._googlemaps_api_authentication()

    def _googlemaps_api_authentication(self):
        self.googlemaps_api = GoogleMaps(api_key=config.app_config['google_api_key'])

    def _setup_elastic_index(self, create_new=False):
        """
        setup elasticsearch index and setup the mapping
        :param create_new:
        :return:
        """
        if not self.elastic.indices.exists(self.index):
            self._create_index()

        if create_new:
            self.elastic.indices.delete(index=self.index, ignore=[400, 404])
            self._create_index()

    def _create_index(self):
        self.elastic.indices.create(
            index=self.index,
            ignore=400
        )
        _mappings = {
            self.doc_type: {
                "properties": {
                    "user_location_coordinate": {"type": "geo_point"},
                    "query": {"type": "string", "index": "not_analyzed"}
                }
            }
        }
        self.elastic.indices.put_mapping(index=self.index, doc_type=self.doc_type, body=_mappings)

    def _sentiment_analyzer(self, tweet):
        tweet = self._tweet_cleaner(tweet)
        res = TextBlob(tweet)
        polarity = res.sentiment.polarity
        if polarity < 0:
            sentiment = 'negative'
        elif polarity == 0:
            sentiment = 'neutral'
        else:
            sentiment = 'positive'

        return (polarity, sentiment)

    def _tweet_cleaner(self, tweet):
        """
        Prepare tweet by removing unwanted sections
        :param tweet: original tweet
        :return: prepared tweet
        """
        # Convert to lower case
        tweet = tweet.lower()
        # Convert www.* or https?://* to empty string
        tweet = re.sub('((www\.[\s]+)|(https?://[^\s]+))', '', tweet)
        # Convert @username to empty string
        tweet = re.sub('@[^\s]+', '', tweet)
        # Remove additional white spaces
        tweet = re.sub('[\s]+', ' ', tweet)
        # Replace #word with word
        tweet = re.sub(r'#([^\s]+)', r'\1', tweet)
        # trim
        tweet = tweet.strip('\'"')

        return tweet

    def _tokenizer(self, tweet):
        tokens = []
        for word in tweet.split():
            if len(word) > 3 and word not in stopwords.words('english') and wordnet.synsets(word):
                tokens.append(word)
        return tokens

    def _get_geo_ponits(self, address):
        if not address:
            return None
        coordinate = None
        res = self.googlemaps_api.search(address.strip(string.punctuation + ' ')).first()
        if res:
            coordinate = [res.lat, res.lng]
        return coordinate

    def _guess_gender(self, name):
        gender = None
        try:
            gender = self.gender_detector.guess(name)
            return gender
        except Exception as e:
            print('error in gender detector')

    def index_tweet(self, data, query):
        tweet = json.loads(data)
        if not (tweet.get('text') and tweet.get('lang') == 'en'):
            raise Exception("err text doest exists or it is not english")

        polarity, sentiment = self._sentiment_analyzer(tweet['text'])
        coordinates = self._get_geo_ponits(tweet['user']['location'])
        gender = self._guess_gender(tweet['user']['name'].split()[0])
        tweet_tokens = self._tokenizer(self._tweet_cleaner(tweet["text"]))

        my_tweet = {
            "author": tweet["user"]["screen_name"],
            "tweet_geo": tweet['geo'],
            "tweet_lang": tweet['lang'],
            "tweet_place": tweet['place'],
            "user_description": tweet['user']['description'],
            "user_followers_count": tweet['user']['followers_count'],
            "user_friends_count": tweet['user']['friends_count'],
            "user_lang": tweet['user']['lang'],
            "user_name": tweet['user']['name'],
            "user_location_name": tweet['user']['location'],
            "user_location_coordinate": {"lat": coordinates[0], "lon": coordinates[1]} if coordinates else None,
            "user_status_count": tweet['user']['statuses_count'],
            "tweet_created_at": parser.parse(tweet['created_at']),
            "user_created_at": parser.parse(tweet['user']['created_at']),
            "tweet_tokens": tweet_tokens,
            'bigrams': ["_".join(x) for x in bigrams(tweet_tokens)],
            'trigrams': ["_".join(x) for x in trigrams(tweet_tokens)],
            "polarity": polarity,
            "sentiment": sentiment,
            "gender": gender,
            "query": query,
            "fetch_time": datetime.datetime.now(),
        }
        try:
            print(self.elastic.index(self.index, self.doc_type, body=my_tweet), query)
        except Exception as e:
            print(e)


class TweetStreamListener(StreamListener):
    def __init__(self, query):
        self.query = query
        self.tweet_analyzer = TweetAnalyzer()

    def on_data(self, data):
        try:
            self.tweet_analyzer.index_tweet(data, self.query)
        except Exception as e:
            print(e)
            time.sleep(1)

        return True

    # on failure
    def on_error(self, status):
        print status


def extract(query):
    twitter_config = config.app_config['twitter']
    # Auth my app
    auth = OAuthHandler(twitter_config['consumer_key'], twitter_config['consumer_secret'])
    # Auth of user
    auth.set_access_token(twitter_config['access_token'], twitter_config['access_token_secret'])

    listener = TweetStreamListener(query)
    # create instance of the tweepy stream
    while True:
        try:
            stream = Stream(auth, listener)
            # start
            stream.filter(track=query)
        except:
            continue


if __name__ == "__main__":
    extract('Justin Trudeau')
