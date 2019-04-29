"""
Asynchronous tasks that update data in Open Humans.
These tasks:
  1. delete any current files in OH if they match the planned upload filename
  2. adds a data file
"""
import logging
import json
import tempfile
import requests
from celery import shared_task
from django.conf import settings
from open_humans.models import OpenHumansMember
from ohapi import api
import tweepy
from collections import defaultdict
import datetime

# Set up logging.
logger = logging.getLogger(__name__)

TWITTER_GRAPHQL_BASE = 'https://api.twitter.com/graphql'
TWITTER_API_BASE = 'https://api.twitter.com'


@shared_task
def process_twitter(oh_id):
    """
    Update the twitter file for a given OH user
    """
    logger.debug('Starting twitter processing for {}'.format(oh_id))
    oh_member = OpenHumansMember.objects.get(oh_id=oh_id)
    recent_tweet_id, recent_like_id = get_last_id(oh_member.get_access_token())
    twitter_member = oh_member.datasourcemember
    auth = tweepy.OAuthHandler(
                            settings.TWITTER_CLIENT_ID,
                            settings.TWITTER_CLIENT_SECRET)

    auth.set_access_token(
            twitter_member.access_token,
            twitter_member.access_token_secret)
    twitter_api = tweepy.API(auth, wait_on_rate_limit=True)
    update_twitter(oh_member, twitter_api, recent_tweet_id, recent_like_id)


def update_twitter(oh_member, twitter_api, recent_tweet_id, recent_like_id):
    new_tweets = fetch_new_items(recent_tweet_id, 'tweets', twitter_api)
    new_likes = fetch_new_items(recent_tweet_id, 'likes', twitter_api)

    new_items = {'likes': new_likes, 'tweets': new_tweets}
    fetched_months = []
    for endpoint in new_items.keys():
        for m in new_items[endpoint].keys():
            fetched_months.append(m)
    fetched_months = list(set(fetched_months))
    for month in fetched_months:
        monthly_data = {'likes': new_likes[month], 'tweets': new_tweets[month]}
        write_new_tweets(oh_member, twitter_api, month, monthly_data)


def fetch_new_items(recent_since_id, mode, twitter_api):
    new_items = defaultdict(list)
    if mode == 'tweets':
        endpoint = twitter_api.user_timeline
    else:
        endpoint = twitter_api.favorites
    if not recent_since_id:
        for tweet in tweepy.Cursor(
                endpoint,
                tweet_mode='extended').items(1000):
            month = str(tweet.created_at)[:7]
            new_items[month].append(tweet._json)
    else:
        for tweet in tweepy.Cursor(
                endpoint, tweet_mode='extended',
                since_id=recent_since_id).items():
            month = str(tweet.created_at)[:7]
            new_items[month].append(tweet._json)
    return new_items


def write_new_tweets(oh_member, twitter_api, month, new_data):
    existing_files = api.exchange_oauth2_member(
        oh_member.get_access_token(),
        all_files=True)
    old_data = None
    file_id = None
    for dfile in existing_files['data']:
        if dfile['basename'] == 'twitter-data-{}.json'.format(month):
            old_data = requests.get(dfile['download_url']).json()
            file_id = dfile['id']
            break
    if old_data:
        old_data['tweets'] = new_data['tweets'] + old_data['tweets']
        old_data['likes'] = new_data['likes'] + old_data['likes']
    else:
        old_data = {'tweets': new_data['tweets'], 'likes': new_data['likes'],
                    'followers': [], 'following': []}
    if month == str(datetime.datetime.today())[:7]:
        me = twitter_api.me()
        old_data['followers'].append(
            {'timestamp': str(datetime.datetime.today()),
                'value': me.followers_count})
        old_data['following'].append(
            {'timestamp': str(datetime.datetime.today()),
                'value': me.friends_count})
    with tempfile.TemporaryFile() as f:
                js = json.dumps(old_data)
                js = str.encode(js)
                f.write(js)
                f.flush()
                f.seek(0)
                api.upload_stream(
                    f, "twitter-data-{}.json".format(month),
                    metadata={
                        "description": "Twitter Data",
                        "tags": ["Twitter"]
                        }, access_token=oh_member.get_access_token())
    if file_id:
        api.delete_file(
            oh_member.get_access_token(),
            project_member_id=oh_member.oh_id,
            file_id=file_id)


def get_last_id(oh_access_token):
    member = api.exchange_oauth2_member(
        oh_access_token,
        all_files=True)
    twitter_files = {}
    tweet_id = None
    like_id = None
    for dfile in member['data']:
        if 'Twitter' in dfile['metadata']['tags']:
            twitter_files[dfile['basename']] = dfile
    if twitter_files:
        filenames = list(twitter_files.keys())
        filenames.sort()
        last_file = twitter_files[filenames[-1]]
        tf_in = tempfile.NamedTemporaryFile(suffix='.json')
        tf_in.write(requests.get(last_file['download_url']).content)
        tf_in.flush()
        twitter_data = json.load(open(tf_in.name))
        print("fetched last ID from OH")
        print(twitter_data['tweets'][0]['id_str'])
        print('---')
        if len(twitter_data['tweets']):
            tweet_id = twitter_data['tweets'][0]['id_str']
        if len(twitter_data['likes']):
            like_id = twitter_data['tweets'][0]['id_str']
    return tweet_id, like_id
