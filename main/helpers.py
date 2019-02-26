from ohapi import api
from django.conf import settings
import arrow
from datetime import timedelta


def get_twitter_files(oh_member):
    try:
        oh_access_token = oh_member.get_access_token(
                                client_id=settings.OPENHUMANS_CLIENT_ID,
                                client_secret=settings.OPENHUMANS_CLIENT_SECRET)
        user_object = api.exchange_oauth2_member(oh_access_token)
        files = []
        for dfile in user_object['data']:
            if 'Twitter' in dfile['metadata']['tags']:
                files.append(dfile)
        if files:
            files.sort(key=lambda x: x['basename'], reverse=True)
        return files

    except:
        return 'error'


def check_update(twitter_member):
    if twitter_member.last_submitted < (arrow.now() - timedelta(hours=1)):
        return True
    return False
