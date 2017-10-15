import sys
import argparse
import json
from multiprocessing import Pool
from datetime import datetime, timedelta
from functools import partial
from urllib import urlencode

import requests
from dateutil import parser


def api_get(url):
    r = requests.get(url).json()
    e = r.get('error')
    if e:
        raise Exception(
            'an error occured with url {}: {}'.format(url, e.get('message')))
    return r


class FBClient(object):

    BASE_URL = 'https://graph.facebook.com/v2.10'

    def __init__(self, access_token=None, app_id=None, app_secret=None):
        self._access_token = access_token
        self.app_id = app_id
        self.app_secret = app_secret

    def build_url(self, method, params=None):
        params = params or {}
        params['access_token'] = self.access_token
        return '{}/{}?{}'.format(self.BASE_URL, method, urlencode(params))

    @property
    def access_token(self):
        if not self._access_token:
            return '{}|{}'.format(self.app_id, self.app_secret)
        return self._access_token

    @access_token.setter
    def access_token(self, v):
        self._access_token = v

    def fetch_user_posts(self, user_id='me', limit=1000, params=None):
        url = self.build_url(
            method='{}/posts'.format(user_id),
            params=params)
        while url:
            resp = api_get(url)
            yield resp['data']
            url = resp.get('paging', {}).get('next')

    def fetch_user_likes(self, user_id='me', limit=1000, params=None):
        url = self.build_url(
            method='{}/likes'.format(user_id),
            params=params)
        while url:
            resp = api_get(url)
            yield resp['data']
            url = resp.get('paging', {}).get('next')

    def fetch_page_likers(self, page_id, limit=1000, params=None):
        url = self.build_url(
            method='{}/likes'.format(page_id),
            params=params)
        while url:
            resp = api_get(url)
            yield resp['data']
            url = resp.get('paging', {}).get('next')


def aggregate_pages(pages):
    data = {}
    for p in pages:
        val = data.get(p['id'])
        if not val:
            data[p['id']] = p, 1
        else:
            data[p['id']] = val[0], val[1] + 1
    return sorted(data.values(), key=lambda v: v[1], reverse=True)


def fetch_all_page_likers(page, client):
    result = []
    for pl in client.fetch_page_likers(page_id=page['id']):
        result.extend(pl)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--access_token', required=True)
    parser.add_argument('-u', '--user_id', required=False)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    client = FBClient(access_token=args.access_token)

    result = []

    user_posts = []
    for up in client.fetch_user_posts():
        user_posts.extend(up)
    result.append({'user_posts': user_posts})

    user_likes = []
    for ul in client.fetch_user_likes():
        user_likes.extend(ul)

    pages_likers = []
    p = Pool(4)
    worker = partial(fetch_all_page_likers, client=client)
    for pl in map(worker, user_likes):
        pages_likers.extend(pl)

    ap = []
    for p, c in aggregate_pages(pages_likers):
        p['counter'] = c
        ap.append(p)
    result.append({'pages': ap})

    dumped = json.dumps({'result': result}, indent=4)
    if args.output:
        with open(args.output, 'wb') as f:
            f.write(dumped)
    else:
        sys.stdout.write(dumped)


if __name__ == '__main__':
    main()
