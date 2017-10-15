import sys
import argparse
import json
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

    def fetch_page_posts(self, page_id, limit=1000, params=None):
        url = self.build_url(
            method='{}/posts'.format(page_id),
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--access_token', required=True)
    parser.add_argument('-p', '--page_id', required=True)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    client = FBClient(access_token=args.access_token)

    result = []

    page_posts = []
    for pp in client.fetch_page_posts(args.page_id):
        page_posts.extend(pp)
    result.append({'page_posts': page_posts})

    page_likers = []
    for pl in client.fetch_page_likers(args.page_id):
        page_likers.extend(pl)

    page_page_likers = []
    for pl in page_likers:
        for ppl in client.fetch_page_likers(page_id=pl['id']):
            page_page_likers.extend(ppl)

    ap = []
    for p, c in aggregate_pages(page_page_likers):
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
