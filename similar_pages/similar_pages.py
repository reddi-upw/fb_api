import sys
import argparse
import json
from datetime import datetime, timedelta
from functools import partial
from urllib.parse import urlencode

import requests
from dateutil import parser


def api_get(url):
    r = requests.get(url).json()
    e = r.get('error')
    if e:
        raise Exception(
            'an error occured with url {}: {}'.format(u, e.get('message')))
    return r


class FBClient:

    BASE_URL = 'https://graph.facebook.com/v2.10'

    def __init__(self, app_id, app_secret):
        self.app_id = app_id
        self.app_secret = app_secret

    def build_url(self, method, params=None):
        params = params or {}
        params['access_token'] = self.access_token
        return '{}/{}?{}'.format(self.BASE_URL, method, urlencode(params))

    @property
    def access_token(self):
        return '{}|{}'.format(self.app_id, self.app_secret)

    def search_pages(self, q, limit):
        url = self.build_url(
            method='search',
            params={'q': q, 'type': 'page', 'limit': limit})
        return api_get(url)['data']

    def fetch_page_likers(self, page_id, limit=1000):
        url = self.build_url(method='{}/likes'.format(page_id))
        return api_get(url)['data']

    def fetch_page_insights(self, page_id, metric, params):
        url = self.build_url(
            method='{}/insights/{}'.format(page_id, metric),
            params=params)

        while url:
            resp = api_get(url)
            yield resp['data']
            url = resp.get('paging', {}).get('next')


METRIC_NAMES = [('page_fans_country', 'page_storytellers_by_country')]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--query', required=True)
    parser.add_argument('-i', '--app_id', required=True)
    parser.add_argument('-s', '--app_secret', required=True)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    client = FBClient(app_id=args.app_id, app_secret=args.app_secret)

    pages = client.search_pages(args.query, limit=1)
    if pages:
        page = pages[0]
    else:
        return 'No page found for query {}'.format(args.query)

    result = []
    result.append({'page': page})
    result.append({'page_likers': client.fetch_page_likers(page['id'])})

    metrics = []
    for m in METRIC_NAMES:
        gen = client.fetch_page_insights(
            page_id=page['id'],
            metric=','.join(m),
            params={'period': 'week'})

        for data in gen:
            metrics.extend(data)
    metrics.sort(key=lambda m: m['name'])

    result.append({'metrics': metrics})
    dumped = json.dumps({'result': result}, indent=4)

    if args.output:
        with open(args.output, 'w', encoding='utf-8') as f:
            f.write(dumped)
    else:
        sys.stdout.write(dumped)


if __name__ == '__main__':
    main()
