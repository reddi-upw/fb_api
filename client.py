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


def api_paginate(url, limit=100):
    l = 0
    while url:
        resp = api_get(url)
        yield resp
        l += len(resp['data'])
        if l >= limit:
            break
        url = resp.get('paging', {}).get('next')


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

    def search_pages(self, q, limit=100):
        url = self.build_url(
            method='search',
            params={'q': q, 'type': 'page', 'limit': limit})
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def search_interests(self, q, limit=100):
        url = self.build_url(
            method='search',
            params={'q': q, 'type': 'adinterest'})
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def search_groups(self, q, limit=100):
        url = self.build_url(
            method='search',
            params={'q': q, 'type': 'group'})
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_user_adaccounts(self, user_id='me', limit=100):
        url = self.build_url(method='{}/adaccounts'.format(user_id))
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_adcampaigns(self, adacc_id, limit=100, params=None):
        # docs/marketing-api/reference/ad-account/campaigns/
        params = params or {}
        url = self.build_url(
            method='{}/campaigns'.format(adacc_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_custom_audiences(self, adacc_id, limit=100, params=None):
        # docs/marketing-api/reference/custom-audience#read
        params = params or {}
        url = self.build_url(
            method='{}/customaudiences'.format(adacc_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_adsets(self, adcamp_id, limit=100, params=None):
        # docs/marketing-api/reference/ad-campaign-group/adsets/
        params = params or {}
        url = self.build_url(
            method='{}/adsets'.format(adcamp_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_page(self, page_id, params=None):
        params = params or {}

        url = self.build_url(
            method=str(page_id),
            params=params)
        return api_get(url)

    def fetch_page_likers(self, page_id, limit=100, params=None):
        params = params or {}
        url = self.build_url(
            method='{}/likes'.format(page_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_page_posts(self, page_id, limit=100, params=None):
        params = params or {}
        url = self.build_url(
            method='{}/posts'.format(page_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_page_insights(self, page_id, metrics, limit=100, params=None):
        params = params or {}
        params['metric'] = ','.join(metrics)
        url = self.build_url(
            method='{}/insights'.format(page_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_group(self, group_id, params=None):
        params = params or {}
        url = self.build_url(
            method=str(group_id),
            params=params)
        return api_get(url)

    def fetch_group_connection(self, group_id, conn, limit=100, params=None):
        params = params or {}
        url = self.build_url(
            method='{}/{}'.format(group_id, conn),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_metadata(self, obj_id):
        url = self.build_url(method=str(obj_id), params={'metadata': 1})
        return api_get(url)['metadata']

    def fetch_page_categories(self):
        url = self.build_url(method='fb_page_categories')
        return api_get(url)['data']
