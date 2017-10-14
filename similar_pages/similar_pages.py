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

    def fetch_page_insights(self, page_id, metric, period):
        url = self.build_url(
            method='{}/insights/{}'.format(page_id, metric),
            params={'period': period})
        try:
            return api_get(url)['data'][0]['values'][-1]
        except IndexError:
            return None

    def fetch_page_posts(self, page_id, limit=100):
        url = self.build_url(
            method='{}/posts'.format(page_id),
            params={'limit': limit})

        while url:
            resp = api_get(url)
            data = resp.get('data')
            if not data:
                return
            yield resp['data']
            url = resp.get('paging', {}).get('next')

    def fetch_post_likers(self, post_id, limit=100):
        url = self.build_url(
            method='{}/likes'.format(post_id),
            params={'limit': limit, 'fields': 'profile_type'})

        while url:
            resp = api_get(url)
            data = resp.get('data')
            if not data:
                return
            yield [l for l in resp['data'] if l['profile_type'] == 'user']
            url = resp.get('paging', {}).get('next')

    def fetch_user_likes(self, user_id):
        url = self.build_url(method='{}/likes'.format(user_id))
        return api_get(url)['data']


def get_last_posts(posts, dt):
    result = []
    posts = sorted(posts, key=lambda p: p['created_time'], reverse=True)
    for p in posts:
        if parser.parse(p['created_time']).replace(tzinfo=None) < dt:
            break
        result.append(p)
    return result


def fetch_likers(client, page_id, limit=1000):
    result = []

    posts_gen = client.fetch_page_posts(page_id)
    last_posts = []
    dt = datetime.now() - timedelta(days=30)
    for p in posts_gen:
        lp = get_last_posts(p, dt)
        last_posts.extend(lp)
        if len(p) > len(lp):
            break

    post_ids = [p['id'] for p in last_posts]
    chunk_size = max(limit // len(post_ids), 50)
    likers_gens = [
        client.fetch_post_likers(p, limit=chunk_size) for p in post_ids]

    while True:
        if not likers_gens:
            return result

        new_likers_gens = []
        for g in likers_gens:
            if len(result) > limit:
                return result
            try:
                likers = next(g)
            except StopIteration:
                pass
            else:
                likers = [l for l in likers if l['profile_type'] == 'user']
                result.extend(likers)
                new_likers_gens.append(g)
        likers_gens = new_likers_gens
    return result


def aggregate_pages(pages):
    data = {}
    for p in pages:
        val = data.get(p['id'])
        if not val:
            data[p['id']] = p, 1
        else:
            data[p['id']] = val[0], val[1] + 1
    return sorted(data.values(), key=lambda v: v[1], reverse=True)


def find_similar_pages(client, page_id):
    likers = fetch_likers(client, page_id)
    result = []
    for i, liker in enumerate(likers):
        result.extend(client.fetch_user_likes(liker['id']))
    return aggregate_pages(result)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-q', '--query', required=True)
    parser.add_argument('-i', '--app_id', required=True)
    parser.add_argument('-s', '--app_secret', required=True)
    args = parser.parse_args()

    client = FBClient(app_id=args.app_id, app_secret=args.app_secret)

    pages = client.search_pages(args.query, limit=1)
    if pages:
        page = pages[0]
    else:
        return 'No page found for query {}'.format(args.query)

    page_likers = client.fetch_page_likers(page['id'])

    fans = client.fetch_page_insights(
        page_id=page['id'],
        metric='page_fans_country',
        period='lifetime')

    storytellers = client.fetch_page_insights(
        page_id=page['id'],
        metric='page_storytellers_by_country',
        period='days_28')

    # similar_pages = find_similar_pages(client, page['id'])

    s = partial(json.dumps, indent=4, sort_keys=True)
    print('SEARCH RESULT:', s(page))
    print()
    print('PAGE LIKERS:', s(page_likers))
    print()
    print('LIKES BY COUNTRY:', s(fans))
    print()
    print('STORYTELLERS BY COUNTRY:', s(storytellers))
    # print()
    # print('SIMILAR PAGES:', s(similar_pages))


if __name__ == '__main__':
    main()
