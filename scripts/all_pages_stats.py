import sys
import argparse
import json
from datetime import datetime

from dateutil import parser
from dateutil.relativedelta import relativedelta
from elasticsearch import Elasticsearch

import config
from client import FBClient


def prepare_page_fields(fb, page_id):
    fields = []
    for f in fb.fetch_metadata(page_id)['fields']:
        name = f['name']
        if name not in config.EXCLUDED_FIELDS:
            fields.append(name)

    metrics = []
    for m in config.METRICS:
        if m not in config.EXCLUDED_METRICS:
            metrics.append(m)

    insights = 'insights.metric({})'.format(','.join(metrics))
    feed = 'feed.limit({}){{{}}}'.format(
        config.FEED_LIMIT,
        ','.join(config.FEED_FIELDS))

    fields.extend([feed, insights])
    return fields


def parse_categories(categories):
    for cat in categories:
        yield cat
        children = cat.get('fb_page_categories', [])
        for child in parse_categories(children):
            yield child


def search(fb):
    categories = fb.fetch_page_categories()

    for cat in parse_categories(categories):
        pages_gen = fb.search_pages(
            q=cat['name'],
            limit=config.PAGES_PER_CATEGORY_LIMIT)

        for pages in pages_gen:
            page_ids = [p['id'] for p in pages]
            for page_id in page_ids:
                fields = prepare_page_fields(fb, page_id)
                page = fb.fetch_page(
                    page_id,
                    params={'fields': ','.join(fields)})
                yield page, cat


def is_page_actual(page):
    kwargs = {'{}s'.format(config.PERIOD): 1}
    dt = datetime.utcnow() - relativedelta(**kwargs)
    feed = page.get('feed', {}).get('data', [])
    if feed:
        post = feed[0]
        return (
            parser.parse(post['created_time']).replace(tzinfo=None) >= dt)
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--access_token', required=False)
    parser.add_argument('-i', '--app_id', required=False)
    parser.add_argument('-s', '--app_secret', required=False)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    fb = FBClient(
        access_token=args.access_token,
        app_id=args.app_id,
        app_secret=args.app_secret)

    es = Elasticsearch(
        hosts=[{'host': config.ES_HOST, 'port': config.ES_PORT}])

    result = []
    for page, cat in search(fb=fb):
        if is_page_actual(page):
            for post in page.get('feed', {}).get('data', []):
                post.get('likes', {}).pop('paging', None)
            page.get('feed', {}).pop('paging', None)
            page.get('insights', {}).pop('paging', None)
            page['category'] = cat

            result.append(page)
            es.index(index='pages', doc_type='page', body=page)
            sys.stdout.write(json.dumps(page, indent=4))

    if args.output:
        dumped = json.dumps({'result': result}, indent=4)
        with open(args.output, 'wb') as f:
            f.write(dumped)


if __name__ == '__main__':
    main()
