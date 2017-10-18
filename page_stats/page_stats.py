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

    def fetch_user_adaccounts(self, user_id='me', limit=100):
        url = self.build_url(method='{}/adaccounts'.format(user_id))
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_adcampaigns(self, adacc_id, limit=100, params=None):
        # https://developers.facebook.com/docs/marketing-api/reference/ad-account/campaigns/
        params = params or {}
        url = self.build_url(
            method='{}/campaigns'.format(adacc_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_custom_audiences(self, adacc_id, limit=100, params=None):
        # https://developers.facebook.com/docs/marketing-api/reference/custom-audience#read
        params = params or {}
        url = self.build_url(
            method='{}/customaudiences'.format(adacc_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_adsets(self, adcamp_id, limit=100, params=None):
        # https://developers.facebook.com/docs/marketing-api/reference/ad-campaign-group/adsets/
        params = params or {}
        url = self.build_url(
            method='{}/adsets'.format(adcamp_id),
            params=params)
        for p in api_paginate(url, limit=limit):
            yield p['data']

    def fetch_page_likers(self, page_id, limit=100, params=None):
        url = self.build_url(
            method='{}/likes'.format(page_id),
            params=params)
        l = 0
        while url:
            resp = api_get(url)
            yield resp['data']
            l += len(resp['data'])
            if l >= limit:
                break
            url = resp.get('paging', {}).get('next')

    def fetch_page_posts(self, page_id, limit=100, params=None):
        url = self.build_url(
            method='{}/posts'.format(page_id),
            params=params)
        l = 0
        while url:
            resp = api_get(url)
            yield resp['data']
            l += len(resp['data'])
            if l >= limit:
                break
            url = resp.get('paging', {}).get('next')

    def fetch_page_insights(self, page_id, metrics, params=None):
        params = params or {}
        params['metric'] = ','.join(metrics)
        url = self.build_url(
            method='{}/insights'.format(page_id),
            params=params)

        while url:
            resp = api_get(url)
            yield resp['data']
            url = resp.get('paging', {}).get('next')

    def fetch_page_metadata(self, page_id):
        url = self.build_url(method=str(page_id), params={'metadata': 1})
        return api_get(url)['metadata']

    def search_interests(self, q):
        url = self.build_url(
            method='search',
            params={'q': q, 'type': 'adinterest'})
        return api_get(url)['data']


def aggregate_categories(pages):
    data = {}
    for p in pages:
        cat = p.get('category')
        if not cat:
            continue

        val = data.get(cat, 0)
        data[cat] = val + 1
    return list(sorted(data.items(), key=lambda v: v[1], reverse=True))


METRIC_NAMES = [
    ('page_stories',
     'page_storytellers',
     'page_stories_by_story_type',
     'page_storytellers_by_story_type',
     'page_storytellers_by_age_gender',
     'page_storytellers_by_city',
     'page_storytellers_by_country',
     'page_storytellers_by_locale',
     'post_stories',
     'post_storytellers',
     'post_stories_by_action_type',
     'post_storytellers_by_action_type',
     'post_story_adds',
     'post_story_adds_unique',
     'post_story_adds_by_action_type',
     'post_story_adds_by_action_type_unique',),

    ('page_impressions',
     'page_impressions_unique',
     'page_impressions_paid',
     'page_impressions_paid_unique',
     'page_impressions_organic',
     'page_impressions_organic_unique',
     'page_impressions_viral',
     'page_impressions_viral_unique',
     'page_impressions_by_story_type',
     'page_impressions_by_story_type_unique',
     'page_impressions_by_city_unique',
     'page_impressions_by_country_unique',
     'page_impressions_by_locale_unique',
     'page_impressions_by_age_gender_unique',
     'page_impressions_frequency_distribution',
     'page_impressions_viral_frequency_distribution',
     'page_impressions_by_paid_non_paid',
     'page_impressions_by_paid_non_paid_unique',),

    ('page_engaged_users',
     'page_post_engagements',
     'page_consumptions',
     'page_consumptions_unique',
     'page_consumptions_by_consumption_type',
     'page_consumptions_by_consumption_type_unique',
     'page_places_checkin_total',
     'page_places_checkin_total_unique',
     'page_places_checkin_mobile',
     'page_places_checkin_mobile_unique',
     'page_places_checkins_by_age_gender',
     'page_places_checkins_by_locale',
     'page_places_checkins_by_country',
     'page_negative_feedback',
     'page_negative_feedback_unique',
     'page_negative_feedback_by_type',
     'page_negative_feedback_by_type_unique',
     'page_positive_feedback_by_type',
     'page_positive_feedback_by_type_unique',
     'page_fans_online',
     'page_fans_online_per_day',
     'page_fan_adds_by_paid_non_paid_unique',),

    ('page_actions_post_reactions_like_total',
     'page_actions_post_reactions_love_total',
     'page_actions_post_reactions_wow_total',
     'page_actions_post_reactions_haha_total',
     'page_actions_post_reactions_sorry_total',
     'page_actions_post_reactions_anger_total',
     'page_actions_post_reactions_total',),

    ('page_total_actions',
     'page_cta_clicks_logged_in_total',
     'page_cta_clicks_logged_in_unique',
     'page_cta_clicks_by_site_logged_in_unique',
     'page_cta_clicks_by_age_gender_logged_in_unique',
     'page_cta_clicks_logged_in_by_country_unique',
     'page_cta_clicks_logged_in_by_city_unique',
     'page_call_phone_clicks_logged_in_unique',
     'page_call_phone_clicks_by_age_gender_logged_in_unique',
     'page_call_phone_clicks_logged_in_by_country_unique',
     'page_call_phone_clicks_logged_in_by_city_unique',
     'page_call_phone_clicks_by_site_logged_in_unique',
     'page_get_directions_clicks_logged_in_unique',
     'page_get_directions_clicks_by_age_gender_logged_in_unique',
     'page_get_directions_clicks_logged_in_by_country_unique',
     'page_get_directions_clicks_logged_in_by_city_unique',
     'page_get_directions_clicks_by_site_logged_in_unique',
     'page_website_clicks_logged_in_unique',
     'page_website_clicks_by_age_gender_logged_in_unique',
     'page_website_clicks_logged_in_by_country_unique',
     'page_website_clicks_logged_in_by_city_unique',
     'page_website_clicks_by_site_logged_in_unique',),

    ('page_fans',
     'page_fans_locale',
     'page_fans_city',
     'page_fans_country',
     'page_fans_gender_age',
     'page_fan_adds',
     'page_fan_adds_unique',
     'page_fans_by_like_source',
     'page_fans_by_like_source_unique',
     'page_fans_group_by_like_source_unique',
     'page_fan_removes',
     'page_fan_removes_unique',
     'page_fans_by_unlike_source_unique',),

    ('page_tab_views_login_top_unique',
     'page_tab_views_login_top',
     'page_tab_views_logout_top',),

    ('page_views_total',
     'page_views_logout',
     'page_views_logged_in_total',
     'page_views_logged_in_unique',
     'page_views_external_referrals',
     'page_views_by_profile_tab_total',
     'page_views_by_profile_tab_logged_in_unique',
     'page_views_by_internal_referer_logged_in_unique',
     'page_views_by_site_logged_in_unique',
     'page_views_by_age_gender_logged_in_unique',
     'page_views',
     'page_views_unique',
     'page_views_login',
     'page_views_login_unique',
     'page_visits_logged_in_by_referers_unique',),

    ('page_video_views',
     'page_video_views_paid',
     'page_video_views_organic',
     'page_video_views_by_paid_non_paid',
     'page_video_views_autoplayed',
     'page_video_views_click_to_play',
     'page_video_views_unique',
     'page_video_repeat_views',
     'page_video_complete_views_30s',
     'page_video_complete_views_30s_paid',
     'page_video_complete_views_30s_organic',
     'page_video_complete_views_30s_autoplayed',
     'page_video_complete_views_30s_click_to_play',
     'page_video_complete_views_30s_unique',
     'page_video_complete_views_30s_repeat_views',
     'post_video_complete_views_30s_autoplayed',
     'post_video_complete_views_30s_clicked_to_play',
     'post_video_complete_views_30s_organic',
     'post_video_complete_views_30s_paid',
     'post_video_complete_views_30s_unique',
     'page_video_views_10s',
     'page_video_views_10s_paid',
     'page_video_views_10s_organic',
     'page_video_views_10s_autoplayed',
     'page_video_views_10s_click_to_play',
     'page_video_views_10s_unique',
     'page_video_views_10s_repeat',
     'page_video_view_time',),

    ('page_posts_impressions',
     'page_posts_impressions_unique',
     'page_posts_impressions_paid',
     'page_posts_impressions_paid_unique',
     'page_posts_impressions_organic',
     'page_posts_impressions_organic_unique',
     'page_posts_impressions_viral',
     'page_posts_impressions_viral_unique',
     'page_posts_impressions_frequency_distribution',
     'page_posts_impressions_by_paid_non_paid',
     'page_posts_impressions_by_paid_non_paid_unique',
     'post_interests_impressions',
     'post_interests_impressions_unique',
     'post_interests_consumptions_unique',
     'post_interests_consumptions',
     'post_interests_consumptions_by_type_unique',
     'post_interests_consumptions_by_type',
     'post_interests_action_by_type_unique',
     'post_interests_action_by_type',),

    ('post_impressions',
     'post_impressions_unique',
     'post_impressions_paid',
     'post_impressions_paid_unique',
     'post_impressions_fan',
     'post_impressions_fan_unique',
     'post_impressions_fan_paid',
     'post_impressions_fan_paid_unique',
     'post_impressions_organic',
     'post_impressions_organic_unique',
     'post_impressions_viral',
     'post_impressions_viral_unique',
     'post_impressions_by_story_type',
     'post_impressions_by_story_type_unique',
     'post_impressions_by_paid_non_paid',
     'post_impressions_by_paid_non_paid_unique',),

    ('post_consumptions',
     'post_consumptions_unique',
     'post_consumptions_by_type',
     'post_consumptions_by_type_unique',
     'post_engaged_users',
     'post_negative_feedback',
     'post_negative_feedback_unique',
     'post_negative_feedback_by_type',
     'post_negative_feedback_by_type_unique',
     'post_engaged_fan',
     'post_fan_reach',
     'page_story_adds',
     'page_story_adds_by_age_gender_unique',
     'page_story_adds_by_city_unique',
     'page_story_adds_by_country_unique',),

    ('post_reactions_like_total',
     'post_reactions_love_total',
     'post_reactions_wow_total',
     'post_reactions_haha_total',
     'post_reactions_sorry_total',
     'post_reactions_anger_total',
     'post_reactions_by_type_total',),

    ('post_video_avg_time_watched',
     'post_video_complete_views_organic',
     'post_video_complete_views_organic_unique',
     'post_video_complete_views_paid',
     'post_video_complete_views_paid_unique',
     'post_video_retention_graph',
     'post_video_retention_graph_clicked_to_play',
     'post_video_retention_graph_autoplayed',
     'post_video_views_organic',
     'post_video_views_organic_unique',
     'post_video_views_paid',
     'post_video_views_paid_unique',
     'post_video_length',
     'post_video_views',
     'post_video_views_unique',
     'post_video_views_autoplayed',
     'post_video_views_clicked_to_play',
     'post_video_views_10s',
     'post_video_views_10s_unique',
     'post_video_views_10s_autoplayed',
     'post_video_views_10s_clicked_to_play',
     'post_video_views_10s_organic',
     'post_video_views_10s_paid',
     'post_video_views_10s_sound_on',
     'post_video_views_sound_on',
     'post_video_view_time',
     'post_video_view_time_organic',
     'post_video_view_time_by_age_bucket_and_gender',
     'post_video_view_time_by_region_id',
     'post_video_views_by_distribution_type',
     'post_video_view_time_by_distribution_type',
     'post_video_view_time_by_country_id',),
]


def fetch_all_page_likers(page, limit, params, client):
    result = []
    for pl in client.fetch_page_likers(page_id=page['id'], limit=limit):
        result.extend(pl)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--page_id', required=False)
    parser.add_argument('-q', '--query', required=False)
    parser.add_argument('-t', '--access_token', required=False)
    parser.add_argument('-i', '--app_id', required=False)
    parser.add_argument('-s', '--app_secret', required=False)
    parser.add_argument(
        '-l', '--limit', required=False, type=int, default=100)
    parser.add_argument('-o', '--output', required=False)
    args = parser.parse_args()

    client = FBClient(
        access_token=args.access_token,
        app_id=args.app_id,
        app_secret=args.app_secret)

    if args.page_id:
        page = {'id': args.page_id}
    else:
        pages = client.search_pages(args.query, limit=1)
        if pages:
            page = pages[0]
        else:
            return 'No page found for query {}'.format(args.query)

    field_names = []
    exclude = (
        'access_token', 'app_id', 'ad_campaign', 'app_links', 'business',
        'description_html', 'instant_articles_review_status',
        'leadgen_form_preview_details', 'merchant_id', 'preferred_audience',
        'promotion_eligible', 'recipient', 'supports_instant_articles',
        'wifi_information')
    for f in client.fetch_page_metadata(page['id'])['fields']:
        if f['name'] not in exclude:
            field_names.append(f['name'])
    fields = ','.join(field_names)

    result = []
    result.append({'page': page})

    likers = []
    likers_of_likers = []
    for pl in client.fetch_page_likers(
        page['id'],
        params={'fields': fields},
        limit=args.limit):

        likers.extend(pl)

    pool = Pool(4)
    worker = partial(
        fetch_all_page_likers,
        client=client,
        params={'fields': fields},
        limit=args.limit)
    for pl in pool.map(worker, likers):
        likers_of_likers.extend(pl)

    top_interests = []
    for cat, n in aggregate_categories(likers + likers_of_likers)[:10]:
        for c in cat.split('/'):
            interests = client.search_interests(q=c)
            if interests:
                top_interests.append(interests[0])

    result.extend(
        [{'likers': aggregate_pages(likers)},
         {'likers_of_likers': aggregate_pages(likers_of_likers)},
         {'top_interests': top_interests}])

    posts = []
    for pp in client.fetch_page_posts(page['id'], limit=args.limit):
        posts.extend(pp)

    result.append({'posts': posts})

    metrics = []
    for m in METRIC_NAMES:
        gen = client.fetch_page_insights(
            page_id=page['id'],
            metrics=m,
            params={'period': 'week'})

        for data in gen:
            metrics.extend(data)
    metrics.sort(key=lambda m: m['name'])

    result.append({'metrics': metrics})
    dumped = json.dumps({'result': result}, indent=4)

    if args.output:
        with open(args.output, 'wb') as f:
            f.write(dumped)
    else:
        sys.stdout.write(dumped)


def aggregate_pages(pages):
    data = {}
    for p in pages:
        val = data.get(p['id'])
        if not val:
            p['counter'] = 1
            data[p['id']] = p
        else:
            val['counter'] += 1
    return sorted(data.values(), key=lambda v: v['counter'], reverse=True)


if __name__ == '__main__':
    main()
