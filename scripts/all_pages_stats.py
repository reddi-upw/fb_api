import sys
import argparse
import json

from string import lowercase

from client import FBClient


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


def fetch_page(client, page_id, args):
    field_names = []
    exclude = (
        'access_token', 'app_id', 'ad_campaign', 'app_links', 'business',
        'description_html', 'instant_articles_review_status',
        'leadgen_form_preview_details', 'merchant_id', 'preferred_audience',
        'promotion_eligible', 'recipient', 'supports_instant_articles',
        'wifi_information')
    for f in client.fetch_metadata(page_id)['fields']:
        if f['name'] not in exclude:
            field_names.append(f['name'])

    metrics = []
    for names in METRIC_NAMES:
        metrics.extend(names)
    m = ','.join(metrics)

    field_names.extend(
        ['feed.limit(100){likes,created_time,message,name,comments{from}}',
         'insights.metric({}).period(week).limit(100)'.format(m)])
    fields = ','.join(field_names)

    page = client.fetch_page(page_id=page_id, params={'fields': fields})
    insights = page.get('insights', {})
    insights.pop('paging', None)

    feed = page.get('feed', {})
    feed.pop('paging', None)
    return page


def main():
    parser = argparse.ArgumentParser()
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

    result = []
    for q in lowercase:
        for chunk in client.search_pages(q, limit=args.limit):
            page_ids = [p['id'] for p in chunk]
            for page_id in page_ids:
                page = fetch_page(client, page_id, args)
                result.append(page)
                dumped_page = json.dumps({'result': result}, indent=4)
                sys.stdout.write(dumped_page)

    dumped = json.dumps({'result': result}, indent=4)

    if args.output:
        with open(args.output, 'wb') as f:
            f.write(dumped)


if __name__ == '__main__':
    main()
