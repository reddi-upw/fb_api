from urlparse import urlsplit, parse_qsl

import requests
from flask import Flask, session, request, redirect, jsonify, url_for

from page_stats.page_stats import FBClient


CLIENT_ID = ""
REDIRECT_URL = "http://localhost:5000/fb-token"
CLIENT_SECRET = ""


FB_DIALOG_URL = (
    "https://www.facebook.com/v2.10/dialog/oauth?"
    "client_id={client_id}&"
    "redirect_uri={redirect_uri}&"
    "scope={scope}")


FB_TOKEN_URL = (
    "https://graph.facebook.com/v2.10/oauth/access_token?"
    "client_id={client_id}&"
    "redirect_uri={redirect_uri}&"
    "client_secret={client_secret}&"
    "code={code}")


FB_SCOPE = [
    'ads_management', 'ads_read',]


app = Flask(__name__)


def flatten(g):
    for values in g:
        for value in values:
            yield value


@app.route("/")
def fb_dialog():
    return redirect(
        FB_DIALOG_URL.format(
            client_id=CLIENT_ID,
            redirect_uri=REDIRECT_URL,
            scope=','.join(FB_SCOPE)),
        code=302)


@app.route("/fb-token")
def fb_token():
    _, _, _, q, _ = urlsplit(request.url)
    q = dict(parse_qsl(q))
    code = q.get('code')

    url = FB_TOKEN_URL.format(
        client_id=CLIENT_ID,
        redirect_uri=REDIRECT_URL,
        client_secret=CLIENT_SECRET,
        code=code)

    r = requests.get(url)
    session['access_token'] = r.json()['access_token']
    print('ACCESS TOKEN', r.json()['access_token'])
    return redirect(url_for("adcampaigns"))


ADCAMPAIGN_FIELDS = (
    'id', 'account_id', 'adlabels', 'boosted_object_id',
    'budget_rebalance_flag', 'buying_type',
    'can_use_spend_cap', 'configured_status', 'created_time',
    'effective_status', 'name', 'objective', 'recommendations',
    'spend_cap', 'start_time', 'status', 'stop_time', 'updated_time')


ADSET_FIELDS = (
    'id', 'account_id', 'adlabels', 'adset_schedule',
    'bid_amount', 'bid_info', 'billing_event', 'budget_remaining',
    'campaign', 'campaign_id', 'configured_status', 'created_time',
    'creative_sequence', 'daily_budget', 'effective_status',
    'end_time', 'frequency_cap', 'frequency_cap_reset_period',
    'frequency_control_specs',
    'instagram_actor_id', 'is_autobid', 'is_average_price_pacing',
    'lifetime_budget', 'lifetime_frequency_cap', 'lifetime_imps',
    'name', 'optimization_goal', 'pacing_type', 'promoted_object',
    'recommendations', 'recurring_budget_semantics', 'rf_prediction_id',
    'rtb_flag', 'start_time', 'status', 'targeting',
    'time_based_ad_rotation_id_blocks', 'time_based_ad_rotation_intervals',
    'updated_time', 'use_new_app_click')


AUDIENCE_FIELDS = (
    'id', 'account_id', 'approximate_count', 'data_source',
    'delivery_status', 'description', 'external_event_source',
    'is_value_based', 'lookalike_audience_ids', 'lookalike_spec',
    'name', 'operation_status', 'opt_out_link', 'permission_for_actions',
    'pixel_id', 'retention_days', 'rule', 'rule_aggregation',
    'subtype', 'time_content_updated', 'time_created', 'time_updated')


@app.route("/adcampaigns")
def adcampaigns():
    client = FBClient(access_token=session['access_token'])
    adaccounts = []
    camp_fields = ','.join(ADCAMPAIGN_FIELDS)
    adset_fields = ','.join(ADSET_FIELDS)
    audience_fields = ','.join(AUDIENCE_FIELDS)
    for adacc in flatten(client.fetch_user_adaccounts()):
        campaigns = []
        for camps in client.fetch_adcampaigns(
            adacc['id'],
            params={'fields': camp_fields}):
            campaigns.extend(camps)
        for camp in campaigns:
            camp_adsets = []
            for adsets in client.fetch_adsets(
                camp['id'],
                params={'fields': adset_fields}):
                camp_adsets.extend(adsets)
                camp['adsets'] = camp_adsets
        adacc['adcampaigns'] = campaigns

        audiences = []
        for aud in client.fetch_custom_audiences(
            adacc_id=adacc['id'],
            params={'fields': audience_fields}):
            audiences.extend(aud)
        adacc['audiences'] = audiences

        adaccounts.append(adacc)
    return jsonify({'adaccounts': adaccounts})


app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


if __name__ == '__main__':
    app.run(debug=True)
