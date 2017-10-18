from urlparse import urlsplit, parse_qsl

import requests
from flask import Flask, session, request, redirect, jsonify, url_for, render_template

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


@app.route("/adcampaigns")
def adcampaigns():
    client = FBClient(access_token=session['access_token'])
    adacc_id = next(client.fetch_user_adaccounts())[0]['id']
    adcampaigns = []
    for camps in client.fetch_adcampaigns(adacc_id):
        adcampaigns.extend(camps)
    return render_template(
        'adcampaigns.html',
        data={'adcampaigns': adcampaigns})


app.secret_key = 'A0Zr98j/3yX R~XHH!jmN]LWX/,?RT'


if __name__ == '__main__':
    app.run(debug=True)
