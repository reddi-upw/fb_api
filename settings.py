# CLIENT_ID = ""

# CLIENT_SECRET = ""

REDIRECT_URL = "http://localhost:5000/fb-token"

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


from local_settings import *
