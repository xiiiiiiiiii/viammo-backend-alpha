from flask import Flask
from flask import request, session, redirect
from flask_cors import CORS

import dotenv
import os
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests as google_auth_requests

from dotenv import load_dotenv


# Load environment variables
load_dotenv()

# Gmail API configuration
SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/userinfo.email",
    "https://www.googleapis.com/auth/userinfo.profile",
    "openid"
]
CLIENT_ID = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_ID')
CLIENT_SECRET = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_SECRET')
FLASK_KEY = os.getenv('FLASK_KEY')

# Print environment and configuration
print(f"CLIENT_ID: {CLIENT_ID}")
print(f"FLASK_KEY: {FLASK_KEY}")

app = Flask(__name__)
#setting app secret key
app.secret_key = FLASK_KEY
# app.config['SESSION_COOKIE_SECURE'] = False  # For HTTPS
# app.config['SESSION_COOKIE_HTTPONLY'] = True
# app.config['SESSION_COOKIE_SAMESITE'] = 'Lax'

# # Enable CORS for all routes
# CORS(app)

# Setting OAUTHLIB insecure transport to 1 (needed for development with self-signed certificates)
os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'



@app.route("/")
def app_root():
    return """  
        <button onclick="window.location.href='/google_login'">
            Login with Google
        </button>
    """, 200

@app.route("/google_login")
def google_login():
    print("Starting Google login flow")
    #creates google login flow object
    flow = Flow.from_client_config(
        client_config={
            "web":
            {
                "client_id":CLIENT_ID
                ,"client_secret":CLIENT_SECRET
                ,"auth_uri":"https://accounts.google.com/o/oauth2/v2/auth"
                ,"token_uri":"https://oauth2.googleapis.com/token"
            }
        }
        #if you need additional scopes, add them here
        ,scopes=SCOPES
    )      

    #redirect uri for the google callback (i.e., the route in our api that handles everything AFTER google auth)
    flow.redirect_uri = "http://localhost:10000/google_login/oauth2callback"

    #pulling authorization url (google login), and state to store in Flask session
    authorization_url, state = (
        flow.authorization_url(
            access_type="offline"
            ,prompt="select_account"
            ,include_granted_scopes="true"
        )
    )

    #connecting/storing state and final redirect AFTER login in the Flask API
    session['state'] = state
    print(f"Session state set to: {state}")
    print(f"Session contents: {session}")

    #redirecting to the authorization URL
    return redirect(authorization_url)

@app.route("/google_login/oauth2callback")
def auth_login_google_oauth2callback():
    print(f"Callback received. Session contents: {session}")
    print(f"URL parameters: {request.args}")
    
    # Get state from URL first, then try session as backup
    url_state = request.args.get('state')
    session_state = session.get('state')
    
    print(f"URL state: {url_state}, Session state: {session_state}")
    
    # Use URL state if available, otherwise try session
    if url_state:
        state = url_state
        print("Using state from URL")
    elif session_state:
        state = session_state
        print("Using state from session")
    else:
        print("No state found in URL or session")
        return "Session state lost. Please try logging in again.", 400
    
    redirect_uri = request.base_url
    #pull the authorization response
    authorization_response = request.url
    
    #create our flow object similar to our initial login with the added "state" information
    flow = Flow.from_client_config(
        client_config={
            "web":
            {
                "client_id":CLIENT_ID
                ,"client_secret":CLIENT_SECRET
                ,"auth_uri":"https://accounts.google.com/o/oauth2/v2/auth"
                ,"token_uri":"https://oauth2.googleapis.com/token"
            }
        }
        ,scopes=SCOPES
        ,state=state    
    )

    flow.redirect_uri = redirect_uri  
    #fetch token
    flow.fetch_token(authorization_response=authorization_response)
    #get credentials
    credentials = flow.credentials
    #verify token, while also retrieving information about the user
    id_info = id_token.verify_oauth2_token(
        id_token=credentials._id_token
        ,request=google_auth_requests.Request()
        ,audience=CLIENT_ID
    )    
    #setting the user information to an element of the session
    #you'll generally want to do something else with this (login, store in JWT, etc)
    session["id_info"] = id_info

    #redirecting to the final redirect (i.e., logged in page)
    redirect_response = redirect("http://localhost:10000/logged_in")

    return redirect_response

@app.route("/logged_in")
def logged_in():
    #retrieve the users picture
    picture = session["id_info"]["picture"]
    #retrieve the users email
    email = session["id_info"]["email"]

    #render the email/picture
    return f"""
    <h1>Logged In</h1>
    <p>Email: {email}</p>
    <img src="{picture}" />
    """, 200

if __name__ == '__main__':    
    app.run(
        port=10000,
        # debug=True,
    )
