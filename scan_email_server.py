from flask import Flask
from flask import jsonify, request, session, redirect
# from flask_cors import CORS

import dotenv
import os
from google_auth_oauthlib.flow import Flow
from google.oauth2 import id_token
from google.auth.transport import requests as google_auth_requests
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials

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
REDIRECT_URI = os.getenv('REDIRECT_URI')

app = Flask(__name__)
#setting app secret key
app.secret_key = FLASK_KEY

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
    flow.redirect_uri = REDIRECT_URI

    #pulling authorization url (google login)
    authorization_url, _state = (
        flow.authorization_url(
            access_type="offline"
            ,prompt="select_account"
            ,include_granted_scopes="true"
        )
    )

    # Redirect to the authorization URL
    return redirect(authorization_url)

@app.route("/google_login/oauth2callback")
def auth_login_google_oauth2callback():
    state = request.args.get('state')
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
    
    # Store the credentials in the session
    session['credentials'] = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes
    }
    
    #verify token, while also retrieving information about the user
    id_info = id_token.verify_oauth2_token(
        id_token=credentials._id_token
        ,request=google_auth_requests.Request()
        ,audience=CLIENT_ID
    )
    
    # Set the user information to an element of the session.
    # TODO: Do something else with this (login, store in JWT, etc)
    session["id_info"] = id_info
    session["oauth_state"] = state
    
    #redirecting to the final redirect (i.e., logged in page)
    redirect_response = redirect("http://localhost:8080/logged_in")

    return redirect_response

@app.route("/logged_in")
def logged_in():
    print(f"logged_in received.")

    # Retrieve credentials from session
    if 'credentials' not in session:
        return redirect('/google_login')
        
    # Rebuild credentials object
    credentials = Credentials(
        token=session['credentials']['token'],
        refresh_token=session['credentials']['refresh_token'],
        token_uri=session['credentials']['token_uri'],
        client_id=session['credentials']['client_id'],
        client_secret=session['credentials']['client_secret'],
        scopes=session['credentials']['scopes']
    )
    
    # Build the Gmail service
    gmail_service = build('gmail', 'v1', credentials=credentials)

    messages = search_emails(gmail_service, "reservation")
    print(f"First messages returned: {messages[0]}")
    email_count = len(messages)
    
    # Retrive User data:
    name = session["id_info"]["name"]
    picture = session["id_info"]["picture"]
    email = session["id_info"]["email"]

    #render the email/picture
    return f"""
    <h1>Hi {name}! You're logged in!</h1>
    <img src="{picture}" />
    <p>Email: {email}</p>
    <p>Number of reservation emails found in Gmail (max 500): {email_count if 'email_count' in locals() else 'Unknown'}</p>
    """, 200

def search_emails(service, query, max_results=500):
    """Search for emails matching the query.
    
    Args:
        service: Authenticated Gmail API service instance.
        query: String used to filter messages matching specific criteria.
        max_results: Maximum number of results to return (default 500)
        
    Returns:
        List of messages that match the criteria
    """
    try:
        # Initialize empty list for messages and nextPageToken
        messages = []
        next_page_token = None
        
        # Keep fetching pages until all results are retrieved or max_results is reached
        while True:
            # Request a page of results
            result = service.users().messages().list(
                userId='me',
                q=query,
                pageToken=next_page_token,
                maxResults=min(max_results - len(messages), 100)  # Gmail API allows max 100 per request
            ).execute()
            
            # Get messages from this page
            page_messages = result.get('messages', [])
            if not page_messages:
                break
                
            # Add messages to our list
            messages.extend(page_messages)
            print(f"Retrieved {len(messages)} emails so far...")
            
            # Check if we've reached the desired number of results
            if len(messages) >= max_results:
                print(f"Reached maximum of {max_results} results")
                break
                
            # Get token for next page or exit if no more pages
            next_page_token = result.get('nextPageToken')
            if not next_page_token:
                break
        
        return messages
        
    except Exception as error:
        print(f"An error occurred: {error}")
        return []

if __name__ == '__main__':    
    app.run(port=8080)
