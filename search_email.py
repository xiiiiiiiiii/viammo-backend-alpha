import os
import pickle
import base64
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Gmail API configuration
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CLIENT_ID = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_ID')
CLIENT_SECRET = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_SECRET')
TOKEN_FILE = 'token.pickle'

def get_gmail_service():
    """Get authenticated Gmail service."""
    creds = None
    
    # Check if token file exists
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
    
    # If credentials don't exist or are invalid, get new ones
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            # Create flow instance with client ID and secret
            flow = InstalledAppFlow.from_client_config(
                {
                    "installed": {
                        "client_id": CLIENT_ID,
                        "client_secret": CLIENT_SECRET,
                        "redirect_uris": ["http://localhost", "urn:ietf:wg:oauth:2.0:oob"],
                        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                        "token_uri": "https://oauth2.googleapis.com/token"
                    }
                },
                SCOPES
            )
            # Open browser for user authentication
            creds = flow.run_local_server(port=0)
            
        # Save credentials for next run
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
    
    # Build and return Gmail service
    return build('gmail', 'v1', credentials=creds)

def search_emails(service, query):
    """Search for emails matching the query."""
    result = service.users().messages().list(userId='me', q=query).execute()
    messages = result.get('messages', [])
    
    return messages

def get_email_details(service, msg_id):
    """Get email details for a message ID."""
    message = service.users().messages().get(userId='me', id=msg_id).execute()
    
    headers = message['payload']['headers']
    subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
    sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
    date = next((h['value'] for h in headers if h['name'] == 'Date'), 'Unknown Date')
    
    # Try to get plain text content
    plain_text = ""
    if 'parts' in message['payload']:
        for part in message['payload']['parts']:
            if part['mimeType'] == 'text/plain':
                plain_text = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                break
    
    return {
        'id': msg_id,
        'subject': subject,
        'sender': sender,
        'date': date,
        'snippet': message['snippet'],
        'body': plain_text[:500] + '...' if len(plain_text) > 500 else plain_text
    }

def main():
    print("Authenticating with Gmail...")
    service = get_gmail_service()
    
    print("Searching for stay confirmation emails...")
    query = '"confirmed stay" OR "stay confirmation"'
    messages = search_emails(service, query)
    
    if not messages:
        print("No matching emails found.")
        return
    
    print(f"Found {len(messages)} matching emails:")
    print("-" * 80)
    
    for i, message in enumerate(messages[:10], 1):  # Limit to first 10 results
        email = get_email_details(service, message['id'])
        print(f"{i}. Subject: {email['subject']}")
        print(f"   From: {email['sender']}")
        print(f"   Date: {email['date']}")
        print(f"   Snippet: {email['snippet']}")
        print("-" * 80)
    
    if len(messages) > 10:
        print(f"...and {len(messages) - 10} more results")

if __name__ == "__main__":
    main()
