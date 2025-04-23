import os
import pickle
import base64
import json
import re
from html import unescape
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from dotenv import load_dotenv

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

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
    
    # Extract email body using a recursive approach to handle different email structures
    plain_text = ""
    
    def get_text_from_part(part):
        """Recursively extract text from email parts."""
        if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
            return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
        
        # If the part has a body with data, but is HTML, extract plain text from HTML
        html_content = ""
        if part.get('mimeType') == 'text/html' and 'data' in part.get('body', {}):
            html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
            # Extract text from HTML using regex to remove tags
            html_text = extract_text_from_html(html)
            html_content = html_text
        
        # Check for nested parts
        if 'parts' in part:
            for subpart in part['parts']:
                text = get_text_from_part(subpart)
                if text:
                    return text
        
        return html_content
    
    def extract_text_from_html(html):
        """Extract plain text from HTML content."""
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html)
        # Decode HTML entities
        text = unescape(text)
        # Replace multiple whitespace with single space
        text = re.sub(r'\s+', ' ', text)
        # Remove leading/trailing whitespace
        text = text.strip()
        return text
    
    # Handle case where content is directly in the payload
    if 'data' in message['payload'].get('body', {}):
        content = base64.urlsafe_b64decode(message['payload']['body']['data']).decode('utf-8')
        # Check if content is HTML
        if message['payload'].get('mimeType') == 'text/html':
            plain_text = extract_text_from_html(content)
        else:
            plain_text = content
    # Handle case with parts
    elif 'parts' in message['payload']:
        plain_text = get_text_from_part(message['payload'])
    
    # If still empty, use the snippet as a fallback
    if not plain_text:
        plain_text = f"[Could not extract body. Snippet: {message['snippet']}]"
    
    return {
        'id': msg_id,
        'subject': subject,
        'sender': sender,
        'date': date,
        'snippet': message['snippet'],
        'body': plain_text
    }

def get_all_email_datas(service, messages, max_words_total_limit=523788, email_count_limit=None):
    messages = messages[:email_count_limit] if email_count_limit else messages
    email_datas = []
    total_word_count = 0
    for i, message in enumerate(messages):
        email_data = get_email_details(service, message['id'])
        
        # Count words in this email (subject, snippet, and body)
        approx_word_count = int(len(str(email_data)) / 7.0)
        
        # Check if adding this email would exceed the word limit
        if total_word_count + approx_word_count > max_words_total_limit:
            print(f"Reached word limit after processing {i} of {len(messages)} emails.")
            print(f"Total word count: {total_word_count}/{max_words_total_limit}")
            break
        
        # Add email data and update word count
        email_datas.append(email_data)
        total_word_count += approx_word_count
        
        # Print progress every 5 emails
        if i % 5 == 0:
            print(f"Processed {i+1}/{len(messages)} emails. Word count: {total_word_count}/{max_words_total_limit}")
    
    print(f"Final email count: {len(email_datas)}/{len(messages)}")
    print(f"Final word count: {total_word_count}/{max_words_total_limit}")
    
    return email_datas

def generate_trips_metadatas(trip_message_datas, num_trips, openai_api_key) -> str:
    """
    Returns a list of trip information JSON objects.
    """

    if not openai_api_key:
        print("Warning: OPENAI_API_KEY environment variable not set. Skipping LLM keyword extraction.")
        return None
    
    try:        
        llm_model = "o4-mini"
        
        # Initialize the LLM with the API key explicitly
        llm = ChatOpenAI(model=llm_model, openai_api_key=openai_api_key)
        
        # Define a prompt template for hotel characteristics
        template = """
        Based on the following travel labeled email messages, please analyze the typical patterns of the user's travel preferences
        and a list of trip json objects with up to {num_trips} trip objects like the one below corresponding to the user's travel preferences.
        Please only return valid JSON and nothing else - no explanations or text before or after the JSON. Please only use the json fields
        that are present in the example trip json objects below - don't add extra json fields, add extra info in notes field for example.

        Make sure to find and account for the following information in the trip json objects:
        - preferred destinations
        - preferred travel dates for preferred destinations
        - number of guests and type of guests for those preferred destinations and dates, try using age of guests to determine if they are adults or children.
        - purpose of the trip, e.g. "Family vacation", "Business trip", "Solo travel", "Couple's getaway", etc. Try using past room types for preferred destinations to determine purpose, e.g. 1 room with 2 queen beds probably isn't a couple's getaway purpose trip.
        - total budget with $ signs, e.g. "$$$$",  "$$$", "$$", "$", etc. with "$$$$" being the highest budget.
        - Preferred hotel characteristics to add to notes field, e.g. "Family friendly", "Ski-in-ski-out", "Beachfront", "Business class", etc.
        - Preferred hotel chains to add to notes field, e.g. "Hilton", "Marriott", "Hyatt", "St. Regis", "Rosewood", "Relais & Chateaux", "Four Seasons", "Leading Hotels of the World", etc.
        - Preferred room types to add to notes field, e.g. "1 King bed Suite", "1 room with King bed and 1 room with 2 queens", "2 Queen beds", "Crib", "Pool view", "Garden view", "Ocean view", "Mountain view", etc.
        - Preferred amenities to add to notes field, e.g. "Free Wi-Fi", "Free breakfast", "Free airport shuttle", "Free parking", "Free Wi-Fi", "Free breakfast", "Free airport shuttle", "Free parking", etc.
        - Preferred hotel features to add to notes field, e.g. "Spa", "Gym", "Pool", "Beachfront", "Ski-in-ski-out", "Walkable", "Ocean view", "Mountain view", "Garden view", etc.
        - Preferred activities to add to notes field, e.g. "Hiking", "Skiing", "Cross Country Skiing", "Backcountry Skiing & Snowboarding", "Surfing", "Golfing", "Scuba diving", "Snorkeling", "Water sports", "Sailing", "Fishing", "etc."
        - Preferred dining experiences to add to notes field, e.g. "Fine dining", "Casual dining", "Fast food", "Cafe", "Bar", "Pub", "Italian", "Japanese", "Mexican", "American", "French", "Spanish", "etc."
        - Preferred children activities to add to notes field, e.g. "Kids club", "Kids activities", "Kids pool", "Kids spa", "Kids gym", "Kids beach", "Kids mountain", "Kids garden", "etc."
        - any other information that would be helpful for a travel planner to know.

        Example returned list of with 1 trip object (up to {num_trips} great):
        [
            {{
                "name": "Tahoe Family",
                "startDate": "2025-04-18T07:00:00.000Z",
                "endDate": "2025-04-21T07:00:00.000Z",
                "destination": {{
                    "city": "Palisades Tahoe",
                    "state": "CA",
                    "country": "USA"
                }},
                "numberOfGuests": {{
                    "$numberInt": "4"
                }},
                "notes": "Ski-in-ski-out, family friendly, 1 room with 2 adults with one king bed, 1 room with 2 kids and 2 queen beds",
                "totalBudget": "$$$$",
                "purpose": "Family vacation"
            }}
        ]

        Trip message datas:
        {trip_message_datas}
        """
        
        prompt = ChatPromptTemplate.from_template(template)
        
        # Generate the response
        chain = prompt | llm
        response = chain.invoke({
            "trip_message_datas": trip_message_datas,
            "num_trips": num_trips
        })

        # Extract JSON from the response
        response_content = response.content
        if not response_content:
            print(f"LLM did not return a response to generate trip metadata")
            return None
        
        # Try to parse the response as JSON
        try:
            # Parse the JSON
            trip_jsons = json.loads(response_content)
            return trip_jsons
        except json.JSONDecodeError as e:
            print(f"Error parsing JSON response: {e}")
            print(f"Raw response: {response_content}")
            return None
            
    except ImportError:
        print("Warning: LangChain or OpenAI packages not installed. Skipping keyword generation.")
        print("To install required packages: pip install langchain langchain-openai")
        return None

def main():
    DISPLAY_LIMIT = 30
    NUM_TRIPS_METADATA_TO_GENERATE = 3
    MAX_IN_CONTEXT_WORDS = 64000

    print("Authenticating with Gmail...")
    service = get_gmail_service()
    
    print("Searching for emails with 'travel' label...")
    query = "label:travel"
    messages = search_emails(service, query)
    
    if not messages:
        print("No matching emails found.")
        return
    
    print(f"Found {len(messages)} matching emails.")

    print(f"Getting email data...")
    email_datas = get_all_email_datas(service, messages, max_words_total_limit=MAX_IN_CONTEXT_WORDS, email_count_limit=None)
    print(f"Fetched {len(email_datas)} email datas (capped at so total context below {MAX_IN_CONTEXT_WORDS} words).")

    print("-" * 80)
    for email_data in email_datas[:DISPLAY_LIMIT]:
        print(f"{email_data['subject']}")
        print(f"   From: {email_data['sender']}")
        print(f"   Date: {email_data['date']}")
        print(f"   Snippet: {email_data['snippet']}")
        print("-" * 80)
        print()

    print(f"Generating up to {NUM_TRIPS_METADATA_TO_GENERATE} trip metadatas...")
    trip_jsons = generate_trips_metadatas(email_datas, NUM_TRIPS_METADATA_TO_GENERATE, os.getenv("OPENAI_API_KEY"))
    
    # Pretty print the trip JSON data
    if trip_jsons:
        print("\n=== Generated Trip Metadata ===\n")
        print(json.dumps(trip_jsons, indent=4))
        print("\n=============================\n")
    
    if len(messages) > DISPLAY_LIMIT:
        print(f"...and {len(messages) - DISPLAY_LIMIT} more results")

if __name__ == "__main__":
    main()
