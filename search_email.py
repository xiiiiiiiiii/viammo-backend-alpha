import os
import pickle
import base64
import json
import re
import datetime
from html import unescape
from dotenv import load_dotenv
from threading import Lock
import concurrent.futures

import time
import tempfile
from typing import List, Dict, Any, Optional

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import HttpError

from langchain_openai import ChatOpenAI
from langchain_core.prompts import ChatPromptTemplate

from groq import Groq

# Load environment variables
load_dotenv()

# Gmail API configuration
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
CLIENT_ID = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_ID')
CLIENT_SECRET = os.getenv('GOOGLE_CLOUD_GMAIL_CLIENT_SECRET')
TOKEN_FILE = 'token.pickle'
MAX_CONCURRENCY = 10

def load_jsonl(file_path):
    with open(file_path, 'r') as f:
        return [json.loads(line) for line in f]

def save_to_jsonl(file_path, a_list):
    # Create directory if it doesn't exist
    dirname = os.path.dirname(file_path)
    if len(dirname.strip()) > 0:
        os.makedirs(dirname, exist_ok=True)

    # Save to JSONL file
    with open(file_path, 'w') as f:
        for item in a_list:
            f.write(json.dumps(item) + '\n')

    print(f"Saved {len(a_list)} records to {file_path}")

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

def get_email_metadatas_batch(msg_ids):
    """Get email metadata for multiple message IDs in a batch request."""
    results = []
    results_lock = Lock()
    
    def fetch_single_message(msg_id, idx):
        """Process a single message and return its metadata."""
        try:
            service = get_gmail_service()

            response = service.users().messages().get(
                userId='me',
                id=msg_id,
                format='metadata',
                metadataHeaders=['Subject', 'From', 'To', 'Date', 'Reply-To', 'CC', 'BCC', 'In-Reply-To']
            ).execute()
        
            # Process the response the same way as the individual method
            headers = response['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), 'Unknown Date')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
            recipient = next((h['value'] for h in headers if h['name'] == 'To'), 'Unknown Recipient')
            reply_to = next((h['value'] for h in headers if h['name'] == 'Reply-To'), 'Unknown Reply-To')
            cc = next((h['value'] for h in headers if h['name'] == 'CC'), 'Unknown CC')
            bcc = next((h['value'] for h in headers if h['name'] == 'BCC'), 'Unknown BCC')
            in_reply_to = next((h['value'] for h in headers if h['name'] == 'In-Reply-To'), 'Unknown In-Reply-To')
            
            email_metadata = {
                'id': msg_id,
                'subject': subject,
                'date': date,
                'sender': sender,
                'recipient': recipient,
                'reply_to': reply_to,
                'cc': cc,
                'bcc': bcc,
                'in_reply_to': in_reply_to,
            }

            with results_lock:
                results.append(email_metadata)
            
            if (idx + 1) % 10 == 0:
                print(f"Fetched {idx+1} email metadatas...")
            
            return email_metadata
        
        except HttpError as error:
            print(f"Error fetching message {msg_id}: {error}")
            return None
    
    # results = [fetch_single_message(msg_id, idx) for idx, msg_id in enumerate(msg_ids)]

    # Create a thread pool with limited concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        # Submit all tasks to the executor
        futures = {executor.submit(fetch_single_message, msg_id, idx): msg_id for idx, msg_id in enumerate(msg_ids)}
        
        # Process results as they complete (optional)
        for future in concurrent.futures.as_completed(futures):
            msg_id = futures[future]
            try:
                # This will re-raise any exceptions from the task
                future.result()
            except Exception as exc:
                print(f"Message {msg_id} generated an exception: {exc}")
    
    return results

def get_full_email_batch(msg_ids):
    """Get full email for multiple message IDs in a batch request."""
    results = []
    results_lock = Lock()
    
    def fetch_single_full_message(msg_id, idx):
        """Process a single message and return its metadata."""
        try:
            service = get_gmail_service()

            response = service.users().messages().get(
                userId='me',
                id=msg_id,
                format='full'
            ).execute()
        
            # Process the response the same way as the individual method
            headers = response['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            date = next((h['value'] for h in headers if h['name'] == 'Date'), 'Unknown Date')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')
            recipient = next((h['value'] for h in headers if h['name'] == 'To'), 'Unknown Recipient')
            reply_to = next((h['value'] for h in headers if h['name'] == 'Reply-To'), 'Unknown Reply-To')
            cc = next((h['value'] for h in headers if h['name'] == 'CC'), 'Unknown CC')
            bcc = next((h['value'] for h in headers if h['name'] == 'BCC'), 'Unknown BCC')
            in_reply_to = next((h['value'] for h in headers if h['name'] == 'In-Reply-To'), 'Unknown In-Reply-To')

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

            def get_text_from_part(part):
                """Recursively extract text from email parts."""
                if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
                    return base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                if part.get('mimeType') == 'text/html' and 'data' in part.get('body', {}):
                    html = base64.urlsafe_b64decode(part['body']['data']).decode('utf-8')
                    return extract_text_from_html(html)
                if 'parts' in part:  # Check for nested parts
                    subpart_texts = [get_text_from_part(subpart) for subpart in part['parts']]
                    subpart_texts = [subpart_text for subpart_text in subpart_texts if subpart_text is not None]
                    return ' '.join(subpart_texts)

            body = get_text_from_part(response['payload'])
            body = body if body else "Unknown body"
            
            email_metadata = {
                'id': msg_id,
                'subject': subject,
                'date': date,
                'sender': sender,
                'recipient': recipient,
                'reply_to': reply_to,
                'cc': cc,
                'bcc': bcc,
                'in_reply_to': in_reply_to,
                'body': body,
            }

            with results_lock:
                results.append(email_metadata)
            
            if (idx + 1) % 10 == 0:
                print(f"Fetched {idx+1} email metadatas...")
            
            return email_metadata
        
        except HttpError as error:
            print(f"Error fetching message {msg_id}: {error}")
            return None
    
    # results = [fetch_single_full_message(msg_id, idx) for idx, msg_id in enumerate(msg_ids[:10])]

    # Create a thread pool with limited concurrency
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_CONCURRENCY) as executor:
        # Submit all tasks to the executor
        futures = {executor.submit(fetch_single_full_message, msg_id, idx): msg_id for idx, msg_id in enumerate(msg_ids)}
        
        # Process results as they complete (optional)
        for future in concurrent.futures.as_completed(futures):
            msg_id = futures[future]
            try:
                # This will re-raise any exceptions from the task
                future.result()
            except Exception as exc:
                print(f"Message {msg_id} generated an exception: {exc}")
    
    return results

def generate_trip_insights(trip_message_datas, openai_api_key, existing_trip_insights = "") -> str:
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
        Based on the following hotel reservation email messages and the existing trip insights, please analyze the typical patterns of
        the user's travel preferences and generate a list of types of trips that the user has taken. For each type of trip, include the
        following key information:
        - destination
        - time of year, e.g. ski week, spring break, summer, end of year holidays, Thanksgiving, Memorial Day, Labor Day, etc.
        - length of the trip
        - number of guests and type of guests, e.g. adults, children, infants, etc.
        - number of times the user did a similar trip
        - likely purpose of the trip
        - total budget with $ signs, e.g. "$$$$",  "$$$", "$$", "$", etc. with "$$$$" being the highest budget.
        - preferred hotel, keep specifics e.g. "Hilton Honolulu", "Hyatt Waikiki", "St. Regis San Francisco", etc.
        - preferred hotel chains, keep specifics e.g. "Hilton", "Marriott", "Hyatt", "St. Regis", "Rosewood", "Relais & Chateaux", "Four Seasons", "Leading Hotels of the World", etc.
        - preferred hotel characteristics, keep specifics e.g. "family friendly", "ski-in-ski-out", "beach front", "pool", "gym", "spa", "free Wi-Fi", "free breakfast", "free airport shuttle", "free parking", etc.
        - preferred room types, keep specifics e.g. "1 King bed Suite", "1 room with King bed and 1 room with 2 queens", "2 Queen beds", "Crib", "Pool view", "Garden view", "Ocean view", "Mountain view", etc.
        - preferred amenities, keep specifics e.g. "ski-in-ski-out", "beach front", "pool", "gym", "spa", "free Wi-Fi", "free breakfast", "free airport shuttle", "free parking", etc.
        - preferred activities, keep specifics e.g. "skiing", "snowboarding", "hiking", "surfing", "golfing", "scuba diving", "snorkeling", "water sports", "etc."
        - preferred dining experiences, keep specifics e.g. "fine dining", "casual dining", "cafe", "pub", "italian", "japanese", "mexican", "etc."
        - preferred payment method, keep specifics e.g. "credit card", "debit card", "hyatt points", "marriott points", "etc."
        - key details from each trip in this trip type
        - any other information that would be helpful for a travel planner to know.

        Try to generate 5-10 trip types with at least 3 trips per trip type unless you don't have enough trips. If you don't have enough
        trips, start by creating trip types based off of individual trips.

        If you already have generated some trip insights, please add new trip types or merge existing trip types. When merging trip type
        information, make sure to keep track of the total number of days for all trips in that trip type, and any other salient details.
        Rank your trip types with a higher total number of days and total number of trips higher in your list. Keep the number of trip types
        between below or equal to 10.

        You're output should be a self-sufficient list of trip types and their key information (not just an addition to an existing list
        of trip insights).

        Return just list of the types of trips and their key information (as highlighted above).

        Here is the existing trip insights you have already started to generate:
        {existing_trip_insights}

        Here are the new hotel reservation emails you need to analyze:
        {trip_message_datas}
        """
        
        prompt = ChatPromptTemplate.from_template(template)
        
        # Generate the response
        chain = prompt | llm
        response = chain.invoke({
            "existing_trip_insights": existing_trip_insights,
            "trip_message_datas": trip_message_datas
        })

        # Extract JSON from the response
        response_content = response.content
        if not response_content:
            print(f"LLM did not return a response to generate trip insights")
            return None
        
        return response_content
            
    except ImportError:
        print("Warning: LangChain or OpenAI packages not installed. Skipping keyword generation.")
        print("To install required packages: pip install langchain langchain-openai")
        return None

def generate_trips_metadatas(trip_message_datas, trip_insights, num_trips, openai_api_key) -> str:
    """
    Returns a list of trip information JSON objects.
    """

    if not openai_api_key:
        print("Warning: OPENAI_API_KEY environment variable not set. Skipping LLM keyword extraction.")
        return None
    
    try:        
        llm_model = "o4-mini"  # Reasoning capabilities are important for this task (e.g. "2 Queen beds probably isn't a couple's getaway purpose trip.")
        
        # Initialize the LLM with the API key explicitly
        llm = ChatOpenAI(model=llm_model, openai_api_key=openai_api_key)
        
        # Define a prompt template for hotel characteristics
        template = """
        Based on the following hotel reservation email messages and the following trip insights, please analyze the typical patterns of the user's
        travel preferences and generate a list of great future possile trips as a json list of distionaries with up to {num_trips} trip objects like
        the one below corresponding to the user's travel preferences. Please only return valid JSON and nothing else - no explanations or text before
        or after the JSON. Please only use the json fields that are present in the example trip json objects below - don't add extra json fields, add
        extra info in notes field for example. Make sure the dates are in the future and correspond to the preferred destinations.

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
                "startDate": "2026-02-18T07:00:00.000Z",
                "endDate": "2026-02-21T07:00:00.000Z",
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

        Here are the trip insights you have already generated:
        {trip_insights}

        Trip message datas:
        {trip_message_datas}
        """
        
        prompt = ChatPromptTemplate.from_template(template)
        
        # Generate the response
        chain = prompt | llm
        response = chain.invoke({
            "trip_message_datas": trip_message_datas,
            "trip_insights": trip_insights,
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

def run_groq_inference(prompt):
    groq_client = Groq()
    completion = groq_client.chat.completions.create(
        model="meta-llama/llama-4-scout-17b-16e-instruct",
        messages=[
            {
                "role": "user",
                "content": prompt
            },
        ],
        temperature=0.6,
        max_completion_tokens=128,
        top_p=1.0,
        stream=False,
        stop=None,
    )
    return completion.choices[0].message.content

def batch_llm_calls(
    prompts: Dict[str, str],
    model: str = "meta-llama/llama-4-scout-17b-16e-instruct",
    system_message: Optional[str] = None,
    poll_interval: int = 5,
    additional_params: Optional[Dict[str, Any]] = None
) -> List[str]:
    """
    Process a batch of prompts using Groq's Batch API and return results as a list.
    
    Args:
        prompts: List of prompts/questions to send to the LLM
        model: The model to use (default: "meta-llama/llama-4-scout-17b-16e-instruct")
        system_message: Optional system message to prepend to each prompt
        poll_interval: How often to check status (in seconds) when waiting
        additional_params: Optional additional parameters for each request
        
    Returns:
        List of completion strings in the same order as the input prompts
    """
    # Initialize Groq client
    client = Groq()
    
    # Validate inputs
    if not prompts:
        raise ValueError("Must provide a list of prompts to create a new batch job")
    
    # Create temporary JSONL batch file
    with tempfile.NamedTemporaryFile(mode='w+', suffix='.jsonl', delete=False) as temp_file:
        temp_filename = temp_file.name
        
        for prompt_id, prompt in prompts.items():
            # Prepare messages for this prompt
            messages = []
            if system_message:
                messages.append({"role": "system", "content": system_message})
            messages.append({"role": "user", "content": prompt})
            
            # Create request body with model and messages
            body = {
                "model": model,
                "messages": messages
            }
            
            # Add any additional parameters
            if additional_params:
                body.update(additional_params)
            
            # Create the full request with an auto-generated ID
            request = {
                "custom_id": prompt_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": body
            }
            
            # Write the request as a single line in JSONL format
            temp_file.write(json.dumps(request) + "\n")
    
    try:
        # Upload the temporary file
        with open(temp_filename, "rb") as f:
            file_response = client.files.create(
                file=f,
                purpose="batch"
            )
        
        # Create the batch job
        batch_response = client.batches.create(
            completion_window="24h",
            endpoint="/v1/chat/completions",
            input_file_id=file_response.id
        )
        batch_id = batch_response.id
        print(f"Batch job created with ID: {batch_id}")
    finally:
        # Clean up the temporary file
        if os.path.exists(temp_filename):
            os.remove(temp_filename)
    
    # Wait for the job to complete
    print(f"Waiting for batch job {batch_id} to complete...")
    while True:
        response = client.batches.retrieve(batch_id)
        status = response.status
        print(f"Batch job status: {status}.")
        
        if status in ["completed", "expired", "cancelled"]:
            break

        time.sleep(poll_interval)
    
    # Process results
    batch_results = response.to_dict()
    output_file_id = batch_results["output_file_id"]
    actual_responses_obj = client.files.content(output_file_id)
    actual_responses_obj.write_to_file("temp_batch_results.jsonl")
    with open('temp_batch_results.jsonl', 'r', encoding='utf-8') as f:
        raw_results = [json.loads(line) for line in f if line.strip()]
    os.remove("temp_batch_results.jsonl")

    results = {
        result["custom_id"]: result["response"]["body"]["choices"][0]["message"]["content"]
        for result in raw_results
    }

    return results

def main():
    DISPLAY_LIMIT = 20
    NUM_TRIPS_METADATA_TO_GENERATE = 5
    HOTEL_RESERVATION_EMAILS_BATCH_SIZE = 20

    hotel_reservation_search_keywords = load_jsonl('hotel_reservation_search_keywords.jsonl')
    hotel_reservation_search_keywords = [f'"{keyword}"' for keyword in hotel_reservation_search_keywords]
    query = ' OR '.join(hotel_reservation_search_keywords)

    if not os.path.exists('hotel_reservation_emails.jsonl'):
        print("Authenticating with Gmail...")
        service = get_gmail_service()
        
        print("Searching for emails...")
        # query = "label:travel" # Autogenerated google search label, misses a lot of emails...
        query = """
        ("Reservation Confirmation" OR "Booking Confirmation" OR "Booking Reference" OR "Confirmation Number" OR "Reservation Number" OR "Hotel Confirmation") -in:chats
        """
        messages = search_emails(service, query, max_results=5000)
        if not messages:
            print("No matching emails found.")
            return
        print(f"Found {len(messages)} matching emails.")

        print(f"Getting email metadatas...")
        msg_ids = [message['id'] for message in messages]
        email_metadatas = get_email_metadatas_batch(msg_ids)
        print(f"Retrieved {len(email_metadatas)} email metadatas before filtering.")

        email_metadatas = [email_metadata for email_metadata in email_metadatas if "Unknown" in email_metadata['in_reply_to']]
        print(f"Filtered down to {len(email_metadatas)} by removing emails that are replies to another email in the same thread.")

        prompts = {
            email_metadata['id']: f"Here is metadata for an email, is it a hotel reservation confirmation? Just answer True or False and nothing else. Metadata: {email_metadata}"
            for email_metadata in email_metadatas
        }
        batch_hotel_reservation_classification = batch_llm_calls(prompts)
        hotel_reservation_emails = [
            email_metadata
            for email_metadata in email_metadatas
            if "True" == batch_hotel_reservation_classification.get(email_metadata['id'], 'False')
        ]
        save_to_jsonl('hotel_reservation_emails.jsonl', hotel_reservation_emails)
    else:
        hotel_reservation_emails = load_jsonl('hotel_reservation_emails.jsonl')

    if not os.path.exists('full_hotel_reservation_emails.jsonl'):
        msg_ids = [message['id'] for message in hotel_reservation_emails]
        full_hotel_reservation_emails = get_full_email_batch(msg_ids)
        save_to_jsonl('full_hotel_reservation_emails.jsonl', full_hotel_reservation_emails)
    else:
        full_hotel_reservation_emails = load_jsonl('full_hotel_reservation_emails.jsonl')
    
    print(f"Filtered down to {len(full_hotel_reservation_emails)} potential hotel reservation emails based on subject and other metadata.")


    if not os.path.exists('full_hotel_reservation_emails_body_checked.jsonl'):
        prompts = {
            email_metadata['id']: f"Here is data for an email, is it a hotel reservation confirmation? Make sure to only keep hotel reservations (and filter out restaurant reservations and other travel related emails). Just answer True or False and nothing else. Metadata: {email_metadata}"
            for email_metadata in full_hotel_reservation_emails
        }
        batch_hotel_reservation_classification_full_email = batch_llm_calls(prompts)
        body_checked_filtered_hotel_reservation_emails = [
            email_metadata
            for email_metadata in full_hotel_reservation_emails
            if "True" == batch_hotel_reservation_classification_full_email.get(email_metadata['id'], 'False')
        ]
        save_to_jsonl('full_hotel_reservation_emails_body_checked.jsonl', body_checked_filtered_hotel_reservation_emails)
    else:
        body_checked_filtered_hotel_reservation_emails = load_jsonl('full_hotel_reservation_emails_body_checked.jsonl')

    print(f"Filtered down to {len(body_checked_filtered_hotel_reservation_emails)} potential hotel reservation emails based on full email data including body.")

    if not os.path.exists('hotel_reservation_key_insights.jsonl'):
        prompts = {
            email_metadata['id']: f""""
            Here is data for a hotel reservation email. Please extract key insights from the email:
            - hotel name
            - check-in, check-out dates, month of year, season of year, is this a ski-week trip? a spring break trip? a summer trip? etc.
            - location of the hotel, e.g. city, state, country, etc. what type of area is it? a beach, a mountain, a city, a town, etc.
            - number of and age of guests
            - total price, price per night, price per room, price per guest, etc.
            - is the guest a type of loyalty program member of a hotel chain? What membership level?
            - payment method (credit, debit, points, promotion, etc.)
            - type of room or suite, views, great and unusual amenities like beach front, pool, gym, michelin dining, etc. (and obvious ones like free wifi, etc.)
            - special requests made by guests (e.g. roses on arrival, baby crib, etc.)
            - probable purpose of the trip: use the room type and number of guests to infer the purpose of the trip, e.g. business, family, couple, etc. 2 queen beds and 2 adults probably isn't a couple's getaway.
            - any other key insights that would be helpful for a travel planner to know.

            Email data:
            {email_metadata}"
            """
            for email_metadata in body_checked_filtered_hotel_reservation_emails
        }
        batch_hotel_reservation_key_insights = batch_llm_calls(prompts, model="meta-llama/llama-4-maverick-17b-128e-instruct")
        hotel_reservation_key_insights = [
            {
                **email_metadata,
                'key_insights': batch_hotel_reservation_key_insights.get(email_metadata['id'], '')
            }
            for email_metadata in body_checked_filtered_hotel_reservation_emails
        ]
        save_to_jsonl('hotel_reservation_key_insights.jsonl', hotel_reservation_key_insights)
    else:
        hotel_reservation_key_insights = load_jsonl('hotel_reservation_key_insights.jsonl')

    print("-" * 80)
    for email_data in hotel_reservation_key_insights[:DISPLAY_LIMIT]:
        print(f"{email_data['subject']}")
        print(f"   Id: {email_data['id']}")
        print(f"   From: {email_data['sender']}")
        print(f"   Date: {email_data['date']}")
        print(f"   To: {email_data['recipient']}")
        print(f"   Reply-To: {email_data['reply_to']}")
        print(f"   CC: {email_data['cc']}")
        print(f"   BCC: {email_data['bcc']}")
        print(f"   In-Reply-To: {email_data['in_reply_to']}")
        print(f"   Key Insights: {email_data['key_insights']}")
        print("-" * 80)
        print()

    # Remove body from the email metadata since we extracted the key insights and we want to reduce token llm count.
    hotel_reservation_key_insights = [
        {
            **{k: v for k, v in email_metadata.items() if k != "body"},
        }
        for email_metadata in hotel_reservation_key_insights
    ]

    # print(f"Generating insights from hotel confirmation emails...")
    # trip_insights = ""
    # trip_insights = generate_trip_insights(hotel_reservation_key_insights, os.getenv("OPENAI_API_KEY"), existing_trip_insights = trip_insights)
    # print(f"trip_insights:\n{trip_insights}")

    # If too much data for context window, split into batches, and cycle through them while accumulating insights.
    print(f"\nGenerating insights from hotel confirmation emails...\n")
    trip_insights = ""
    num_batches = (len(hotel_reservation_key_insights) + HOTEL_RESERVATION_EMAILS_BATCH_SIZE - 1) // HOTEL_RESERVATION_EMAILS_BATCH_SIZE
    for i in range(0, len(hotel_reservation_key_insights), HOTEL_RESERVATION_EMAILS_BATCH_SIZE):
        current_batch = hotel_reservation_key_insights[i:i + HOTEL_RESERVATION_EMAILS_BATCH_SIZE]
        batch_num = i // HOTEL_RESERVATION_EMAILS_BATCH_SIZE + 1
        print(f"Processing batch {batch_num}/{num_batches} ({len(current_batch)} emails)...")

        # Call generate_trip_insights with the current batch and existing insights
        trip_insights = generate_trip_insights(
            current_batch,
            os.getenv("OPENAI_API_KEY"),
            existing_trip_insights=trip_insights  # Pass the accumulated insights
        )

        print(f"Processed batch {batch_num}/{num_batches} ({len(current_batch)} emails), current trip insights:\n{trip_insights}\n")


    print(f"Generating up to {NUM_TRIPS_METADATA_TO_GENERATE} trip metadatas...")
    # hotel_reservation_key_insights # If too much data for context window, just send summarized trip_insights, works pretty well.
    trip_jsons = generate_trips_metadatas([], trip_insights, NUM_TRIPS_METADATA_TO_GENERATE, os.getenv("OPENAI_API_KEY"))
    # Pretty print the trip JSON data
    if trip_jsons:
        print("\n=== Generated Trip Metadata ===\n")
        print(json.dumps(trip_jsons, indent=4))
        print("\n=============================\n")

if __name__ == "__main__":
    main()
