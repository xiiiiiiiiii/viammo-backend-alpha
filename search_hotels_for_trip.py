# Usage:
# uv run search_hotels_for_trip.py --trip_id "67e31524c3bdddc136254061" --limit 5

# quick substring search in mongodb
# db.tripadvisor_hotel_review.find({ brand: { $regex: "regis", $options: "i" } })
    
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
import os
import json
import argparse
import re
from dotenv import load_dotenv
from bson import json_util
from datetime import datetime

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description='Search for hotels based on trip data from MongoDB.')
parser.add_argument('--trip_id', required=True,
                    help='MongoDB _id of the trip to use for search')
parser.add_argument('--limit', type=int, default=10, 
                    help='Limit the number of results returned (default: 10)')
parser.add_argument('--output', 
                    help='Optional JSON file to save results (default: prints to console)')
parser.add_argument('--disable_text_search', action='store_true',
                    help='Disable BM25 text search and use only exact field matching (default: text search enabled)')
parser.add_argument('--generate_keywords', action='store_true', default=True,
                    help='Use OpenAI (through LangChain) to generate ideal hotel characteristics based on trip data (default: enabled)')
parser.add_argument('--rerank_results', action='store_true', default=True,
                    help='Use OpenAI to rerank results based on trip data (default: enabled)')
args = parser.parse_args()

llm_model = "gpt-4o-mini"

# Load environment variables from .env file
load_dotenv()

# Get MongoDB credentials from environment variables
username = os.getenv("MONGODB_USERNAME")
password = os.getenv("MONGODB_PASSWORD")
cluster = os.getenv("MONGODB_CLUSTER")

# US state abbreviation to full name mapping
US_STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", 
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware", 
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho", 
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", 
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", 
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota", "MS": "Mississippi", 
    "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", 
    "NH": "New Hampshire", "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", 
    "NC": "North Carolina", "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", 
    "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina", 
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", 
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", 
    "WI": "Wisconsin", "WY": "Wyoming", "DC": "District of Columbia"
}

# Create a reverse mapping (full name to abbreviation)
US_STATE_ABBREVS = {v: k for k, v in US_STATES.items()}

# Filter out common stop words and short words
stop_words = set(['the', 'and', 'or', 'a', 'an', 'in', 'on', 'at', 'to', 'for', 'with', 
                'by', 'about', 'as', 'of', 'from', 'that', 'this', 'it', 'is', 'are', 
                'was', 'were', 'be', 'been', 'being', 'have', 'has', 'had', 'do', 'does', 
                'did', 'will', 'would', 'should', 'could', 'can', 'may', 'might', 'must',
                'i', 'you', 'he', 'she', 'we', 'they', 'me', 'him', 'her', 'us', 'them'])

# Construct MongoDB URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName=Viammo-Cluster-alpha"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # Send a ping to confirm a successful connection
    client.admin.command('ping')
    print("Connected to MongoDB successfully!")

    db = client["viammo-alpha"]
    
    # 1. Get trip data from the 'trips' collection
    trips_collection = db["trips"]
    
    try:
        # Convert trip_id string to ObjectId
        trip_id_obj = ObjectId(args.trip_id)
        trip_data = trips_collection.find_one({"_id": trip_id_obj})
        
        if not trip_data:
            print(f"No trip found with ID: {args.trip_id}")
            exit(1)
            
        # Get trip title from either 'title' or 'name' field
        trip_title = trip_data.get('name', '')
        
        # Create a variable to store trip data information for later use
        trip_data_string = f"Found trip: {trip_title}\n"
        
        # 2. Extract relevant fields for search
        destination = trip_data.get('destination', {})
        # Add destination to trip data string
        trip_data_string += f"\nTrip data:\n"
        trip_data_string += f"- destination: {destination}\n"
        
        # Get date information from trip
        start_date = trip_data.get('startDate', 'N/A')
        end_date = trip_data.get('endDate', 'N/A')
        
        # Format dates to remove time portion (if they're valid dates)
        if start_date != 'N/A' and isinstance(start_date, str) and 'T' in start_date:
            start_date = start_date.split('T')[0]  # Keep only the date part before 'T'
        if end_date != 'N/A' and isinstance(end_date, str) and 'T' in end_date:
            end_date = end_date.split('T')[0]  # Keep only the date part before 'T'
            
        trip_data_string += f"- startDate: {start_date}\n"
        trip_data_string += f"- endDate: {end_date}\n"
        
        # Handle destination field that might be a string or an object
        if isinstance(destination, dict):
            destination_city = destination.get('city', '')
            destination_state = destination.get('state', '')
            destination_country = destination.get('country', 'United States')
        else:
            # If destination is a string, try to parse it
            # Format might be "City, State, Country" or variations
            destination_parts = str(destination).split(',')
            
            if len(destination_parts) >= 1:
                # First part is likely the city
                destination_city = destination_parts[0].strip()
            else:
                destination_city = ''
                
            if len(destination_parts) >= 2:
                # Second part is likely the state/region
                destination_state = destination_parts[1].strip()
            else:
                destination_state = ''
                
            if len(destination_parts) >= 3:
                # Third part is likely the country
                destination_country = destination_parts[2].strip()
            else:
                destination_country = 'United States'
        
        # Debug other key fields
        total_budget = trip_data.get('totalBudget', None)
        trip_data_string += f"- totalBudget: {total_budget}\n"
        
        notes = trip_data.get('notes', '')
        trip_data_string += f"- notes: {notes[:50]}{'...' if len(str(notes)) > 50 else ''}\n"
        
        # Use either 'title' or 'name' field for title
        title = trip_data.get('name', '')
        trip_data_string += f"- title: {title}\n"
        
        # Get purpose field (if available)
        purpose = trip_data.get('purpose', '')
        trip_data_string += f"- purpose: {purpose[:50]}{'...' if len(str(purpose)) > 50 else ''}\n"
        
        # Print all collected trip information at once
        print(trip_data_string)
        
        # Extract relevant keywords from trip data
        search_keywords = []
        
        # Add keywords from title
        if title:
            # Extract meaningful words, ignore common words like "to", "in", etc.
            title_words = [word for word in re.findall(r'\b\w+\b', title.lower()) 
                          if len(word) > 2 and word not in stop_words]
            search_keywords.extend(title_words)
            print(f"Extracted keywords {title_words} from title")
        else:
            print("No title keywords extracted (empty title)")
        
        # Add keywords from purpose
        if purpose:
            # Extract meaningful words from purpose
            purpose_words = [word.lower() for word in re.findall(r'\b[a-zA-Z]+\b', str(purpose))]
            
            # Filter out stop words and short words (less than 3 characters)
            meaningful_purpose_words = [word for word in purpose_words if word not in stop_words and len(word) > 2]
            
            # Add unique words from purpose
            for word in meaningful_purpose_words:
                if word not in search_keywords:
                    search_keywords.append(word)
            
            print(f"Extracted keywords {meaningful_purpose_words} from purpose")
        else:
            print("No purpose keywords extracted (empty purpose field)")
        
        # Add keywords from notes
        if notes:
            # Extract all meaningful words from notes (instead of just specific amenities)
            # Find all words, convert to lowercase
            notes_words = [word.lower() for word in re.findall(r'\b[a-zA-Z]+\b', notes)]
            
            # Filter out stop words and short words (less than 3 characters)
            meaningful_words = [word for word in notes_words if word not in stop_words and len(word) > 2]
            
            # Add unique words from notes
            for word in meaningful_words:
                if word not in search_keywords:
                    search_keywords.append(word)
            
            print(f"Extracted keywords {meaningful_words} from notes")
        
        # Option to generate ideal hotel characteristics using LangChain and a mini OpenAI model
        if args.generate_keywords:
            try:
                from langchain_openai import ChatOpenAI
                from langchain.prompts import ChatPromptTemplate
                
                # Check if OpenAI API key is set
                openai_api_key = os.getenv("OPENAI_API_KEY")
                
                if not openai_api_key:
                    print("Warning: OPENAI_API_KEY environment variable not set. Skipping keyword generation.")
                else:
                    print("Generating ideal hotel characteristics using LangChain and OpenAI...")
                    
                    # Initialize the LLM with the API key explicitly
                    llm = ChatOpenAI(model=llm_model, openai_api_key=openai_api_key)
                    
                    # Define a prompt template for hotel characteristics
                    template = """
                    Based on the following trip information, generate keywords for ideal hotel characteristics that would best match this trip:
                    
                    {trip_data}
                    
                    Please provide a list of keywords from the following categories to use in a bm25 hotel search:
                    1. Ideal detailed hotel description
                    2. 10-15 amenity keywords that would be important for this trip
                    3. 3-5 trip type keywords that match this traveler (e.g., "family", "business", "couples", "solo travel")
                    4. 2-3 hotel style keywords that would be appropriate (e.g., "Luxury", "Modern", "Boutique", "Budget")
                    
                    Format your response as a simple list of lowercase keywords separated by spaces.
                    
                    Return only the list of keywords, no bullets, no numbers, no other text.
                    """
                    
                    prompt = ChatPromptTemplate.from_template(template)
                    
                    # Generate the response
                    chain = prompt | llm
                    response = chain.invoke({"trip_data": trip_data_string})

                    # Extract keywords from the response
                    response_content = response.content
                    print(f"Response content: {response_content}")
                    if not response_content or len(response.content.split()) == 0:
                        print(f"LLM did not return a response")
                    else:
                        generated_keywords = set([word.lower() for word in response.content.split()])
                        print(f"Extracted keywords: \n{generated_keywords}")
                            
                        # Add to search keywords if not already present.
                        for word in generated_keywords:
                            if word not in search_keywords:
                                search_keywords.append(word)
                        
                        print(f"\nAdded {len(generated_keywords)} generated keywords to search")
            except ImportError:
                print("Warning: LangChain or OpenAI packages not installed. Skipping keyword generation.")
                print("To install required packages: pip install langchain langchain-openai")
        
        # Use totalBudget directly as price_level (already in $ format)
        price_level = trip_data.get('totalBudget', "")
        
        # 3. Search for hotels
        hotels_collection = db["tripadvisor-hotel_review"]
        
        # Check basic stats about the collection
        total_hotels = hotels_collection.count_documents({})
        print(f"\nTotal hotels in database: {total_hotels}")
        
        # Build search query with available fields
        query_conditions = []
        
        # 1. Add location filter based on address_obj
        if destination_city:
            # Check if we have hotels with city data
            city_hotels_count = hotels_collection.count_documents({"address_obj.city": {"$exists": True}})
            print(f"Hotels with city data: {city_hotels_count}")
            
            # Match city in address_obj (just use "Aspen" without other parts)
            city_condition = {"address_obj.city": "Aspen"}
            query_conditions.append(city_condition)
        
        # Handle state matching with abbreviations and full names
        if destination_state:
            state_value = destination_state.strip()
            state_conditions = []
            
            # Case 1: Input is a 2-letter state code (e.g., "CO")
            if len(state_value) == 2 and state_value.upper() in US_STATES:
                state_abbrev = state_value.upper()
                full_state_name = US_STATES[state_abbrev]
                print(f"Converting state abbreviation '{state_value}' to full name '{full_state_name}'")
                
                # Match both abbreviation and full name
                state_conditions.append({"address_obj.state": state_abbrev})
                state_conditions.append({"address_obj.state": full_state_name})
                
            # Case 2: Input is a full state name (e.g., "Colorado")
            elif state_value.title() in US_STATE_ABBREVS:
                full_state_name = state_value.title()
                state_abbrev = US_STATE_ABBREVS[full_state_name]
                print(f"Also matching state abbreviation '{state_abbrev}' for '{full_state_name}'")
                
                # Match both abbreviation and full name
                state_conditions.append({"address_obj.state": state_abbrev})
                state_conditions.append({"address_obj.state": full_state_name})
                
            # Case 3: Input doesn't match known states, use as-is
            else:
                state_conditions.append({"address_obj.state": state_value})
            
            # Add OR condition to match any state format
            if len(state_conditions) > 1:
                query_conditions.append({"$or": state_conditions})
            else:
                query_conditions.append(state_conditions[0])
        
        # Country matching (United States vs USA)
        if destination_country:
            country_value = destination_country.strip()
            country_conditions = []
            
            # Handle common variations of United States
            if country_value.upper() in ["USA", "U.S.A.", "U.S.", "UNITED STATES", "UNITED STATES OF AMERICA"]:
                country_conditions.append({"address_obj.country": "United States"})
                country_conditions.append({"address_obj.country": "USA"})
                country_conditions.append({"address_obj.country": "U.S.A."})
                country_conditions.append({"address_obj.country": "U.S."})
            # Handle variations of United Kingdom
            elif country_value.upper() in ["UK", "U.K.", "UNITED KINGDOM", "GREAT BRITAIN"]:
                country_conditions.append({"address_obj.country": "United Kingdom"})
                country_conditions.append({"address_obj.country": "UK"})
                country_conditions.append({"address_obj.country": "U.K."})
                country_conditions.append({"address_obj.country": "Great Britain"})
            # Add other common country variations as needed
            else:
                # Use as-is for other countries
                country_conditions.append({"address_obj.country": country_value})
            
            # Add OR condition to match any country format
            if len(country_conditions) > 1:
                query_conditions.append({"$or": country_conditions})
            else:
                query_conditions.append(country_conditions[0])
        
        # 2. Add price level filter (required)
        if price_level:
            # Use exact price level match
            query_conditions.append({"price_level": price_level})
        
        # Let's print some diagnostic info about the collection
        price_levels = hotels_collection.distinct("price_level")
        print(f"Available price levels in database: {price_levels}")
        
        # Check how many hotels are in Aspen
        aspen_hotels = hotels_collection.count_documents({"address_obj.city": "Aspen"})
        print(f"Hotels in Aspen: {aspen_hotels}")

        # Check how many Aspen Colorado hotels
        aspen_colorado_hotels = hotels_collection.count_documents({
            "address_obj.city": "Aspen",
            "address_obj.state": "Colorado",
        })
        print(f"Hotels in Aspen Colorado: {aspen_colorado_hotels}")

        # Check how many Aspen Colorado United States hotels
        aspen_colorado_us_hotels = hotels_collection.count_documents({
            "address_obj.city": "Aspen",
            "address_obj.state": "Colorado",
            "address_obj.country": "United States"
        })
        print(f"Hotels in Aspen Colorado United States: {aspen_colorado_us_hotels}")
        
        # Check how many hotels have the exact price level
        price_hotels = hotels_collection.count_documents({"price_level": price_level})
        print(f"Hotels with price level '{price_level}': {price_hotels}")
        
        # Check how many Aspen hotels have the price level
        aspen_price_hotels = hotels_collection.count_documents({
            "address_obj.city": "Aspen",
            "price_level": price_level
        })
        print(f"Aspen hotels with price level '{price_level}': {aspen_price_hotels}")
        
        # Check if text index exists, create if needed
        indexes = hotels_collection.list_indexes()
        text_index_exists = False
        for index in indexes:
            if index.get('name') == 'text_search_index':
                text_index_exists = True
                break
                
        if not text_index_exists:
            print("Creating text index for full-text search (this may take a minute)...")
            hotels_collection.create_index([
                ("name", "text"), 
                ("description", "text"),
                ("styles", "text"),
                ("trip_types.name", "text"),
                ("amenities", "text"),
                ("brand", "text")
            ], name="text_search_index")
            print("Text index created successfully")

        
        # # Debug
        # search_keywords = ['pool']
        
        # Build combined search including full-text search
        if search_keywords and not args.disable_text_search:
            # Full-text search with $text (uses BM25 for ranking)
            # Join keywords into a single space-separated string for $text operator
            text_search_string = " ".join(search_keywords)
            print(f"\nAdding full-text search with BM25 scoring for: '{text_search_string}'")
            
            # If we have at least one keyword, add a text search query
            if text_search_string:
                # Create a text search query (uses BM25 TF-IDF scoring)
                text_query = {
                    "$text": {
                        "$search": text_search_string,
                        "$caseSensitive": False,
                        "$diacriticSensitive": False
                    }
                }
                query_conditions.append(text_query)
                
                # Project the text score in results
                projection = {"score": {"$meta": "textScore"}}
                
                # Note: we'll also sort by the text score (higher score = better relevancy)
                # This will override the rating sort for better relevance
                text_sort = [("score", {"$meta": "textScore"})]
        elif search_keywords and args.disable_text_search:
            print(f"\nText search disabled. Keywords will be ignored: {', '.join(search_keywords)}")
        
        # Combine with AND logic for required conditions
        final_query = {"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0] if query_conditions else {}
        
        # Print final query for debugging
        print(f"\nFinal query: {json.dumps(final_query, indent=2)}")
        
        # Find matched hotels
        # If using full-text search, use the textScore for sorting
        if search_keywords and not args.disable_text_search and text_search_string:
            # Get the limited results with proper sorting
            search_results = list(hotels_collection.find(final_query,  projection).sort(text_sort).limit(args.limit))
            print(f"\nFound {len(search_results)} hotels matching search criteria (sorted by BM25 text relevance)")
        else:
            # Just sort by rating if no text search
            search_results = list(hotels_collection.find(final_query).sort([("rating", -1)]).limit(args.limit))
            print(f"Found {len(search_results)} hotels matching search criteria (sorted by rating)")
        
        # Process and display results
        if search_results:
            # Convert MongoDB documents to displayable JSON
            parsed_results = json.loads(json_util.dumps(search_results))
            
            # Rerank results using OpenAI if requested
            if args.rerank_results:
                try:
                    from langchain_openai import ChatOpenAI
                    from langchain.prompts import ChatPromptTemplate
                    
                    # Check if OpenAI API key is set
                    openai_api_key = os.getenv("OPENAI_API_KEY")
                    
                    if not openai_api_key:
                        print("Warning: OPENAI_API_KEY environment variable not set. Skipping reranking.")
                    else:
                        print("\nReranking results using OpenAI...")
                        
                        # Initialize the LLM
                        llm = ChatOpenAI(model=llm_model, openai_api_key=openai_api_key)
                        
                        # Create a prompt template for reranking
                        rerank_template = """
                        Based on the following trip information and list of hotels, select the single best hotel that matches the trip requirements.
                        Consider the trip purpose, budget, and any specific requirements mentioned.
                        
                        Trip Information:
                        {trip_data}
                        
                        Hotels:
                        {hotels_data}
                        
                        Return only the hotel name that best matches the trip requirements.
                        Do not include any explanation or additional text.
                        
                        Best Hotel: """
                        
                        prompt = ChatPromptTemplate.from_template(rerank_template)
                        
                        # Prepare all hotel data for the prompt
                        hotels_data = []
                        for i, hotel in enumerate(parsed_results, 1):
                            hotel_info = f"""
                            Hotel {i}:
                            Name: {hotel.get('name', 'Unknown')}
                            Rating: {hotel.get('rating', 'N/A')}/5
                            Price Level: {hotel.get('price_level', 'N/A')}
                            Styles: {', '.join(hotel.get('styles', []))}
                            Trip Types: {', '.join([t.get('name', t) if isinstance(t, dict) else t for t in hotel.get('trip_types', [])])}
                            Amenities: {', '.join([a.get('name', a) if isinstance(a, dict) else a for a in hotel.get('amenities', [])])}
                            Description: {hotel.get('description', '')[:200]}  # Limit description length for each hotel
                            """
                            hotels_data.append(hotel_info)
                        
                        # Get the best hotel from LLM
                        chain = prompt | llm
                        response = chain.invoke({
                            "trip_data": trip_data_string,
                            "hotels_data": "\n".join(hotels_data)
                        })
                        
                        best_hotel_name = response.content.strip()
                        print(f"\nLLM selected best hotel: {best_hotel_name}")
                        
                        # Find the selected hotel and move it to the top
                        for i, hotel in enumerate(parsed_results):
                            if hotel.get('name') == best_hotel_name:
                                # Move the selected hotel to the top
                                selected_hotel = parsed_results.pop(i)
                                parsed_results.insert(0, selected_hotel)
                                print(f"Moved {best_hotel_name} to the top of the results")
                                break
                        
                        print("Reranking complete!")
                        
                except ImportError:
                    print("Warning: LangChain or OpenAI packages not installed. Skipping reranking.")
                    print("To install required packages: pip install langchain langchain-openai")
            
            # Display summary if not saving to file
            if not args.output:
                print("\nRecommended Hotels:")
                print("=" * 80)
                
                # Create an array to store formatted JSON objects
                formatted_results = []
                
                for i, hotel in enumerate(parsed_results, 1):
                    hotel_id = hotel.get('location_id', 'N/A')
                    name = hotel.get('name', 'Unnamed Hotel')
                    rating = hotel.get('rating', 'N/A')
                    price = hotel.get('price_level', 'N/A')
                    
                    # Show scores
                    print(f"{i}. {name} (ID: {hotel_id})")
                    
                    # Always show BM25 score if available
                    score = hotel.get('score', None)
                    score_text = f"BM25 Score: {score:.2f} | " if score else ""
                    
                    # Add rerank score if reranking is enabled
                    if args.rerank_results:
                        rerank_score = hotel.get('rerank_score', 0)
                        score_text += f"Rerank Score: {rerank_score:.1f}/10 | "
                    
                    # Add rating and price
                    score_text += f"Rating: {rating}/5 | Price: {price}"
                    print(f"   {score_text}")
                    
                    # Display latitude and longitude if available
                    latitude = hotel.get('latitude', None)
                    longitude = hotel.get('longitude', None)
                    if latitude and longitude:
                        print(f"   Location: ({latitude}, {longitude})")
                    
                    # Get main photo URL if available:
                    main_photo_url = None
                    if 'photos' in hotel and len(hotel['photos']) > 0:
                        first_photo = hotel['photos'][0]
                        if 'images' in first_photo and 'original' in first_photo['images']:
                            main_photo_url = first_photo['images']['original'].get('url', None)
                            if main_photo_url:
                                print(f"   Main Photo: {main_photo_url}")
                    
                    # Display address
                    address_obj = hotel.get('address_obj', {})
                    address_string = None
                    if address_obj:
                        address_string = address_obj.get('address_string', '')
                        if address_string:
                            print(f"   Address: {address_string}")
                    
                    # Create a JSON object for this hotel
                    today_date = datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000Z")
                    
                    formatted_hotel = {
                        "trip_id": {"$oid": str(trip_id_obj)},
                        "type": "accommodation",
                        "name": f"Stay at {name}",
                        "date": start_date,
                        "endDate": end_date,
                        "location": {
                            "name": name,
                            "address": address_string or "",
                            "coordinates": {
                                "lat": {"$numberDouble": str(latitude) if latitude else "0"},
                                "lng": {"$numberDouble": str(longitude) if longitude else "0"}
                            }
                        },
                        "notes": f"Rating: {rating}/5",
                        "status": "draft",
                        "createdAt": today_date,
                        "updatedAt": today_date,
                        "description": hotel.get('description', ''),
                        "main_media": main_photo_url or "",
                        "budget": price
                    }
                    
                    # Add this hotel to the formatted results array
                    formatted_results.append(formatted_hotel)
                    
                    # Display all fields used in text index
                    
                    # 1. Display hotel styles (e.g., Luxury, Boutique)
                    styles = hotel.get('styles', [])
                    if styles:
                        print(f"   Styles: {', '.join(styles[:5])}")
                        if len(styles) > 5:
                            print(f"        + {len(styles)-5} more")
                    
                    # 2. Display trip types
                    trip_types = hotel.get('trip_types', [])
                    if trip_types:
                        # Trip types can be strings or objects with 'name' field
                        trip_type_names = []
                        for trip_type in trip_types[:5]:
                            if isinstance(trip_type, str):
                                trip_type_names.append(trip_type)
                            elif isinstance(trip_type, dict) and 'name' in trip_type:
                                trip_type_names.append(trip_type['name'])
                        
                        if trip_type_names:
                            print(f"   Trip Types: {', '.join(trip_type_names)}")
                            if len(trip_types) > 5:
                                print(f"        + {len(trip_types)-5} more")
                    
                    # 3. Display amenities
                    amenities = hotel.get('amenities', [])
                    if amenities:
                        # Handle both string arrays and object arrays with 'name' field
                        amenity_names = []
                        for amenity in amenities:
                            if isinstance(amenity, str):
                                amenity_names.append(amenity)
                            elif isinstance(amenity, dict) and 'name' in amenity:
                                amenity_names.append(amenity['name'])
                        
                        if amenity_names:
                            print(f"   Amenities ({len(amenity_names)}):")
                            # Group amenities into chunks of 5 for better display
                            chunk_size = 5
                            for i in range(0, len(amenity_names), chunk_size):
                                chunk = amenity_names[i:i + chunk_size]
                                print(f"     - {', '.join(chunk)}")
                    
                    # 4. Show a snippet of description
                    description = hotel.get('description', '')
                    if description:
                        # Create a formatter function to wrap text at 72 characters with clean breaks
                        def format_text_block(text, width=72):
                            """Format text into lines of maximum width characters, breaking at word boundaries."""
                            words = text.split()
                            lines = []
                            current_line = []
                            current_length = 0
                            
                            for word in words:
                                # Check if adding this word would exceed the width
                                if current_length + len(word) + (1 if current_length > 0 else 0) > width:
                                    # Line would be too long, start a new line
                                    lines.append(' '.join(current_line))
                                    current_line = [word]
                                    current_length = len(word)
                                else:
                                    # Add word to current line
                                    current_line.append(word)
                                    # Add 1 for the space before the word (if not the first word)
                                    current_length += len(word) + (1 if current_length > 0 else 0)
                            
                            # Add the last line if there's anything left
                            if current_line:
                                lines.append(' '.join(current_line))
                                
                            return lines
                        
                        # Format the description with clean line breaks
                        formatted_lines = format_text_block(description)
                        
                        # Take first 5 lines (or fewer if description is shorter)
                        snippet_lines = formatted_lines[:5]
                        
                        # Add ellipsis if there are more lines
                        if len(formatted_lines) > 5:
                            snippet_lines[-1] += "..."
                            
                        # Display each line of the description with proper indentation
                        print(f"   Description:")
                        for line in snippet_lines:
                            print(f"     {line}")
                    
                    print("-" * 80)
            
            # Save to file if requested
            if args.output:
                with open(args.output, 'w') as f:
                    json.dump(formatted_results, f, indent=2)
                print(f"Results saved to {args.output}")
            else:
                print("No matching hotels found for this trip.")
                
                # Suggest a more relaxed search if no results
                print("\nTry modifying your trip details:")
                print("- Check the destination spelling")
                print("- Add specific amenities you're looking for in the trip notes")
                print("- Adjust your budget to match available options")
    
            # Print formatted JSON array at the end
            if 'formatted_results' in locals() and formatted_results:
                print("\n" + "=" * 80)
                print("FORMATTED JSON RESULTS:")
                print("=" * 80)
                print(json.dumps(formatted_results, indent=2))
                print("=" * 80)
        else:
            print("No matching hotels found for this trip.")
            
            # Suggest a more relaxed search if no results
            print("\nTry modifying your trip details:")
            print("- Check the destination spelling")
            print("- Add specific amenities you're looking for in the trip notes")
            print("- Adjust your budget to match available options")
    
    except ValueError:
        print(f"Invalid trip ID format: {args.trip_id}")
        print("Trip ID should be a valid MongoDB ObjectId (24 character hex string)")
    
except PyMongoError as e:
    print(f"MongoDB error: {str(e)}")
except Exception as e:
    print(f"An error occurred: {str(e)}")
finally:
    # Close the MongoDB connection
    client.close()
    print("\nMongoDB connection closed.")
