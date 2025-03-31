# Usage:
# uv run search_tripadvisor_collections.py --type hotel_review --query "spa mountain view" --limit 5
# uv run search_tripadvisor_collections.py --type restaurant_review --query "italian pasta" --city "Aspen" --state "Colorado"

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import json
import argparse
import re
from dotenv import load_dotenv
from bson import json_util

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description='Search TripAdvisor location data in MongoDB by keywords.')
parser.add_argument('--type', choices=['hotel_review', 'restaurant_review'], required=True,
                    help='Type of location data to search (hotel_review or restaurant_review)')
parser.add_argument('--query', required=False, default="",
                    help='Search keywords (e.g., "spa mountain view", "italian pasta")')
parser.add_argument('--city', 
                    help='Filter by city (e.g., "Aspen")')
parser.add_argument('--state', 
                    help='Filter by state (e.g., "Colorado")')
parser.add_argument('--country', default="United States",
                    help='Filter by country (default: "United States")')
parser.add_argument('--limit', type=int, default=10, 
                    help='Limit the number of results returned (default: 10)')
parser.add_argument('--output', 
                    help='Optional JSON file to save results (default: prints to console)')
args = parser.parse_args()

# Ensure at least one search criterion is provided
if not args.query and not args.city and not args.state and not args.country:
    parser.error("At least one search criterion must be provided: --query, --city, --state, or --country")

# Load environment variables from .env file
load_dotenv()

# Define collection mapping
collections_data = {
    'hotel_review': {
        "collection_name": "tripadvisor-hotel_review",
        "search_fields": ["name", "description", "amenities", "trip_types.name", "styles"]
    },
    'restaurant_review': {
        "collection_name": "tripadvisor-restaurant_review",
        "search_fields": ["name", "description", "cuisine.name"]
    },
}

data_type = args.type
search_info = []
if args.query:
    search_info.append(f"keywords: \"{args.query}\"")
if args.city:
    search_info.append(f"city: \"{args.city}\"")
if args.state:
    search_info.append(f"state: \"{args.state}\"")
if args.country:
    search_info.append(f"country: \"{args.country}\"")

print(f"Searching {collections_data[data_type]['collection_name']} collection for {', '.join(search_info)}")
print(f"Limiting results to {args.limit}")

# Get MongoDB credentials from environment variables
username = os.getenv("MONGODB_USERNAME")
password = os.getenv("MONGODB_PASSWORD")
cluster = os.getenv("MONGODB_CLUSTER")

# Construct MongoDB URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName=Viammo-Cluster-alpha"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

try:
    # Send a ping to confirm a successful connection
    client.admin.command('ping')
    print("Connected to MongoDB successfully!")

    db = client["viammo-alpha"]
    collection_name = collections_data[data_type]["collection_name"]
    collection = db[collection_name]
    
    # Build the query
    query_conditions = []
    
    # Add text search conditions if query is provided
    if args.query:
        # Prepare search fields for this collection
        search_fields = collections_data[data_type]["search_fields"]
        
        # Split the query string into keywords
        keywords = args.query.strip().split()
        
        # Build the keyword search conditions
        search_conditions = []
        for field in search_fields:
            # Check if this is a field that needs array handling (contains a dot)
            if '.' in field:
                # Extract the array field name and the subfield
                array_field, subfield = field.split('.', 1)
                for keyword in keywords:
                    # For array fields, use $elemMatch to match inside array elements
                    field_condition = {
                        array_field: {
                            "$elemMatch": {
                                subfield: {"$regex": f".*{re.escape(keyword)}.*", "$options": "i"}
                            }
                        }
                    }
                    search_conditions.append(field_condition)
            else:
                # Regular field handling
                for keyword in keywords:
                    field_condition = {field: {"$regex": f".*{re.escape(keyword)}.*", "$options": "i"}}
                    search_conditions.append(field_condition)
        
        if search_conditions:
            query_conditions.append({"$or": search_conditions})
    
    # Add address filters if provided
    address_conditions = {}
    if args.city:
        address_conditions["address_obj.city"] = {"$regex": f"^{re.escape(args.city)}$", "$options": "i"}
    if args.state:
        address_conditions["address_obj.state"] = {"$regex": f"^{re.escape(args.state)}$", "$options": "i"}
    if args.country:
        address_conditions["address_obj.country"] = {"$regex": f"^{re.escape(args.country)}$", "$options": "i"}
    
    if address_conditions:
        query_conditions.append(address_conditions)
    
    # Combine all conditions with AND logic
    if query_conditions:
        final_query = {"$and": query_conditions} if len(query_conditions) > 1 else query_conditions[0]
    else:
        final_query = {}
    
    # Execute the search
    results = list(collection.find(final_query).limit(args.limit))
    
    print(f"Found {len(results)} results matching your criteria.")
    
    # Process and display or save results
    if results:
        # Convert MongoDB documents to displayable JSON
        parsed_results = json.loads(json_util.dumps(results))
        
        # Display summary if not saving to file
        if not args.output:
            print("\nSearch Results:")
            print("=" * 80)
            
            for i, result in enumerate(parsed_results, 1):
                location_id = result.get("location_id", "N/A")
                name = result.get("name", "Unnamed Location")
                rating = result.get("rating", "N/A")
                price_level = result.get("price_level", "N/A")
                
                print(f"{i}. {name} ({rating} stars) | {price_level})")
                print(f"   Location ID: {location_id}")
                
                # Display address if available
                address_obj = result.get("address_obj", {})
                if address_obj:
                    address_string = address_obj.get("city", "") + ", " + address_obj.get("state", "") + " " + address_obj.get("country", "")
                    if address_string:
                        print(f"   Address: {address_string}")
                
                # Display additional category-specific information
                if data_type == 'hotel_review':
                    amenities = result.get("amenities", [])
                    if amenities and len(amenities) > 0:
                        print(f"   Top amenities: {', '.join(amenities[:5])}")
                            
                elif data_type == 'restaurant_review':
                    cuisine = result.get("cuisine", [])
                    if cuisine:
                        cuisine_names = [c.get("name") for c in cuisine[:5] if c.get("name")]
                        if cuisine_names:
                            print(f"   Cuisine: {', '.join(cuisine_names)}")
                
                # Show a snippet of description if available
                description = result.get("description", "")
                if description:
                    # Create a short snippet (~100 characters)
                    snippet = description[:100] + "..." if len(description) > 100 else description
                    print(f"   Description: {snippet}")
                
                print("-" * 80)
        
        # Save to file if requested
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(parsed_results, f, indent=2)
            print(f"Results saved to {args.output}")

    else:
        print("No results found matching your criteria.")

except Exception as e:
    print(f"An error occurred: {str(e)}")

finally:
    # Close the MongoDB connection
    client.close()
    print("MongoDB connection closed.")
