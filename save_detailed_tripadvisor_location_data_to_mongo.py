# Usage:
# 1 - Required:
# - create collection in MongoDB with name used below
# - add unique index on location_id ...createIndex( { "location_id": 1 }, { unique: true } )
#
# 2 - Run:
# uv run save_detailed_tripadvisor_location_data_to_mongo.py --type hotel_review


from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import requests
import json
import argparse
import time
from dotenv import load_dotenv

parser = argparse.ArgumentParser(description='Get detailed location data from TripAdvisor API files and load it into MongoDB.')
parser.add_argument('--type', choices=['hotel_review', 'restaurant_review'], required=True,
                    help='Type of location IDs to load (exactly one)')
parser.add_argument('--limit', type=int, default=None, 
                    help='Limit the number of IDs to process and save (default: no limit)')
parser.add_argument('--with_photos', action='store_true', default=True,
                    help='Fetch additional photos for each location (default: enabled)')
parser.add_argument('--photos_per_location', type=int, default=5,
                    help='Number of photos to fetch per location (default: 5)')

args = parser.parse_args()

# Load environment variables from .env file
load_dotenv()

collections_data = {
    'hotel_review': {
        "collection_name": "tripadvisor-hotel_review",
        "location_ids_list_file": "./data/aspen/tripadvisor-hotel_review/ids.jsonl",
    },
    'restaurant_review': {
        "collection_name": "tripadvisor-restaurant_review",
        "location_ids_list_file": "./data/aspen/tripadvisor-restaurant_review/ids.jsonl",
    },
}

data_type = args.type
print(f"Loading data for {data_type} into {collections_data[data_type]['collection_name']}")
collection_data = collections_data[data_type]

if args.limit:
    print(f"Limiting to a maximum of {args.limit} IDs")

# Get MongoDB credentials from environment variables
username = os.getenv("MONGODB_USERNAME")
password = os.getenv("MONGODB_PASSWORD")
cluster = os.getenv("MONGODB_CLUSTER")

TRIPADVISOR_API_KEY = os.getenv("TRIPADVISOR_API_KEY")

# Construct MongoDB URI
uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName=Viammo-Cluster-alpha"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
client.admin.command('ping')
print("Pinged your deployment. You successfully connected to MongoDB!\n")

db = client["viammo-alpha"]
collection_name = collection_data["collection_name"]
collection = db[collection_name]

# load data from ./data/aspen/tripadvisor-hotels/ids.jsonl
location_ids_list_file = collection_data["location_ids_list_file"]
with open(location_ids_list_file, 'r') as f:
    location_ids = [json.loads(line) for line in f]

headers = {"accept": "application/json"}
location_ids = location_ids[:args.limit] if args.limit else location_ids
for location_id in location_ids:
  url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details?key={TRIPADVISOR_API_KEY}"
  response = requests.get(url, headers=headers)
  document = json.loads(response.text)
  # print(document)

  if "error" in document:
     print(f"Error for location_id: {location_id}:\n {document}")
     continue
  
  # If photos are requested, fetch them
  if args.with_photos:
      try:
          # Fetch photos for this location, using the limit parameter
          photos_url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/photos?language=en&limit={args.photos_per_location}&key={TRIPADVISOR_API_KEY}"
          photos_response = requests.get(photos_url, headers=headers)
          photos_data = json.loads(photos_response.text)
          
          # Check if we got valid photos data
          if "data" in photos_data and isinstance(photos_data["data"], list):
              # Save all photos returned by the API (already limited)
              document["photos"] = photos_data["data"]
              print(f"Added {len(photos_data['data'])} photos for location_id: {location_id}")
          else:
              if "error" in photos_data:
                  print(f"Error fetching photos for location_id {location_id}: {photos_data['error']}")
              else:
                  print(f"No photos found for location_id {location_id}")
          
      except Exception as e:
          print(f"Exception fetching photos for location_id {location_id}: {str(e)}")

  # upsert into mongo
  result = collection.update_one(
    {"location_id": location_id},
    {"$set": document},
    upsert=True
  )
  print(f"Result for location_id {location_id}: {result}\n")

client.close()
