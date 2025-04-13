#!/usr/bin/env python3

import requests
import json
import os
import argparse
from dotenv import load_dotenv
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def get_viator_destinations():
    """
    Call the Viator destinations endpoint
        
    Returns:
        dict: JSON response from the Viator API
    """
    try:
        # API endpoint
        url = "https://api.viator.com/partner/destinations"
        
        # Headers
        headers = {
            "Accept": "application/json;version=2.0",
            "Accept-Language": "en-US",
            "exp-api-key": os.getenv("VIATOR_API_KEY")
        }
        
        # Make the request
        response = requests.get(url, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch destinations: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching destinations: {e}")
        return None

def get_viator_tags_en():
    """
    Call the Viator destinations endpoint
        
    Returns:
        dict: JSON response from the Viator API
    """
    try:
        # API endpoint
        url = "https://api.viator.com/partner/products/tags"
        
        # Headers
        headers = {
            "Accept": "application/json;version=2.0",
            "Accept-Language": "en-US",
            "exp-api-key": os.getenv("VIATOR_API_KEY")
        }
        
        # Make the request
        response = requests.get(url, headers=headers)
        
        # Check if request was successful
        if response.status_code == 200:
            tags = response.json()
            tags_en = {
                tag['tagId']: tag['allNamesByLocale']['en']
                for tag in tags['tags']
            }
            return tags_en
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch destinations: {e}")
        return None
    except Exception as e:
        print(f"Unexpected error fetching destinations: {e}")
        return None

def get_viator_products(destination_id, tags_en, page_size=50):
    """
    Call the Viator products search endpoint
        
    Args:
        destination_id (int): The destination ID to search for products
        page_size (int): Number of results per page (max 50)
        
    Returns:
        list: Combined list of all products from all pages
    """
    # API endpoint
    url = "https://api.viator.com/partner/products/search"
    
    # Headers
    headers = {
        "Accept": "application/json;version=2.0",
        "Accept-Language": "en-US",
        "exp-api-key": os.getenv("VIATOR_API_KEY"),
    }

    all_products = []
    start = 1
    total_count = None
    page = 1

    while total_count is None or start <= total_count:
        print(f"Fetching page {page} (results {start}-{start+page_size-1})...")
        
        payload = {
            "filtering": {
                "destination": str(destination_id)
            },
            "currency": "USD",
            "pagination": {
                "start": start,
                "count": page_size
            }
        }
        
        # Make the request
        response = requests.post(url, headers=headers, json=payload)
        
        # Check if request was successful
        if response.status_code == 200:
            data = response.json()
            products = data.get('products', [])

            # Add tag strings to products
            products = [
                {
                    **p,
                    'tags_str': [tags_en[tag] for tag in p['tags']],
                }
                for p in products
            ]

            all_products.extend(products)
            
            # Update total count if not already set
            if total_count is None:
                total_count = data.get('totalCount', 0)
                
            # If no products or reached the end, break
            if not products or len(products) < page_size:
                break
                
            # Move to next page
            start += page_size
            page += 1
        else:
            print(f"Error: {response.status_code}")
            print(response.text)
            return None
            
    print(f"Retrieved a total of {len(all_products)} products")
    return {"products": all_products, "totalCount": total_count or len(all_products)}


def save_to_mongodb(products_data, limit=None):
    """
    Save products data to MongoDB
    
    Args:
        products_data (list): List of product data
        limit (int, optional): Maximum number of products to save
        
    Returns:
        bool: True if successful, False otherwise
    """
    data_type = 'viator_product'
    collections_data = {
        'viator_product': {
            "collection_name": "viator-products",
        },
    }

    print(f"\nLoading data for {data_type} into {collections_data[data_type]['collection_name']}")
    collection_data = collections_data[data_type]

    if limit:
        print(f"Limiting to a maximum of {limit} products")
        products_data = products_data[:limit]

    try:
        # Get MongoDB credentials from environment variables
        username = os.getenv("MONGODB_USERNAME")
        password = os.getenv("MONGODB_PASSWORD")
        cluster = os.getenv("MONGODB_CLUSTER")
        
        if not username or not password or not cluster:
            print("Error: Missing MongoDB credentials in environment variables")
            return False

        # Construct MongoDB URI
        uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName=Viammo-Cluster-alpha"

        # Create a new client and connect to the server
        client = MongoClient(uri, server_api=ServerApi('1'), serverSelectionTimeoutMS=5000)

        # Send a ping to confirm a successful connection
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!\n")

        db = client["viammo-alpha"]
        collection_name = collection_data["collection_name"]
        collection = db[collection_name]

        for product in products_data:
            productCode = product['productCode']

            # upsert into mongo
            result = collection.update_one(
                {"productCode": productCode},
                {"$set": product},
                upsert=True
            )
            print(f"Result for productCode {productCode}: {result}\n")

        client.close()
        return True
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")
        return False


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Fetch and save Viator products for a location')
    parser.add_argument('--location', type=str, required=True, help='Location name to search for (e.g. "Aspen")')
    parser.add_argument('--limit', type=int, help='Limit the number of products to save')
    parser.add_argument('--page-size', type=int, default=50, help='Number of products per page (max 50)')
    args = parser.parse_args()
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Check for required environment variables
        if not os.getenv("VIATOR_API_KEY"):
            print("Error: VIATOR_API_KEY environment variable is missing")
            return
            
        # Get destinations
        print(f"Fetching Viator destinations...")
        destinations = get_viator_destinations()
        if not destinations:
            print("Failed to retrieve destinations. Please check your API key and network connection.")
            return
        
        print(f"Total destinations: {destinations['totalCount']}")
        
        # Find location by name
        location_name = args.location
        matching_locations = [
            d for d in destinations['destinations']
            if location_name.lower() in d['name'].lower()
        ]
        
        if not matching_locations:
            print(f"No locations found matching '{location_name}'")
            return
        
        print(f"Found {len(matching_locations)} locations matching '{location_name}':")
        for i, loc in enumerate(matching_locations):
            print(f"{i+1}. {loc['name']} (ID: {loc['destinationId']})")

        if len(matching_locations) > 1:
            print(f"Multiple locations found matching '{location_name}', stopping.")
            return
        
        # Use the first location.
        location = matching_locations[0]
        location_id = location['destinationId']
        print(f"\nUsing location: {location['name']} (ID: {location_id})")
        
        # Get products for the location
        print(f"Fetching products for {location['name']}...")
        
        # Adjust page size to be between 1 and 50
        page_size = max(1, min(50, args.page_size))
        if page_size != args.page_size:
            print(f"Adjusting page size to {page_size} (must be between 1 and 50)")
        
        tags_en = get_viator_tags_en()
        if not tags_en:
            print("Failed to retrieve tags. Please check your API key and network connection.")
            return
        
        products = get_viator_products(location_id, tags_en, page_size)
        if not products:
            print("Failed to retrieve products. Please check your API key and network connection.")
            return
        
        print(f"\nFound {len(products['products'])} products")
        for i, p in enumerate(products['products']):
            print(f"{i+1}. {p['title']} {p['tags_str']}")

        
        # Save to MongoDB
        if not save_to_mongodb(products['products'], args.limit):
            print("Failed to save products to MongoDB. Please check your database credentials.")
            return
            
        print("Process completed successfully!")
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


if __name__ == "__main__":
    main() 