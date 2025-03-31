from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
from dotenv import load_dotenv
import pprint

# Load environment variables from .env file
load_dotenv()

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
    print("Pinged your deployment. You successfully connected to MongoDB!")

    # Access the viammo-alpha database and trips collection
    db = client["viammo-alpha"]
    collection = db["trips"]

    # Retrieve the first 10 documents from the collection
    documents = collection.find().limit(10)

    # Display the documents
    print("\nFirst 10 documents in the trips collection:")
    print("="*50)
    
    pp = pprint.PrettyPrinter(indent=2)
    for i, doc in enumerate(documents, 1):
        print(f"\nDocument {i}:")
        pp.pprint(doc)
        print("-"*50)

    print("\nTotal documents displayed: 10")

except Exception as e:
    print(f"An error occurred: {e}")

finally:
    # Close the MongoDB connection
    client.close()
    print("MongoDB connection closed.")
