
import os
import requests
import json
from dotenv import load_dotenv

from datetime import datetime
from supabase import create_client, Client

# Load environment variables from .env file
load_dotenv()

# Supabase setup
url: str = os.environ.get("SUPABASE_URL")
key: str = os.environ.get("SUPABASE_KEY")
print(f"URL: '{url}'")
print(f"KEY: '{key}'")
supabase: Client = create_client(url, key)

TRIPADVISOR_API_KEY = os.getenv("TRIPADVISOR_API_KEY")
ta_headers = {"accept": "application/json"}

aspen_location_ids = {'577616', '306603', '1163774', '8491710', '82759', '663657', '82743', '82751', '622353', '663891', '260392', '2417338', '82754', '582207', '209075', '23264318', '282485', '278976', '22951026', '1595848', '223743', '1867550', '23962513', '76944', '1146106', '278099', '119955', '21268470', '6498936', '295766', '82776', '209406', '120018', '26828936', '1158531', '1047486', '264848', '224728', '379343', '10816706', '110816', '85870', '567149', '122398', '83481', '15182489', '186795', '282482', '82756', '572247', '28102257', '205850', '22861958', '17479882', '32966789', '310478', '252948', '82737', '125821', '82729', '601490', '120769', '82770', '253316', '21292532', '25942678', '1230421', '585324', '614028', '125822', '276030', '6495343', '278096', '638104', '7375440', '82766', '1230583', '254631', '287948', '670108', '572215', '85618', '89772', '252945', '663408', '78181', '74314', '24095411', '248985', '7592699', '23593486', '88177', '115584', '142253', '27716312', '3575356', '6988965', '23996995', '90255', '25410463', '82741', '82758', '85076', '120027', '272992', '23256321', '85619', '300705', '84083', '25181430', '6420746', '261939', '23275424', '252947', '218275', '115118', '302200', '1910537', '22951038', '21314126', '16493435', '3783835', '4743943', '559455', '559456', '324443', '26826414', '23148830', '120020', '27716614', '85077', '13980722', '1911148', '19525668', '82773', '82762', '1534871', '24991645', '74313', '98606', '238826', '82749', '15225303', '21401110', '82763', '1916605', '24093475', '577618', '261938', '119956', '278097', '23469488', '302157', '18031954', '1210740', '1730530'}

# Save to Supabase
print(f"\nSaving {len(aspen_location_ids)} hotel IDs to Supabase table 'tripadvisor-hotel-info'...")

# Prepare data for insertion
current_time = datetime.now().isoformat()

data_to_insert = []

for location_id in aspen_location_ids:
  url = f"https://api.content.tripadvisor.com/api/v1/location/{location_id}/details?key={TRIPADVISOR_API_KEY}"
  response = requests.get(url, headers=ta_headers)
  document = json.loads(response.text)
  record = {
    "location_id": location_id, 
    "info": document,
  }
  data_to_insert.append(record)

# try:
# Insert data into Supabase table
result = supabase.table("tripadvisor-hotel-info-v0").upsert(data_to_insert).execute()
print(f"Upserted {len(data_to_insert)} records")

# Check for errors in the response
if hasattr(result, 'error') and result.error:
    print(f"Warning: {result.error}")
# except Exception as e:
#     print(f"Error saving to Supabase: {str(e)}")

