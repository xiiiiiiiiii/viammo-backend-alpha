# Usage:
# 
# 1 - dump source html files from each page:
# From https://www.tripadvisor.com/Hotels-g29141-Aspen_Colorado-Hotels.html
# To ./data/aspen/tripadvisor-hotels/*.html
# 
# 2 - run:
# uv run ./data/extract_ids.py ./data/aspen/tripadvisor-restaurant_review/*.html --type restaurant_review --output ./data/aspen/tripadvisor-hotel_review/ids.jsonl
# 

import re
import argparse
import json

# Set up command-line argument parsing
parser = argparse.ArgumentParser(description='Extract location IDs from TripAdvisor HTML files.')
parser.add_argument('html_files', nargs='+', help='Paths to the HTML files to process')
parser.add_argument('--output', default='tripadvisor_location_ids.jsonl', help='Output JSONL file path')
parser.add_argument('--type', choices=['hotel_review', 'restaurant_review'], required=True,
                    help='Type of location IDs to extract (exactly one)')
args = parser.parse_args()


# Define the regex patterns to find TripAdvisor location URLs

# Hotels:
# From https://www.tripadvisor.com/Hotels-g29141-Aspen_Colorado-Hotels.html
# e.g. https://www.tripadvisor.com/Hotel_Review-g29141-d120018-Reviews-The_St_Regis_Aspen_Resort-Aspen_Colorado.html

# Restaurants:
# From https://www.tripadvisor.com/Restaurants-g29141-Aspen_Colorado.html
# e.g. https://www.tripadvisor.com/Restaurant_Review-g29141-d2523557-Reviews-French_Alpine_Bistro_Creperie_du_Village-Aspen_Colorado.html

patterns = {
    'hotel_review': re.compile(r'/Hotel_Review-g\d+-d(\d+)-Reviews-[^"\s]+\.html'),      # Hotels
    'attraction_product_review': re.compile(r'/AttractionProductReview-g\d+-d(\d+)-[^"\s]+\.html'),  # Attractions
    'restaurant_review': re.compile(r'/Restaurant_Review-g\d+-d(\d+)-Reviews-[^"\s]+\.html')   # Restaurants
}

test_urls = {
    'hotel_review': {
        'html': """
        u20AC\u20AC\u20AC"}},{"@type":"ListItem","position":4,"item":{"@type":"Hotel","name":"The St. Regis Aspen Resort","address":{"@type":"PostalAddress","streetAddress":"315 East Dean Street","addressLocality":"Aspen","addressRegion":"Colorado","addressCountry":"United States","postalCode":"81611"},"telephone":"+1 970-920-3300","image":"https:\u002F\u002Fdynamic-media-cdn.tripadvisor.com\u002Fmedia\u002Fphoto-o\u002F2e\u002Fd4\u002Ff2\u002F89\u002Fwinter-exterior.jpg?w=1200&h=-1&s=1","url":"https://www.tripadvisor.com/Hotel_Review-g29141-d120018-Reviews-The_St_Regis_Aspen_Resort-Aspen_Colorado.html","aggregateRating":{"@type":"AggregateRating","ratingValue":4.5,"reviewCount":1325},"priceRange":"\u20AC\u20AC\u20AC"}},{"@type":"ListItem","position":5,"item":{"@type":"Hotel","name":"W Aspen","address":{"@type":"PostalAddress","streetAddress":"550 South Spring Street","addressLocality":"Aspen","addressRegion":"Colorado","addressCountry":"United States","postalCode":"81611"},"telephone":"+1 970-431-0800",
        """,
        'id': '120018'
    },
    'restaurant_review': {
        'html': """
        an><span class="html-tag">&lt;span&gt;</span><span class="html-tag">&lt;a <span class="html-attribute-name">href</span>="<a class="html-attribute-value html-external-link" target="_blank" href="https://www.tripadvisor.com/Restaurant_Review-g29141-d2523557-Reviews-French_Alpine_Bistro_Creperie_du_Village-Aspen_Colorado.html" rel="noreferrer noopener">/Restaurant_Review-g29141-d2523557-Reviews-French_Alpine_Bistro_Creperie_du_Village-Aspen_Colorado.html</a>" <span class="html-attribute-name">class</span>="<span class="html-attribute-value">aWhIG _S _Z</span>" <span class="html-attribute-name">target</span>="<sp
        """,
        'id': '2523557'
    }
}

# Check extraction works.
for pattern_type, url in test_urls.items():
    match = patterns[pattern_type].findall(test_urls[pattern_type]['html'])
    assert match[0] == test_urls[pattern_type]['id']

# Get the selected pattern
selected_type = args.type
selected_pattern = patterns[selected_type]

print(f"Extracting {selected_type} IDs from {len(args.html_files)} HTML files...")

# Initialize a set to store all unique IDs across all files
all_location_ids = set()

# Process each HTML file
for file_path in args.html_files:
    try:
        print(f"Processing {file_path}...")
        
        # Read the HTML file
        with open(file_path, 'r', encoding='utf-8') as file:
            html_content = file.read()

        # Find all matches in the HTML content for the selected pattern
        matches = selected_pattern.findall(html_content)
        file_location_ids = set(matches)
        
        # Add these IDs to our total set
        all_location_ids.update(file_location_ids)
        
        # Print file-specific stats
        print(f"  Found {len(file_location_ids)} {selected_type} IDs in this file")
    
    except Exception as e:
        print(f"Error processing {file_path}: {str(e)}")

# Print the final result with all extracted location IDs
print(f"\nTotal unique {selected_type} IDs found across all files: {len(all_location_ids)}")
# print(all_location_ids)

# Save IDs to JSONL file
try:
    # Convert set to sorted list for consistent output
    ids_list = sorted(list(all_location_ids))
    with open(args.output, 'w') as f:
        for location_id in ids_list:
            json.dump(location_id, f)
            f.write('\n')
    print(f"\nSaved {len(ids_list)} {selected_type} IDs to {args.output}")
except Exception as e:
    print(f"Error saving to JSONL file: {str(e)}")

# Verify saved IDs by reloading them
try:
    print(f"\nVerifying saved IDs by reloading from {args.output}...")
    reloaded_ids = set()
    with open(args.output, 'r') as f:
        for line in f:
            location_id = json.loads(line.strip())
            reloaded_ids.add(location_id)
    
    # Compare original and reloaded sets
    if reloaded_ids == all_location_ids:
        print(f"✓ Verification successful! All {selected_type} IDs were saved and reloaded correctly.")
        print(f"  Original count: {len(all_location_ids)}")
        print(f"  Reloaded count: {len(reloaded_ids)}")
        # print(reloaded_ids)
    else:
        print(f"⚠ Verification failed! Some {selected_type} IDs were lost or changed during save/reload.")
        print(f"  Original count: {len(all_location_ids)}")
        print(f"  Reloaded count: {len(reloaded_ids)}")
        print(f"  Missing IDs: {all_location_ids - reloaded_ids}")
        print(f"  Extra IDs: {reloaded_ids - all_location_ids}")
except Exception as e:
    print(f"Error verifying saved IDs: {str(e)}")

