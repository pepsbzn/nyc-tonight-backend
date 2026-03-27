import os
import json
import time
import polars as pl
import urllib.request

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
INPUT_FOLDER = os.path.join(PROJECT_DIR, "input_folder")
OUTPUT_FOLDER = os.path.join(PROJECT_DIR, "output_folder")

# Load API keys
api_keys = {}
with open(os.path.join(INPUT_FOLDER, "api_keys.txt")) as f:
    for line in f:
        if "=" in line:
            key, val = line.split("=", 1)
            api_keys[key.strip()] = val.strip().strip('"')

google_key = api_keys["google_places"]

# New Places API (v1)
SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
DETAILS_URL = "https://places.googleapis.com/v1/places/{place_id}"
SEARCH_FIELD_MASK = "places.id,places.displayName,places.formattedAddress,places.rating,places.userRatingCount,places.priceLevel"
DETAILS_FIELD_MASK = "websiteUri"

queries = [
    "live music venues in Manhattan, New York",
    "live bands bars in Manhattan, New York",
    "jazz bars in Manhattan, New York",
    "jazz clubs in Manhattan, New York",
    "concert halls in Manhattan, New York",
    "music venues in Manhattan, New York",
    "blues bars in Manhattan, New York",
    "rock venues in Manhattan, New York",
]

seen_place_ids = set()
all_places = []


def search_query(query):
    """Run a text search query using the new Places API."""
    body = {"textQuery": query}
    count = 0

    while True:
        data_bytes = json.dumps(body).encode("utf-8")
        req = urllib.request.Request(
            SEARCH_URL,
            data=data_bytes,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": google_key,
                "X-Goog-FieldMask": SEARCH_FIELD_MASK,
            },
            method="POST",
        )
        with urllib.request.urlopen(req) as resp:
            data = json.loads(resp.read())

        for place in data.get("places", []):
            place_id = place.get("id", "")
            if place_id in seen_place_ids:
                continue
            seen_place_ids.add(place_id)
            all_places.append({
                "Name": place.get("displayName", {}).get("text", ""),
                "Address": place.get("formattedAddress", ""),
                "Rating": place.get("rating", "N/A"),
                "Total Ratings": place.get("userRatingCount", "N/A"),
                "Price Level": place.get("priceLevel", "N/A"),
                "Place ID": place_id,
            })
            count += 1

        next_token = data.get("nextPageToken")
        if not next_token:
            break

        time.sleep(2)
        body = {"textQuery": query, "pageToken": next_token}

    return count


def fetch_website(place_id):
    """Fetch website URL for a place using the new Places API."""
    url = DETAILS_URL.format(place_id=place_id)
    req = urllib.request.Request(
        url,
        headers={
            "X-Goog-Api-Key": google_key,
            "X-Goog-FieldMask": DETAILS_FIELD_MASK,
        },
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())
    return data.get("websiteUri", "N/A")


# Run all searches
for q in queries:
    print(f"Searching: {q}")
    found = search_query(q)
    print(f"  +{found} new (total: {len(all_places)})")
    time.sleep(0.5)

# Filter: min 100 ratings, sort by rating desc, take top 100
all_places = [p for p in all_places if isinstance(p.get("Total Ratings"), (int, float)) and p["Total Ratings"] >= 100]
all_places = sorted(all_places, key=lambda p: p.get("Rating", 0), reverse=True)
all_places = all_places[:100]

print(f"\nAfter filtering: {len(all_places)} venues (min 100 ratings, top 100 by rating)")
print("Fetching website URLs...")

# Fetch website URLs
for i, place in enumerate(all_places):
    try:
        place["Website"] = fetch_website(place["Place ID"])
    except Exception:
        place["Website"] = "N/A"
    if (i + 1) % 20 == 0:
        print(f"  {i + 1}/{len(all_places)} websites fetched")

df = pl.DataFrame(all_places)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
df.write_csv(os.path.join(OUTPUT_FOLDER, "live_music.csv"))

print(f"\nSaved {len(all_places)} live music venues to output_folder/live_music.csv")
for row in df.to_dicts():
    print(f"  {row['Name']} | {row['Website']}")
