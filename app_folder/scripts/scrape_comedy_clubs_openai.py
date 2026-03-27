import os
import json
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed
from openai import OpenAI

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

client = OpenAI(api_key=api_keys["open_ai_api"])

# Load comedy clubs with websites
venues_df = pl.scan_csv(os.path.join(OUTPUT_FOLDER, "comedy_clubs.csv")).collect()
venues = venues_df.filter(
    (pl.col("Website") != "N/A") &
    (pl.col("Website") != "")
).select("Name", "Address", "Website").to_dicts()

print(f"Scraping {len(venues)} comedy club(s) via OpenAI (concurrent).\n")


def scrape_venue(venue):
    """Scrape a single venue. Returns list of event dicts."""
    name = venue["Name"]
    address = venue["Address"]
    url = venue["Website"]

    try:
        msg = f"Go to {url} and find their calendar or events page."
        msg += f" Extract all upcoming events/shows in the next 14 days (today is 2026-03-26)."
        msg += "\n\nReturn a JSON array with these exact fields:"
        msg += f'\n- "name": Venue name (use "{name}")'
        msg += '\n- "event": Event/show name (comedian name, show title, etc.)'
        msg += '\n- "description": What the event is (1-2 sentences)'
        msg += '\n- "cost": Price if listed, otherwise "N/A"'
        msg += '\n- "date": Date in YYYY-MM-DD format'
        msg += '\n- "time": Start time (e.g. "8:00 PM"), or "N/A"'
        msg += f'\n- "address": "{address}"'
        msg += "\n\nOnly include events in the next 14 days."
        msg += "\nOnly include events that start at 4:00 PM or later."
        msg += "\nReturn ONLY a valid JSON array. No explanations, no markdown."
        msg += "\nIf no events found, return exactly: []"

        response = client.responses.create(
            model="gpt-5-mini",
            tools=[{"type": "web_search_preview"}],
            input=msg,
        )

        # Extract text from response
        text = ""
        for item in response.output:
            if item.type == "message":
                for content in item.content:
                    if content.type == "output_text":
                        text = content.text.strip()

        # Strip markdown fencing if present
        if text.startswith("```"):
            text = text.split("\n", 1)[1]
            text = text.rsplit("```", 1)[0]
        text = text.strip()

        if not text:
            return name, []

        # Find the JSON array in the response
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1:
            text = text[start:end + 1]

        events = json.loads(text)
        return name, events

    except json.JSONDecodeError as e:
        return name, f"JSON error: {e}"
    except Exception as e:
        return name, f"ERROR: {e}"


# Run all venues concurrently (max 5 at a time)
all_events = []

with ThreadPoolExecutor(max_workers=5) as executor:
    futures = {executor.submit(scrape_venue, v): v for v in venues}

    for future in as_completed(futures):
        name, result = future.result()
        if isinstance(result, str):
            print(f"  {name} -> {result}")
        elif not result:
            print(f"  {name} -> No events found")
        else:
            all_events.extend(result)
            print(f"  {name} -> {len(result)} events found")

print(f"\nTotal events collected: {len(all_events)}")

if all_events:
    df = pl.DataFrame(all_events)

    # Standardize column names
    col_map = {}
    for col in df.columns:
        lower = col.lower()
        if lower == "name":
            col_map[col] = "Name"
        elif lower == "event":
            col_map[col] = "Event"
        elif lower == "description":
            col_map[col] = "Description"
        elif lower == "cost":
            col_map[col] = "Cost"
        elif lower == "date":
            col_map[col] = "Date"
        elif lower == "time":
            col_map[col] = "Time"
        elif lower == "address":
            col_map[col] = "Address"
    df = df.rename(col_map)

    for col in ["Name", "Event", "Description", "Cost", "Date", "Time", "Address"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit("N/A").alias(col))

    df = df.select(["Name", "Event", "Description", "Cost", "Date", "Time", "Address"])

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    df.write_csv(os.path.join(OUTPUT_FOLDER, "comedy_clubs_events_openai.csv"))
    print(f"Saved to output_folder/comedy_clubs_events_openai.csv")
    for row in df.to_dicts():
        print(f"  {row['Name']} | {row['Event']} | {row['Date']} {row['Time']} | {row['Cost']}")
else:
    print("No events found.")
