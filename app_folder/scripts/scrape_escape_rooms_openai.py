import os
import json
import datetime
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

today = datetime.date.today().strftime("%Y-%m-%d")

# Load escape rooms with websites
venues_df = pl.scan_csv(os.path.join(OUTPUT_FOLDER, "escape_rooms.csv")).collect()
venues = venues_df.filter(
    (pl.col("Website") != "N/A") &
    (pl.col("Website") != "")
).select("Name", "Address", "Website").to_dicts()

print(f"Scraping {len(venues)} escape room(s) via OpenAI (concurrent).\n")


def scrape_venue(venue):
    """Scrape a single venue. Returns one entry per room/experience."""
    name = venue["Name"]
    address = venue["Address"]
    url = venue["Website"]

    try:
        msg = f"Go to {url} and find all the escape room experiences available at this venue."
        msg += " Look for a 'Rooms', 'Experiences', 'Book', or 'Book Now' page."
        msg += f"\n\nToday is {today}. We want ONE entry per distinct room/experience (not per time slot)."
        msg += "\n\nFor each room, return:"
        msg += f'\n- "name": Venue name (use "{name}")'
        msg += '\n- "event": Room or experience name (e.g. "The Lost Tomb")'
        msg += '\n- "description": 1-2 sentences describing the room theme and story'
        msg += '\n- "cost": Price per person if listed (just the number, e.g. "35"), otherwise "N/A"'
        msg += '\n- "duration": Length of the experience (e.g. "60 min"), or "N/A"'
        msg += '\n- "difficulty": Difficulty level if listed (e.g. "Hard", "3/5"), or "N/A"'
        msg += f'\n- "date": Use today\'s date: {today}'
        msg += '\n- "time": Typical start time for evening sessions (e.g. "6:00 PM"), or "Various times"'
        msg += f'\n- "address": "{address}"'
        msg += '\n- "ticket_url": The direct booking URL for this room. Look for a "Book Now" or "Reserve" button. Use the venue homepage if no direct link exists.'
        msg += "\n\nReturn ONLY a valid JSON array, one object per room. No explanations, no markdown."
        msg += "\nIf no rooms found, return exactly: []"

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
            print(f"  {name} -> No rooms found")
        else:
            all_events.extend(result)
            print(f"  {name} -> {len(result)} rooms found")

print(f"\nTotal rooms collected: {len(all_events)}")

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
        elif lower == "duration":
            col_map[col] = "Duration"
        elif lower == "difficulty":
            col_map[col] = "Difficulty"
        elif lower == "ticket_url":
            col_map[col] = "Ticket_URL"
    df = df.rename(col_map)

    for col in ["Name", "Event", "Description", "Cost", "Duration", "Difficulty", "Date", "Time", "Address", "Ticket_URL"]:
        if col not in df.columns:
            df = df.with_columns(pl.lit("N/A").alias(col))

    df = df.select(["Name", "Event", "Description", "Cost", "Duration", "Difficulty", "Date", "Time", "Address", "Ticket_URL"])

    os.makedirs(OUTPUT_FOLDER, exist_ok=True)
    df.write_csv(os.path.join(OUTPUT_FOLDER, "escape_rooms_events.csv"))
    print(f"Saved to output_folder/escape_rooms_events.csv")
    for row in df.to_dicts():
        line = f"  {row['Name']} | {row['Event']} | {row['Cost']}"
        print(line.encode("utf-8", errors="replace").decode("utf-8"))
else:
    print("No rooms found.")
