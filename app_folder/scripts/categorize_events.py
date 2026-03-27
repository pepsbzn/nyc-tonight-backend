import os
import json
import polars as pl
import anthropic

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
INPUT_FOLDER = os.path.join(PROJECT_DIR, "input_folder")
OUTPUT_FOLDER = os.path.join(PROJECT_DIR, "output_folder")

VALID_CATEGORIES = {"music", "comedy", "trivia", "karaoke", "escape_room", "board_game", "museum", "other"}
BATCH_SIZE = 30

# Load API keys
api_keys = {}
with open(os.path.join(INPUT_FOLDER, "api_keys.txt")) as f:
    for line in f:
        if "=" in line:
            key, val = line.split("=", 1)
            api_keys[key.strip()] = val.strip().strip('"')

client = anthropic.Anthropic(api_key=api_keys["anthropic_api"])

# Load events with keywords
events_df = pl.scan_csv(os.path.join(OUTPUT_FOLDER, "events_with_keywords.csv")).collect()
events = events_df.to_dicts()

print(f"Loaded {len(events)} events. Categorizing in batches of {BATCH_SIZE}...\n")


def categorize_batch(batch):
    """Send a batch of events to Claude and return a list of category strings."""
    lines = []
    for i, event in enumerate(batch):
        lines.append(
            f"{i+1}. Venue: \"{event.get('Name', '')}\" | "
            f"Event: \"{event.get('Event', '')}\" | "
            f"Description: \"{event.get('Description', 'N/A')}\" | "
            f"Keywords: \"{event.get('Keywords', '')}\""
        )

    prompt = (
        "Classify each event into exactly ONE category from this list:\n"
        "music, comedy, trivia, karaoke, escape_room, board_game, museum, other\n\n"
        "Events:\n"
        + "\n".join(lines)
        + "\n\n"
        "Return ONLY a JSON array of category strings in the same order as the events above.\n"
        'Example: ["music", "comedy", "trivia"]\n'
        "No other text."
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=512,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()

    # Clean markdown fencing if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    categories = json.loads(text)

    # Validate and fallback to "other" for unexpected values
    return [c if c in VALID_CATEGORIES else "other" for c in categories]


all_categories = []
total_batches = (len(events) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(total_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(events))
    batch = events[start:end]

    print(f"Batch {batch_num + 1}/{total_batches} (events {start + 1}-{end})...")

    try:
        categories = categorize_batch(batch)
        while len(categories) < len(batch):
            categories.append("other")
        all_categories.extend(categories[:len(batch)])
        print(f"  -> Done")
    except Exception as e:
        print(f"  -> ERROR: {e} — defaulting batch to 'other'")
        all_categories.extend(["other"] * len(batch))

# Add category column and save
result_df = events_df.with_columns(
    pl.Series("Category", all_categories)
)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
result_df.write_csv(os.path.join(OUTPUT_FOLDER, "events_categorized.csv"))

print(f"\nSaved {len(events)} categorized events to output_folder/events_categorized.csv")
print("\nCategory breakdown:")
print(result_df.group_by("Category").len().sort("len", descending=True))
