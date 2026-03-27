import os
import json
import polars as pl
import anthropic

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
INPUT_FOLDER = os.path.join(PROJECT_DIR, "input_folder")
OUTPUT_FOLDER = os.path.join(PROJECT_DIR, "output_folder")

BATCH_SIZE = 30

# Load API keys
api_keys = {}
with open(os.path.join(INPUT_FOLDER, "api_keys.txt")) as f:
    for line in f:
        if "=" in line:
            key, val = line.split("=", 1)
            api_keys[key.strip()] = val.strip().strip('"')

client = anthropic.Anthropic(api_key=api_keys["anthropic_api"])

# Load events
events_df = pl.scan_csv(os.path.join(OUTPUT_FOLDER, "events.csv")).collect()
events = events_df.to_dicts()

print(f"Loaded {len(events)} events. Generating keywords in batches of {BATCH_SIZE}...\n")


def generate_keywords_batch(batch):
    """Send a batch of events to Claude and return a list of keyword strings."""
    lines = []
    for i, event in enumerate(batch):
        lines.append(
            f"{i+1}. Venue: \"{event.get('Name', '')}\" | "
            f"Event: \"{event.get('Event', '')}\" | "
            f"Description: \"{event.get('Description', 'N/A')}\" | "
            f"Cost: {event.get('Cost', 'N/A')}"
        )

    prompt = (
        "For each event below, generate 8-12 relevant keywords that someone might search for.\n\n"
        "Include:\n"
        "- Genre/type keywords (music, comedy, trivia, etc.)\n"
        "- Mood keywords (social, artsy, fun, casual, fancy, etc.)\n"
        "- Location keywords (downtown, upper east side, midtown, etc.)\n"
        "- Vibe keywords (rowdy, chill, intimate, loud, interactive, etc.)\n"
        "- Time keywords (weeknight, weekend, evening, late night, etc.)\n\n"
        "Events:\n"
        + "\n".join(lines)
        + "\n\n"
        "Return ONLY a JSON array of strings — one comma-separated keyword string per event, in the same order.\n"
        'Example: ["live music, indie rock, concert, bar, social, evening", "trivia, bar games, fun, weekly"]\n'
        "No other text."
    )

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()

    # Clean markdown fencing if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1]
        text = text.rsplit("```", 1)[0]
    text = text.strip()

    return json.loads(text)


all_keywords = []
total_batches = (len(events) + BATCH_SIZE - 1) // BATCH_SIZE

for batch_num in range(total_batches):
    start = batch_num * BATCH_SIZE
    end = min(start + BATCH_SIZE, len(events))
    batch = events[start:end]

    print(f"Batch {batch_num + 1}/{total_batches} (events {start + 1}-{end})...")

    try:
        keywords = generate_keywords_batch(batch)
        # Pad with empty string if Claude returned fewer items than expected
        while len(keywords) < len(batch):
            keywords.append("")
        all_keywords.extend(keywords[:len(batch)])
        print(f"  -> Done")
    except Exception as e:
        print(f"  -> ERROR: {e} — defaulting batch to empty keywords")
        all_keywords.extend([""] * len(batch))

# Add keywords column and save
result_df = events_df.with_columns(
    pl.Series("Keywords", all_keywords)
)

os.makedirs(OUTPUT_FOLDER, exist_ok=True)
result_df.write_csv(os.path.join(OUTPUT_FOLDER, "events_with_keywords.csv"))

print(f"\nSaved {len(events)} events with keywords to output_folder/events_with_keywords.csv")
