import os
import json
import time
import anthropic
import polars as pl
from concurrent.futures import ThreadPoolExecutor, as_completed

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

client = anthropic.Anthropic(api_key=api_keys["anthropic_api"])

# Event CSV files and their category slugs
EVENT_FILES = {
    "comedy_clubs_events_openai.csv": "comedy",
    "improv_events.csv":              "improv",
    "live_music_events.csv":          "live_music",
    "museums_events.csv":             "museum",
    "bar_events_events.csv":          "bar_events",
    "board_games_events.csv":         "board_games",
    "escape_rooms_events.csv":        "escape_room",
}

TAXONOMY = """
CATEGORY SLUGS (always include the one matching this event):
comedy, improv, live_music, museum, escape_room, bar_events, board_games

APPROVED KEYWORD VOCABULARY:

Category keywords:
- comedy: comedy, stand-up, humor, laughs, comedian, improv, sketch, unscripted
- live_music: live_music, concert, band, performance, jazz, rock, indie, hip-hop, soul, blues, classical, electronic, folk, latin, salsa, funk, swing, acoustic
- museum: museum, art, gallery, exhibit, culture, history, science, photography, sculpture, painting, contemporary-art, interactive
- escape_room: escape_room, puzzle, challenge, teamwork, immersive, active, timed, brain-teaser, adventure, mission
- bar_events: bar_events, drinks, cocktails, nightlife, social, karaoke, trivia, DJ, open-mic, tasting, mixology, casual
- board_games: board_games, trivia, games, strategy, tabletop, nerdy, chill, card-games

Vibe keywords (pick 1-2 that fit):
intimate, energetic, chill, challenging, educational, interactive, late-night, immersive, rowdy, sophisticated, casual, loud, mellow, BYOB, free, affordable, upscale, dive-bar, rooftop, underground

Audience keywords (pick 1-2 that fit):
solo-friendly, date-night, groups, adults, families, couples, team-building, birthday, first-date

Free-form (2-3 words GPT picks based on the specific event — real searchable terms a user might type):
Examples: jazz-trio, improv-jam, wine-tasting, horror-escape, art-deco, spoken-word, drag-show, open-mic, trivia-night, comedy-roast, live-DJ
"""

SYSTEM_PROMPT = f"""You are tagging NYC events for a searchable database.
Given an event's category, name, and description, return 6-9 keywords.

Rules:
1. Always include the category slug as the FIRST keyword
2. Pick 2-3 keywords from the approved vocabulary that best fit
3. Pick 1-2 vibe keywords
4. Pick 1-2 audience keywords
5. Add 1-2 free-form keywords specific to this event that a real user would search
6. Never include venue names, addresses, prices, dates, or generic words like "event", "show", "NYC", "great"

{TAXONOMY}

Return ONLY a JSON array of strings. Example:
["comedy", "stand-up", "humor", "late-night", "intimate", "adults", "date-night"]"""


def generate_keywords_for_event(row, category_slug):
    """Call Claude to generate keywords for a single event."""
    name = row.get("Event") or row.get("Name") or ""
    venue = row.get("Name") or ""
    description = row.get("Description") or ""

    user_msg = f"""Category: {category_slug}
Venue: {venue}
Event: {name}
Description: {description}

Return keywords as a JSON array."""

    for attempt in range(3):
        try:
            response = client.messages.create(
                model="claude-haiku-4-5",
                max_tokens=200,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_msg}]
            )
            text = response.content[0].text.strip()
            start = text.find("[")
            end = text.rfind("]")
            if start != -1 and end != -1:
                keywords = json.loads(text[start:end + 1])
                return ", ".join(keywords)
            return category_slug
        except anthropic.RateLimitError:
            time.sleep(2 ** attempt)  # 1s, 2s, 4s backoff
        except Exception:
            return category_slug
    return category_slug


def process_file(filename, category_slug):
    """Add Keywords column to a single event CSV."""
    filepath = os.path.join(OUTPUT_FOLDER, filename)
    if not os.path.exists(filepath):
        print(f"  SKIP: {filename} not found")
        return

    df = pl.read_csv(filepath)

    # Only retry rows that only have the category slug (failed last time)
    needs_retry = "Keywords" not in df.columns or df.filter(pl.col("Keywords") == category_slug).height > 0
    if not needs_retry:
        print(f"\n{filename}: all keywords already complete, skipping")
        return

    rows = df.to_dicts()
    total = len(rows)
    retry_indices = [i for i, r in enumerate(rows) if r.get("Keywords", "") == category_slug or not r.get("Keywords")]
    print(f"\n{filename}: retrying {len(retry_indices)}/{total} events...")

    # Process only the ones that need retry
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(generate_keywords_for_event, rows[i], category_slug): i
            for i in retry_indices
        }
        results = {}
        for future in as_completed(futures):
            i = futures[future]
            results[i] = future.result()
            if (len(results) % 20) == 0:
                print(f"  {len(results)}/{len(retry_indices)} retried...")

    # Merge results back
    keywords_list = [r.get("Keywords", category_slug) for r in rows]
    for i, kw in results.items():
        keywords_list[i] = kw

    df = df.with_columns(pl.Series("Keywords", keywords_list))
    df.write_csv(filepath)
    print(f"  Done — saved {total} events to {filename}")


if __name__ == "__main__":
    print("Generating keywords for all event CSVs using Claude...\n")
    for filename, slug in EVENT_FILES.items():
        process_file(filename, slug)
    print("\nAll done!")
