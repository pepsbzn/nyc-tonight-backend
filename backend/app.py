from flask import Flask, jsonify, request
from flask_cors import CORS
import polars as pl
import os
import json

app = Flask(__name__)
CORS(app, origins=["*"])  # Allow all origins for Lovable/Railway

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_FOLDER = os.path.join(PROJECT_DIR, "output_folder")

_cache = {}


def load_venue_websites():
    """Build a name->website lookup from all venue CSVs."""
    venue_files = [
        "comedy_clubs.csv", "improv.csv", "live_music.csv",
        "museums.csv", "bar_events.csv", "board_games.csv", "escape_rooms.csv",
    ]
    lookup = {}
    for fname in venue_files:
        path = os.path.join(OUTPUT_FOLDER, fname)
        if not os.path.exists(path):
            continue
        df = pl.scan_csv(path).select(["Name", "Website"]).collect()
        for row in df.to_dicts():
            name = row.get("Name", "")
            website = row.get("Website", "")
            if name and website and website not in ("N/A", ""):
                lookup[name] = website
    return lookup


def load_events():
    cache_key = "all_events"

    # Check if any source file changed
    files = [
        ("comedy_clubs_events_openai.csv", "comedy"),
        ("improv_events.csv", "improv"),
        ("live_music_events.csv", "live_music"),
        ("museums_events.csv", "museum"),
        ("bar_events_events.csv", "bar"),
        ("board_games_events.csv", "board_games"),
        ("escape_rooms_events.csv", "escape_room"),
    ]

    mtimes = {}
    for fname, _ in files:
        path = os.path.join(OUTPUT_FOLDER, fname)
        if os.path.exists(path):
            mtimes[fname] = os.path.getmtime(path)

    if cache_key in _cache and _cache[cache_key]["mtimes"] == mtimes:
        return _cache[cache_key]["data"]

    venue_websites = load_venue_websites()

    frames = []
    for fname, category in files:
        path = os.path.join(OUTPUT_FOLDER, fname)
        if not os.path.exists(path):
            continue
        df = pl.scan_csv(path).collect()
        df = df.with_columns(pl.lit(category).alias("category"))

        # Normalize column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})

        # Use scraped ticket_url if available, else fall back to venue website
        if "ticket_url" in df.columns:
            df = df.with_columns(
                pl.when(
                    pl.col("ticket_url").is_not_null() &
                    (pl.col("ticket_url") != "") &
                    (pl.col("ticket_url") != "N/A")
                )
                .then(pl.col("ticket_url"))
                .otherwise(
                    pl.col("name").map_elements(lambda n: venue_websites.get(n, ""), return_dtype=pl.Utf8)
                )
                .alias("ticketUrl")
            )
        else:
            df = df.with_columns(
                pl.col("name").map_elements(
                    lambda n: venue_websites.get(n, ""), return_dtype=pl.Utf8
                ).alias("ticketUrl")
            )

        # Keep only the columns we need
        keep = ["name", "event", "description", "cost", "date", "time", "address", "category", "ticketUrl"]
        for col in keep:
            if col not in df.columns:
                df = df.with_columns(pl.lit("").alias(col))
        df = df.select(keep)

        frames.append(df)

    if not frames:
        return []

    combined = pl.concat(frames, how="diagonal")

    # Add an integer id column
    combined = combined.with_row_index("id", offset=1)

    # Drop rows with no event name
    combined = combined.filter(
        pl.col("event").is_not_null() & (pl.col("event") != "") & (pl.col("event") != "N/A")
    )

    # Cast id to int (already is, but be safe)
    result = combined.to_dicts()

    _cache[cache_key] = {"mtimes": mtimes, "data": result}
    return result


@app.route("/api/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/events")
def get_events():
    page = request.args.get("page", 1, type=int)
    page_size = request.args.get("page_size", 50, type=int)
    category = request.args.get("category", None)
    search = request.args.get("search", None)
    date = request.args.get("date", None)

    all_events = load_events()

    # Filter
    filtered = all_events
    if category and category != "all":
        filtered = [e for e in filtered if e.get("category") == category]
    if date:
        filtered = [e for e in filtered if e.get("date") == date]
    if search:
        q = search.lower()
        filtered = [
            e for e in filtered
            if q in (e.get("event") or "").lower()
            or q in (e.get("name") or "").lower()
            or q in (e.get("description") or "").lower()
        ]

    total = len(filtered)
    offset = (page - 1) * page_size
    page_data = filtered[offset: offset + page_size]

    return jsonify({
        "events": page_data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size
    })


@app.route("/api/events/<int:event_id>")
def get_event(event_id):
    all_events = load_events()
    event = next((e for e in all_events if e.get("id") == event_id), None)
    if not event:
        return jsonify({"error": "Not found"}), 404
    return jsonify(event)


@app.route("/api/categories")
def get_categories():
    return jsonify([
        {"id": "comedy", "label": "Comedy"},
        {"id": "improv", "label": "Improv"},
        {"id": "live_music", "label": "Live Music"},
        {"id": "museum", "label": "Museums"},
        {"id": "bar", "label": "Bar Events"},
        {"id": "board_games", "label": "Board Games"},
        {"id": "escape_room", "label": "Escape Rooms"},
    ])


if __name__ == "__main__":
    print("Loading events...")
    events = load_events()
    print(f"Loaded {len(events)} events.")
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
