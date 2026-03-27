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


GOOGLE_MAPS_KEY = os.environ.get("GOOGLE_MAPS_KEY", "")

VENUE_FILES = [
    "comedy_clubs.csv", "improv.csv", "live_music.csv",
    "museums.csv", "bar_events.csv", "board_games.csv", "escape_rooms.csv",
]


def load_venue_data():
    """Build name -> {website, photoUrl} lookup from all venue CSVs."""
    lookup = {}
    for fname in VENUE_FILES:
        path = os.path.join(OUTPUT_FOLDER, fname)
        if not os.path.exists(path):
            continue
        df = pl.read_csv(path)
        has_photo = "Photo_URL" in df.columns
        cols = ["Name", "Website"] + (["Photo_URL"] if has_photo else [])
        for row in df.select(cols).to_dicts():
            name = row.get("Name", "")
            if not name:
                continue
            raw_photo = row.get("Photo_URL", "") if has_photo else ""
            # Places photo URLs need /media?maxWidthPx=800&key= appended
            if raw_photo and not raw_photo.endswith("N/A") and "/media" not in raw_photo:
                raw_photo = f"{raw_photo}/media?maxWidthPx=800&key={GOOGLE_MAPS_KEY}"
            lookup[name] = {
                "website": row.get("Website", "") or "",
                "photoUrl": raw_photo or "",
            }
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

    venue_data = load_venue_data()

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
                    pl.col("name").map_elements(
                        lambda n: venue_data.get(n, {}).get("website", ""), return_dtype=pl.Utf8
                    )
                )
                .alias("ticketUrl")
            )
        else:
            df = df.with_columns(
                pl.col("name").map_elements(
                    lambda n: venue_data.get(n, {}).get("website", ""), return_dtype=pl.Utf8
                ).alias("ticketUrl")
            )

        # Add photoUrl from venue data
        df = df.with_columns(
            pl.col("name").map_elements(
                lambda n: venue_data.get(n, {}).get("photoUrl", ""), return_dtype=pl.Utf8
            ).alias("photoUrl")
        )

        # Add mapsEmbedUrl from address
        if GOOGLE_MAPS_KEY:
            import urllib.parse
            df = df.with_columns(
                pl.col("address").map_elements(
                    lambda a: f"https://www.google.com/maps/embed/v1/place?key={GOOGLE_MAPS_KEY}&q={urllib.parse.quote(a)}",
                    return_dtype=pl.Utf8
                ).alias("mapsEmbedUrl")
            )
        else:
            df = df.with_columns(pl.lit("").alias("mapsEmbedUrl"))

        # Keep only the columns we need
        keep = ["name", "event", "description", "cost", "date", "time", "address", "category", "ticketUrl", "photoUrl", "mapsEmbedUrl"]
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
