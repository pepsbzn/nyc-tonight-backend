"""
Refresh All Events + Push to GitHub
=====================================
Run this script every two weeks to re-scrape all venues and push fresh data.

Usage:
    python app_folder/scripts/refresh_all.py
"""

import os
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))

SCRAPE_SCRIPTS = [
    "scrape_comedy_clubs_openai.py",
    "scrape_improv_openai.py",
    "scrape_live_music_openai.py",
    "scrape_museums_openai.py",
    "scrape_bar_events_openai.py",
    "scrape_board_games_openai.py",
    "scrape_escape_rooms_openai.py",
]


def run_script(script_name):
    path = os.path.join(SCRIPT_DIR, script_name)
    result = subprocess.run(
        ["python", path],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace"
    )
    # Pull the last line that has a count summary
    lines = [l for l in result.stdout.strip().splitlines() if l.strip()]
    summary = lines[-2] if len(lines) >= 2 else (lines[-1] if lines else "no output")
    return script_name, result.returncode, summary, result.stderr[-200:] if result.stderr else ""


def push_to_github():
    print("\n--- Pushing fresh data to GitHub ---")

    today = datetime.now().strftime("%Y-%m-%d")
    commit_msg = f"Refresh event data {today}"

    commands = [
        ["git", "add", "output_folder/"],
        ["git", "commit", "-m", commit_msg],
        ["git", "push"],
    ]

    for cmd in commands:
        result = subprocess.run(cmd, cwd=PROJECT_DIR, capture_output=True, text=True)
        label = " ".join(cmd[:2])
        if result.returncode == 0:
            print(f"  {label} -> OK")
        else:
            # git commit returns 1 if nothing to commit — that's fine
            output = result.stdout.strip() or result.stderr.strip()
            if "nothing to commit" in output:
                print(f"  {label} -> Nothing new to commit")
            else:
                print(f"  {label} -> ERROR: {output}")
                return False
    return True


if __name__ == "__main__":
    print("=" * 50)
    print(" NYC Events Refresh")
    print(f" Started: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print("=" * 50)
    print(f"\nRunning {len(SCRAPE_SCRIPTS)} scrapers concurrently...\n")

    results = {}
    with ThreadPoolExecutor(max_workers=7) as executor:
        futures = {executor.submit(run_script, s): s for s in SCRAPE_SCRIPTS}
        for future in as_completed(futures):
            name, code, summary, err = future.result()
            status = "OK" if code == 0 else "FAILED"
            print(f"  [{status}] {name}")
            print(f"         {summary}")
            if code != 0 and err:
                print(f"         Error: {err}")
            results[name] = code

    failed = [n for n, c in results.items() if c != 0]
    print(f"\nScraping complete. {len(SCRAPE_SCRIPTS) - len(failed)}/{len(SCRAPE_SCRIPTS)} scripts succeeded.")
    if failed:
        print(f"  Failed: {', '.join(failed)}")

    success = push_to_github()
    if success:
        print("\nDone! Railway will auto-redeploy with fresh data in ~60 seconds.")
    else:
        print("\nScraping done but git push failed. Check your GitHub connection.")
