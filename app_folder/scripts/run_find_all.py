import subprocess
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

scripts = [
    "find_comedy_clubs.py",
    "find_live_music.py",
    "find_bar_events.py",
    "find_museums.py",
    "find_improv.py",
    "find_board_games.py",
    "find_escape_rooms.py",
]

def run_script(script_name):
    path = os.path.join(SCRIPT_DIR, script_name)
    result = subprocess.run(["python", path], capture_output=True, text=True)
    return script_name, result.returncode, result.stdout[-500:], result.stderr[-300:]

print("Running all find scripts concurrently...\n")

with ThreadPoolExecutor(max_workers=7) as executor:
    futures = {executor.submit(run_script, s): s for s in scripts}
    for future in as_completed(futures):
        name, code, out, err = future.result()
        status = "OK" if code == 0 else "FAILED"
        print(f"[{status}] {name}")
        print(out)
        if err:
            print(f"  STDERR: {err}")

print("\nAll done.")
