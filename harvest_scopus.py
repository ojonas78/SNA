#!/usr/bin/env python3
"""
harvest_scopus.py  –  Incremental Scopus downloader with progress tracking
"""

import os, time, json, gzip, datetime, requests
from pathlib import Path
from collections import deque
from dotenv import load_dotenv

# ─────────────────── Load secrets ────────────────────────────────────
load_dotenv()
API_KEY = os.getenv("SCOPUS_API_KEY")
if not API_KEY:
    raise RuntimeError("SCOPUS_API_KEY not found. Create a .env file.")

# ─────────────────── Config ──────────────────────────────────────────
MAX_REQS_PER_RUN = 10000         # Reasonable limit per run
CHUNK_SIZE_REQS  = 5000          # Flush every 200 requests (~5k docs)
DATE_RANGE       = "2023-2026"
BASE_URL         = "https://api.elsevier.com/content/search/scopus"

OUT_RAW_DIR      = Path("Data/raw")
OUT_RAW_DIR.mkdir(parents=True, exist_ok=True)

STATE_FILE       = Path("cursor_state.json")
HEADERS          = {"X-ELS-APIKey": API_KEY, "Accept": "application/json"}

# ─────────────────── Resume state ────────────────────────────────────
if STATE_FILE.exists():
    state         = json.loads(STATE_FILE.read_text())
    cursor_value  = state["cursor"]
    chunk_counter = state["chunk_counter"]
    print(f"📂 Resuming from chunk {chunk_counter}, cursor: {cursor_value[:20]}...")
else:
    cursor_value  = "*"
    chunk_counter = 0
    print("🆕 Starting fresh harvest")

# ─────────────────── Helper: write one raw .jsonl.gz chunk ───────────
def flush_chunk(records, counter):
    today = datetime.datetime.now().strftime("%Y%m%d")
    path  = OUT_RAW_DIR / f"scopus_raw_{counter:06d}_{today}.jsonl.gz"
    with gzip.open(path, "wt", encoding="utf-8") as gz:
        for rec in records:
            gz.write(json.dumps(rec, ensure_ascii=False) + "\n")
    print(f"💾 Chunk {counter:06d}: {len(records):,} docs → {path.name}")

# ─────────────────── Simple 9‑req/s token‑bucket limiter ─────────────
MAX_RPS   = 9
WINDOW    = 1.0
recent_ts = deque()

def rate_limited_get(session, params):
    while True:
        now = time.perf_counter()
        while recent_ts and now - recent_ts[0] >= WINDOW:
            recent_ts.popleft()
        if len(recent_ts) < MAX_RPS:
            break
        time.sleep(WINDOW - (now - recent_ts[0]) + 0.001)

    resp = session.get(BASE_URL, headers=HEADERS, params=params, timeout=30)
    recent_ts.append(time.perf_counter())
    return resp

# ─────────────────── Query skeleton ──────────────────────────────────
query = {
    "query" : "(AFFIL('Università Bocconi') OR AFFIL('Università degli studi di Verona') OR AFFIL('Università Cattolica del Sacro Cuore') OR AFFIL('Università IULM Milano') OR AFFIL('Politecnico di Milano'))",
    "date"  : DATE_RANGE,
    "sort"  : "-coverDate",
    "count" : 25,
    "cursor": cursor_value,
    "view"  : "COMPLETE",
}

# ─────────────────── Main loop ───────────────────────────────────────
session        = requests.Session()
records_buffer = []
requests_done  = 0
total_docs     = 0
t_start        = time.perf_counter()
last_progress  = t_start

print(f"\n🚀 Starting harvest: max {MAX_REQS_PER_RUN} requests, chunk size {CHUNK_SIZE_REQS}")
print("─" * 70)

while True:
    # retry up to 5 times on network errors
    for attempt in range(5):
        try:
            resp = rate_limited_get(session, query)
            break
        except requests.exceptions.RequestException as e:
            print(f"⚠ network error {attempt+1}/5 – {e}")
            time.sleep(2 + attempt)
    else:
        print("🚫 network failed 5 times; aborting.")
        break

    if resp.status_code != 200:
        print(f"🚫 HTTP {resp.status_code} – {resp.text[:200]}")
        break

    data    = resp.json()
    entries = data.get("search-results", {}).get("entry", [])
    if not entries:
        print("\n✅ No more entries – dataset complete.")
        break

    records_buffer.extend(entries)
    requests_done += 1
    total_docs += len(entries)

    # Progress update every 10 requests or 5 seconds
    now = time.perf_counter()
    if requests_done % 10 == 0 or (now - last_progress) >= 5:
        elapsed = now - t_start
        rate = requests_done / elapsed if elapsed > 0 else 0
        eta = (MAX_REQS_PER_RUN - requests_done) / rate if rate > 0 else 0
        pct = (requests_done / MAX_REQS_PER_RUN) * 100

        print(f"📊 [{requests_done:4d}/{MAX_REQS_PER_RUN}] {pct:5.1f}% | "
              f"Docs: {total_docs:6,} | Rate: {rate:.1f} req/s | "
              f"ETA: {eta/60:.1f}m", end='\r')
        last_progress = now

    # pagination
    next_cursor = data["search-results"]["cursor"].get("@next")
    if not next_cursor:
        print("\n✅ Reached end – no @next cursor.")
        break
    query["cursor"] = next_cursor

    # chunk flush
    if requests_done % CHUNK_SIZE_REQS == 0:
        print()  # New line before chunk message
        chunk_counter += 1
        flush_chunk(records_buffer, chunk_counter)
        records_buffer = []

    # per‑run quota
    if requests_done >= MAX_REQS_PER_RUN:
        last_date = entries[-1].get("prism:coverDate")
        print(f"\n⏸ Hit {MAX_REQS_PER_RUN} requests limit. Last coverDate: {last_date}")
        break

# flush leftovers
if records_buffer:
    print()
    chunk_counter += 1
    flush_chunk(records_buffer, chunk_counter)

# persist state
STATE_FILE.write_text(json.dumps({"cursor": query["cursor"],
                                  "chunk_counter": chunk_counter}))
elapsed = time.perf_counter() - t_start
print("─" * 70)
print(f"✅ Done! Requests: {requests_done:,} | Docs: {total_docs:,} | "
      f"Time: {elapsed:.1f}s ({elapsed/60:.1f}m)")
