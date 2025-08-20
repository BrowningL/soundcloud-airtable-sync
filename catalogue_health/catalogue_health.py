# catalogue_health/catalogue_health.py

# … imports & config unchanged …

# Change default artist field:
FIELD_ARTIST = os.getenv("FIELD_ARTIST", "Artist")  # was "Primary Artist"

def run_catalogue_health(limit_override: Optional[int] = None, dry_run_override: Optional[bool] = None) -> Dict[str, Any]:
    """Run once, return a summary dict for HTTP response/logging."""
    global LIMIT, DRY_RUN
    if limit_override is not None:
        LIMIT = int(limit_override)
    if dry_run_override is not None:
        DRY_RUN = bool(dry_run_override)

    start_ts = datetime.now(timezone.utc)
    run_id = uuid.uuid4()
    log.info("▶️ Catalogue Health run started | run_id=%s", run_id)

    bootstrap_db()
    records = fetch_airtable_catalogue()
    log.info("Fetched %d Airtable records%s", len(records), f" (LIMIT={LIMIT})" if LIMIT else "")

    rows: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(check_one, rec): rec["id"] for rec in records}
        for fut in as_completed(futures):
            rid = futures[fut]
            try:
                rows.append(fut.result())
            except Exception as e:
                log.error("record %s failed: %s", rid, e)

    persist_rows(run_id, rows)

    both = sum(1 for r in rows if r["spotify_exists"] and r["apple_exists"])
    only_spotify = sum(1 for r in rows if r["spotify_exists"] and not r["apple_exists"])
    only_apple = sum(1 for r in rows if r["apple_exists"] and not r["spotify_exists"])
    not_found = sum(1 for r in rows if not r["apple_exists"] and not r["spotify_exists"] and (r.get("notes") != "no_isrc_or_upc_or_text"))
    skipped = sum(1 for r in rows if (r.get("notes") == "no_isrc_or_upc_or_text"))

    end_ts = datetime.now(timezone.utc)
    summary = {
        "run_id": str(run_id),
        "started_at": start_ts.isoformat(),
        "finished_at": end_ts.isoformat(),
        "total": len(rows),
        "both": both,
        "only_spotify": only_spotify,
        "only_apple": only_apple,
        "not_found": not_found,
        "skipped": skipped,
    }
    persist_run_summary(run_id, start_ts, end_ts, rows)
    log.info("✅ Catalogue Health run complete | %s", summary)
    return summary

if __name__ == "__main__":
    # still runnable via `python -u catalogue_health/catalogue_health.py`
    run_catalogue_health()
