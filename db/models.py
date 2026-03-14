"""
Database layer for sanctions screener.
Uses Supabase PostgreSQL via REST API (no driver needed).
"""

import json
import os
import requests
from datetime import datetime

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SERVICE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")


def _headers():
    """Supabase REST API headers with service_role key (full write access)."""
    return {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates",
    }


def _api(path):
    return f"{SUPABASE_URL}/rest/v1/{path}"


def init_db():
    """No-op for Supabase – schema is set up via SQL Editor."""
    pass


def upsert_entity(conn_unused, entity: dict):
    """
    Kept for API compatibility with crawlers.
    Actual upsert happens in upsert_entities_batch().
    Returns the entity so it can be collected.
    """
    return entity


def upsert_entities_batch(entities: list[dict], batch_size: int = 500):
    """
    Upsert entities in batches to Supabase.
    Uses PostgREST upsert (INSERT ... ON CONFLICT).
    """
    total = 0
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i + batch_size]
        rows = []
        for e in batch:
            entity_id = f"{e['source']}:{e.get('source_id', '')}"
            rows.append({
                "id": entity_id,
                "source": e.get("source", ""),
                "source_id": e.get("source_id", ""),
                "entity_type": e.get("entity_type", "unknown"),
                "full_name": e.get("full_name", ""),
                "first_name": e.get("first_name", ""),
                "last_name": e.get("last_name", ""),
                "aliases": e.get("aliases", []),
                "name_original_script": e.get("name_original_script", ""),
                "identifiers": e.get("identifiers", []),
                "nationalities": e.get("nationalities", []),
                "addresses": e.get("addresses", []),
                "birth_dates": e.get("birth_dates", []),
                "birth_places": e.get("birth_places", []),
                "listed_date": e.get("listed_date", ""),
                "delisted_date": e.get("delisted_date", ""),
                "programs": e.get("programs", []),
                "reasons": e.get("reasons", ""),
                "legal_basis": e.get("legal_basis", ""),
                "source_url": e.get("source_url", ""),
                "raw_data": e.get("raw_data", {}),
                "last_updated": datetime.utcnow().isoformat(),
            })

        resp = requests.post(
            _api("entities"),
            headers=_headers(),
            json=rows,
        )
        if resp.status_code not in (200, 201):
            raise Exception(f"Supabase upsert failed ({resp.status_code}): {resp.text[:200]}")

        total += len(batch)
    return total


def update_source_status(conn_unused, source: str, count: int, duration: float, error: str = None):
    """Update sync status for a source in Supabase."""
    now = datetime.utcnow().isoformat()
    row = {
        "source": source,
        "last_sync": now,
        "entity_count": count,
        "sync_duration_seconds": round(duration, 2),
        "error_message": error,
    }
    if not error:
        row["last_success"] = now

    resp = requests.post(
        _api("source_status"),
        headers=_headers(),
        json=row,
    )
    if resp.status_code not in (200, 201):
        raise Exception(f"Supabase source_status update failed: {resp.text[:200]}")


def get_source_stats() -> list[dict]:
    """Get sync status for all sources."""
    resp = requests.get(
        _api("source_status?order=source"),
        headers={
            "apikey": SUPABASE_SERVICE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
        },
    )
    if resp.status_code == 200:
        return resp.json()
    return []
