#!/usr/bin/env python3
"""Import a legacy lead JSON snapshot into DATABASE_URL.

Run this only from an operator-controlled path outside the repository, for example:
    DATABASE_URL=... python import_snapshot.py /secure/restore/leads.snapshot.json

The importer deliberately does not create, copy, or retain a backup in this
repository. It accepts a JSON list or an object containing a ``leads`` or
``calls`` list and can be safely re-run without duplicating CallSid records.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def parse_timestamp(value: Any) -> datetime | None:
    if not value or not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed
    except ValueError:
        return None


def normalized_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict):
        records = payload.get("leads") or payload.get("calls") or []
    else:
        raise ValueError("snapshot must be a JSON list or contain a 'leads'/'calls' list")
    if not isinstance(records, list) or not all(isinstance(record, dict) for record in records):
        raise ValueError("snapshot records must be JSON objects")
    return records


def imported_sid(record: dict[str, Any]) -> str:
    original = record.get("call_sid") or record.get("CallSid")
    if isinstance(original, str) and original.strip():
        return original.strip()
    canonical = json.dumps(record, sort_keys=True, separators=(",", ":"), default=str)
    return "imported-" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:40]


def main() -> int:
    parser = argparse.ArgumentParser(description="Import a JSON call/lead snapshot into DATABASE_URL.")
    parser.add_argument("snapshot", type=Path, help="Path to a JSON snapshot outside this repository")
    args = parser.parse_args()
    source = args.snapshot.expanduser().resolve()
    repository = Path(__file__).resolve().parent
    if repository == source or repository in source.parents:
        parser.error("refusing a snapshot stored inside this repository; use an external protected path")
    if not source.is_file():
        parser.error("snapshot path is not a file")

    try:
        with source.open("r", encoding="utf-8") as handle:
            records = normalized_records(json.load(handle))
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        print(f"Import failed: {exc}", file=sys.stderr)
        return 2

    # Import only after input validation. storage reads DATABASE_URL at import time.
    from storage import (  # pylint: disable=import-outside-toplevel
        Call,
        SessionLocal,
        TranscriptLine,
        init_db,
        update_extracted_fields,
        upsert_call,
    )

    init_db()
    imported = skipped = 0
    for record in records:
        sid = imported_sid(record)
        session = SessionLocal()
        try:
            if session.query(Call.id).filter_by(call_sid=sid).first():
                skipped += 1
                continue
        finally:
            session.close()

        status = str(record.get("status") or "imported")
        started = parse_timestamp(record.get("timestamp") or record.get("started_at"))
        caller = record.get("caller_number") or record.get("from") or record.get("From")
        upsert_call(sid, caller_number=str(caller) if caller else None, status=status, occurred_at=started)
        fields = {key: record.get(key) for key in (
            "name", "phone", "email", "rv_details", "goals", "current_setup", "location", "summary"
        )}
        update_extracted_fields(sid, fields, extraction_status="imported")

        transcript = record.get("transcript") or record.get("transcript_lines") or []
        if isinstance(transcript, list):
            session = SessionLocal()
            try:
                call = session.query(Call).filter_by(call_sid=sid).one()
                for sequence, entry in enumerate(transcript, start=1):
                    if isinstance(entry, dict):
                        speaker = str(entry.get("speaker") or "Unknown")
                        content = str(entry.get("content") or entry.get("text") or "")
                    else:
                        text = str(entry)
                        speaker, separator, content = text.partition(":")
                        if not separator:
                            speaker, content = "Unknown", text
                    if content.strip():
                        session.add(TranscriptLine(
                            call_id=call.id, sequence=sequence, speaker=speaker.strip()[:32] or "Unknown", content=content.strip()
                        ))
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()
        imported += 1

    print(f"Import complete: {imported} records imported, {skipped} already present. No snapshot data was copied into the repository.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
