# services/loghit_processor.py

import json
from urllib.parse import urlparse, parse_qs

VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}


def parse_log_line(raw_line: str) -> dict | None:
    parts = raw_line.strip().split('\t')
    if len(parts) < 9:
        return None  # malformed line

    return {
        "timestamp": parts[0],
        "edge_ip": parts[1],
        "method": parts[2],
        "path": urlparse(parts[3]).path,
        "query_string": parts[4],
        "status": parts[5],
        "user_agent": parts[6],
        "referer": parts[7],
        "client_ip": parts[8]
    }


def parse_query_string(qs: str) -> dict:
    return {k: v[0] for k, v in parse_qs(qs).items()}


def classify_entry(entry: dict) -> str | None:
    path = entry["path"]
    qs = parse_query_string(entry["query_string"])

    if path in {"/impression", "/viewer"} and all(k in qs for k in ("client", "booking", "creative")):
        return "impression"
    elif path in {"/client", "/response"} and all(k in qs for k in ("client", "site")):
        return "webhit"
    return None


def process_log_payload(raw_json: str) -> tuple[str, dict] | None:
    try:
        payload = json.loads(raw_json)
        entry = parse_log_line(payload["raw"])
        if not entry:
            return None

        entry["host"] = payload["host"]
        entry["line_num"] = payload["line_num"]
        entry["query"] = parse_query_string(entry["query_string"])
        category = classify_entry(entry)
        if not category:
            return None

        return category, entry
    except Exception:
        return None
