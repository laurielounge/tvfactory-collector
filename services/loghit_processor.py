# services/loghit_processor.py

import json
import re
from urllib.parse import urlparse, parse_qs

INT_HEAD = re.compile(r"^\d+")
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


def extract_leading_int(value: str) -> str:
    match = INT_HEAD.match(value)
    return match.group(0) if match else None


def parse_query_string(qs: str) -> dict:
    parsed = parse_qs(qs)
    return {k: extract_leading_int(v[0]) for k, v in parsed.items()}


def classify_entry(entry: dict) -> str | None:
    path = entry["path"]
    qs = parse_query_string(entry["query_string"])

    # Normalize aliases
    aliases = {
        "client": qs.get("client") or qs.get("c"),
        "booking": qs.get("booking") or qs.get("campaign") or qs.get("b"),
        "creative": qs.get("creative") or qs.get("r"),
        "site": qs.get("site") or qs.get("s"),
    }

    entry["query"] = {k: v for k, v in aliases.items() if v}  # overwrite with normalized

    if path in {"/impression", "/viewer"} and all(k in aliases for k in ("client", "booking", "creative")):
        return "impression"
    elif path in {"/client", "/response"} and all(k in aliases for k in ("client", "site")):
        return "webhit"

    return None


def process_log_payload(raw_json: str) -> tuple[str, dict] | None:
    """
    Parses and classifies a JSON-wrapped log line.

    Steps:
    - Decode JSON (should contain keys: host, line_num, raw)
    - Parse the `raw` field into structured log fields (timestamp, path, query, etc)
    - Classify based on path + query parameters:
        - /impression or /viewer → impression
        - /client or /response   → webhit
    - Normalize query aliases (e.g., b → booking, r → creative)

    Returns:
    - (category, structured_entry) if valid
    - None if the line is malformed or irrelevant
    """

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
