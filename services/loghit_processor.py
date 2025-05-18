# services/loghit_processor.py - Optimized version
import json
import re
from urllib.parse import urlparse, parse_qs

from core.logger import logger

# Precompile regex patterns at module level for better performance
INT_HEAD = re.compile(r"^\d+")
PATH_PATTERN = re.compile(r'(GET|POST)\s+/(client|response|impression|viewer)')
VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}

# Cache URL parsing results where possible
_url_cache = {}


def parse_log_line(raw_line: str) -> dict | None:
    """Parse log line with optimized splitting (faster than regex for simple delimiters)"""
    parts = raw_line.strip().split('\t')
    if len(parts) < 9:
        return None  # malformed line

    # Fast path extraction without full URL parsing when possible
    path = None
    url = parts[3]
    if url in _url_cache:
        path = _url_cache[url]
    else:
        parsed = urlparse(url)
        path = parsed.path
        # Cache up to 10,000 URLs to prevent memory bloat
        if len(_url_cache) < 10000:
            _url_cache[url] = path

    return {
        "timestamp": parts[0],
        "edge_ip": parts[1],
        "method": parts[2],
        "path": path,
        "query_string": parts[4],
        "status": parts[5],
        "user_agent": parts[6],
        "referer": parts[7],
        "client_ip": parts[8]
    }


def extract_leading_int(value: str) -> str:
    """Extract leading integer from string using precompiled regex"""
    match = INT_HEAD.match(value)
    return match.group(0) if match else None


def parse_query_string(qs: str) -> dict:
    """Parse query string with optimized handling"""
    parsed = parse_qs(qs)
    return {k: extract_leading_int(v[0]) for k, v in parsed.items()}


def classify_entry(entry: dict) -> str | None:
    """Classify log entry with optimized path and parameter checks"""
    path = entry["path"]

    # Early return if path isn't one we care about
    if path not in VALID_PATHS:
        return None

    qs = parse_query_string(entry["query_string"])

    # Normalize aliases with direct dictionary access (faster than chained gets)
    client = qs.get("client") or qs.get("c")
    booking = qs.get("booking") or qs.get("campaign") or qs.get("b")
    creative = qs.get("creative") or qs.get("r")
    site = qs.get("site") or qs.get("s")

    # Store only non-None values
    aliases = {}
    if client: aliases["client"] = client
    if booking: aliases["booking"] = booking
    if creative: aliases["creative"] = creative
    if site: aliases["site"] = site

    entry["query"] = aliases

    # Fast path categorization with direct checks
    if path in {"/impression", "/viewer"}:
        if client and booking and creative:
            return "impression"
    elif path in {"/client", "/response"}:
        if client and site:
            return "webhit"

    return None


def process_log_payload(raw_json: str) -> tuple[str, dict] | None:
    """Process log payload with optimized error handling"""
    try:
        payload = json.loads(raw_json)
        logger.debug(f"Payload is {payload}")
        # Fast path for missing fields
        if not all(k in payload for k in ("raw", "host", "line_num")):
            return None

        entry = parse_log_line(payload["raw"])
        logger.debug(f"Entry is {entry}")
        if not entry:
            return None

        entry["host"] = payload["host"]
        entry["line_num"] = payload["line_num"]

        # Inline query parsing to reduce function calls
        entry["query"] = parse_query_string(entry["query_string"])
        logger.debug(f"Entry is now {entry}")

        category = classify_entry(entry)
        logger.debug(f"Category is {category}")
        if not category:
            logger.debug(f"No category found for {entry}")
            return None

        return category, entry
    except Exception:
        return None
