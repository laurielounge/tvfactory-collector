# services/vector_processor.py
import ipaddress
import re
from urllib.parse import parse_qs
from utils.ip import is_bogon_ip
from core.logger import logger
from utils.ip import format_ipv4_as_mapped_ipv6

# Precompile regex patterns for performance
INT_HEAD = re.compile(r"^\d+")
VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}


def extract_leading_int(value: str) -> str:
    """Extract leading integer from string using precompiled regex"""
    if not value:
        return None
    match = INT_HEAD.match(value)
    return match.group(0) if match else None


def process_vector_payload(payload: dict) -> tuple[str, dict] | None:
    """
    Process a Vector-formatted log message.

    Args:
        payload: The JSON payload received from Vector

    Returns:
        (category, entry) tuple if valid, None if not classifiable
    """
    # Skip messages without required fields
    if not all(k in payload for k in ("path", "query_string", "client_ip")):
        return None

    # Extract path without query string
    path = payload["path"].split("?")[0] if "?" in payload["path"] else payload["path"]
    logger.debug(f"Path is {path}")

    # Early exit if path not relevant
    if path not in VALID_PATHS:
        logger.debug("Not a valid path")
        return None

    # Parse query string
    params = {}
    query_params = parse_qs(payload["query_string"])
    logger.debug(f"Query params are {query_params}")

    # Extract values with simple aliasing
    client = (query_params.get("client", []) + query_params.get("c", []))[0] if (
            query_params.get("client") or query_params.get("c")) else None
    site = (query_params.get("site", []) + query_params.get("s", []))[0] if (
            query_params.get("site") or query_params.get("s")) else None
    booking = (query_params.get("booking", []) + query_params.get("campaign", []) + query_params.get("b", []))[0] if (
            query_params.get("booking") or query_params.get("campaign") or query_params.get("b")) else None
    creative = (query_params.get("creative", []) + query_params.get("r", []))[0] if (
            query_params.get("creative") or query_params.get("r")) else None
    logger.debug(f"Query params are {client=} {site=} {booking=} {creative=}")
    # Extract leading integers from any found values
    if client: client = extract_leading_int(client)
    if site: site = extract_leading_int(site)
    if booking: booking = extract_leading_int(booking)
    if creative: creative = extract_leading_int(creative)

    # Build query dict for downstream processing
    query = {}
    if client: query["client"] = client
    if site: query["site"] = site
    if booking: query["booking"] = booking
    if creative: query["creative"] = creative

    # Classify message
    category = None
    if path in {"/impression", "/viewer"} and client and booking and creative:
        logger.debug("Definitely a impression")
        category = "impression"
    elif path in {"/client", "/response"} and client and site:
        logger.debug("Definitely a webhit")
        category = "webhit"
    else:
        logger.debug("Path not identified")
        return None
    # Extract and format IP
    raw_ip = payload.get("client_ip", "").split(",")[0].strip()
    try:
        ipaddress.ip_address(raw_ip)
        is_bogon = is_bogon_ip(raw_ip)
        ip = format_ipv4_as_mapped_ipv6(raw_ip)

        # For impressions, consider preserving revenue
        if is_bogon and category == "impression":
            # Log but don't drop impressions from private networks
            logger.info(f"Processing ungeolocation-capable impression from bogon: {raw_ip}")
            # Flag it for the downstream process to avoid API calls
            return None
        elif is_bogon:
            # For webhits, we can be more selective
            logger.debug(f"Dropping bogon IP in webhit: {raw_ip}")
            return None

    except ValueError:
        logger.warning(f"[SKIP] Invalid IP: {raw_ip}")
        return None

    # Build entry object for downstream
    entry = {
        # Include original fields needed downstream
        "timestamp": payload.get("timestamp"),
        "ipaddress": ip,
        "client_ip": raw_ip,
        "user_agent": payload.get("user_agent"),
        "host": payload.get("hostname"),
        "path": path,
        "query_string": payload.get("query_string"),
        # Add our processed query params
        "query": query
    }
    logger.debug(f"Entry is {entry}")

    return category, entry
