# services/log_collector_service.py
import asyncio
import hashlib
import json
import subprocess
from urllib.parse import urlparse, parse_qs

from core.logger import logger
from utils.redis_client import get_redis_client
from utils.timer import StepTimer

LOG_QUEUE = "loghit_queue"
STATE_KEY_PREFIX = "logstate:"
SSH_PORT = 822
LOG_PATH = "/var/log/nginx/shadow_pipeline.log"
VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}

HOSTS = [
    "10.0.0.50",
    "10.0.0.51",
    "10.0.0.52",
    "10.0.0.53",
    "10.0.0.150",
    "10.0.0.151",
    "10.0.0.60",
    "10.0.0.61",
    "10.0.0.153"
]


def parse_log_line(raw_line: str) -> dict | None:
    parts = raw_line.strip().split('\t')
    if len(parts) < 9:
        return None  # malformed

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

    if path in {"/impression", "/viewer"}:
        if all(k in qs for k in ("client", "booking", "creative")):
            return "impression"
    elif path in {"/client", "/response"}:
        if all(k in qs for k in ("client", "site")):
            return "webhit"
    return None  # discard


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


class LogCollectorService:
    def __init__(self):
        self.redis = get_redis_client()
        self.timer = StepTimer()

    def _get_log_state(self, hostname):
        return self.redis.hgetall(f"{STATE_KEY_PREFIX}{hostname}")

    def _set_log_state(self, hostname, line_num, line_hash):
        self.redis.hset(f"{STATE_KEY_PREFIX}{hostname}", mapping={
            "last_line_num": line_num,
            "last_line_hash": line_hash
        })

    def _ssh_fetch_lines(self, hostname, start_line):
        cmd = [
            "ssh", "-p", str(SSH_PORT),
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            f"root@{hostname}",
            f"sed -n {start_line},\$p {LOG_PATH}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"[{hostname}] SSH error: {result.stderr.strip()}")
            return []
        return result.stdout.splitlines()

    def _hash_line(self, line):
        return hashlib.sha256(line.encode()).hexdigest()

    def _process_host(self, hostname):
        with self.timer.time("fetch_state"):
            state = self._get_log_state(hostname)
            last_line = int(state.get("last_line_num", 1))
            last_hash = state.get("last_line_hash", "")

            if not state:
                logger.info(f"[{hostname}] No previous state found. Starting fresh from line 1.")

        with self.timer.time("ssh_fetch"):
            lines = self._ssh_fetch_lines(hostname, last_line)

        if not lines:
            logger.info(f"[{hostname}] No new lines since last read.")
            return

        first_line_hash = self._hash_line(lines[0])
        if last_hash and first_line_hash != last_hash:
            logger.warning(
                f"[{hostname}] First line hash mismatch (expected {last_hash[:8]}..., got {first_line_hash[:8]}...). Continuing anyway — not assuming rotation.")

        with self.timer.time("queue_push"):
            for i, line in enumerate(lines, start=last_line + 1):
                payload = {
                    "host": hostname,
                    "line_num": i,
                    "raw": line
                }
                self.redis.lpush(LOG_QUEUE, json.dumps(payload))
                final_line = line
                final_line_num = i

        with self.timer.time("state_save"):
            self._set_log_state(hostname, final_line_num, self._hash_line(final_line))

        logger.info(f"[{hostname}] Pushed {len(lines)} lines.")
        self.timer.tick()

    async def start(self, interval_seconds: int = 30, run_once=False):
        for host in HOSTS:
            self._process_host(host)

        if run_once:
            return  # ✅ exit cleanly after one pass

        while True:
            await asyncio.sleep(interval_seconds)
            for host in HOSTS:
                self._process_host(host)
