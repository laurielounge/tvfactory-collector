# services/log_collector_service.py
import asyncio
import hashlib
import json
import subprocess
from urllib.parse import urlparse, parse_qs

from redis.asyncio import Redis
from redis.exceptions import RedisError

from core.logger import logger
from infrastructure.circuit_breaker import RedisHealthChecker, HealthCheckRegistry
from infrastructure.redis_client import get_redis_client
from utils.timer import StepTimer

LOG_QUEUE = "loghit_queue"
STATE_KEY_PREFIX = "logstate:"
SSH_PORT = 822
LOG_PATH = "/var/log/nginx/shadow_pipeline.log"
VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}

HOSTS = ["10.0.0.50", "10.0.0.51", "10.0.0.52", "10.0.0.53", "10.0.0.150", "10.0.0.151", "10.0.0.60", "10.0.0.61",
         "10.0.0.153", "10.0.0.154"]


# --- Utility Functions ---

def parse_log_line(raw_line: str) -> dict | None:
    """Parses a raw tab-delimited log line into structured fields."""
    parts = raw_line.strip().split('\t')
    if len(parts) < 9:
        return None
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
    """Converts a query string into a dictionary."""
    return {k: v[0] for k, v in parse_qs(qs).items()}


def classify_entry(entry: dict) -> str | None:
    """Classifies a log entry into 'impression', 'webhit', or None."""
    path = entry["path"]
    qs = parse_query_string(entry["query_string"])
    if path in {"/impression", "/viewer"} and all(k in qs for k in ("client", "booking", "creative")):
        return "impression"
    if path in {"/client", "/response"} and all(k in qs for k in ("client", "site")):
        return "webhit"
    return None


def process_log_payload(raw_json: str) -> tuple[str, dict] | None:
    """Parses and classifies a raw log payload."""
    try:
        payload = json.loads(raw_json)
        entry = parse_log_line(payload["raw"])
        if not entry:
            return None
        entry["host"] = payload["host"]
        entry["line_num"] = payload["line_num"]
        entry["query"] = parse_query_string(entry["query_string"])
        category = classify_entry(entry)
        return (category, entry) if category else None
    except Exception as e:
        logger.warning(f"Failed to process log payload: {e}")
        return None


# --- Collector Service Class ---

class LogCollectorService:
    """
    Asynchronous log collector that fetches logs from remote hosts via SSH,
    detects log rotations, queues log lines into Redis, and tracks progress state.
    """

    def __init__(self, redis_client: Redis):
        logger.info("Init log collector service")
        self.timer = StepTimer()
        self.redis = redis_client

    @classmethod
    async def create(cls):
        redis_client = await get_redis_client()
        return cls(redis_client)

    async def start(self, interval_seconds: int = 300, run_once=False):
        logger.info("LogCollectorService pre-start.")
        registry = HealthCheckRegistry(
            RedisHealthChecker(self.redis),
        )
        await registry.assert_healthy_or_exit()
        logger.info("LogCollectorService started.")
        for host in HOSTS:
            await self._process_host(host)

        if run_once:
            return

        while True:
            await asyncio.sleep(interval_seconds)
            for host in HOSTS:
                await self._process_host(host)
            logger.info("Finished log collecting")

    async def _get_log_state(self, hostname: str) -> dict:
        try:
            return await self.redis.hgetall(f"{STATE_KEY_PREFIX}{hostname}") or {}
        except RedisError as e:
            logger.warning(f"[REDIS GET ERROR] {hostname}: {e}")
            return {}

    async def _save_log_state(self, hostname: str, filename: str, line_num: int, line_hash: str):
        try:
            await self.redis.hset(f"{STATE_KEY_PREFIX}{hostname}", mapping={
                "filename": filename,
                "last_line_num": line_num,
                "first_line_hash": line_hash,
            })
        except RedisError as e:
            logger.warning(f"[REDIS SET ERROR] {hostname}: {e}")

    async def _process_host(self, hostname: str):
        logger.info(f"Processing host name {hostname}")
        with self.timer.time("fetch_state"):
            state = await self._get_log_state(hostname)
            filename = state.get("filename", LOG_PATH)
            last_line = int(state.get("last_line_num", 1))
            prev_hash = state.get("first_line_hash", "")

        logger.debug(f"[{hostname}] Resuming from {filename} at line {last_line}")

        first_line = self._ssh_head_line(hostname)
        if first_line is None:
            logger.warning(f"[{hostname}] Could not read first line from {LOG_PATH}")
            return

        current_hash = self._hash_line(first_line)
        rotated = prev_hash and current_hash != prev_hash

        if rotated:
            logger.info(f"[{hostname}] Detected log rotation (hash changed).")
            rotated_file = self._find_latest_rotated_log(hostname)
            if rotated_file:
                logger.info(f"[{hostname}] Completing tail of rotated file: {rotated_file}")
                lines = self._ssh_fetch_lines(hostname, last_line, rotated_file)
                await self._queue_lines(hostname, lines, last_line, rotated_file)
            else:
                logger.warning(f"[{hostname}] No rotated log found — some data may be lost.")

            lines = self._ssh_fetch_lines(hostname, 2, LOG_PATH)
            await self._queue_lines(hostname, lines, 1, LOG_PATH, current_hash)
        else:
            lines = self._ssh_fetch_lines(hostname, last_line, filename)
            await self._queue_lines(hostname, lines, last_line, filename, prev_hash or current_hash)

    async def _queue_lines(self, hostname, lines, start_line, filename, first_line_hash=None):
        final_line_num = start_line
        for i, line in enumerate(lines, start=start_line + 1):
            payload = {"host": hostname, "line_num": i, "raw": line}
            await self.redis.lpush(LOG_QUEUE, json.dumps(payload))
            final_line_num = i

        if lines:
            logger.info(f"[{hostname}] Pushed {len(lines)} lines from {filename}")
            await self._save_log_state(hostname, filename, final_line_num, first_line_hash or self._hash_line(lines[0]))
        else:
            logger.debug(f"[{hostname}] No new lines to queue from {filename}")

    @staticmethod
    def _hash_line(line: str) -> str:
        return hashlib.sha256(line.encode()).hexdigest()

    @staticmethod
    def _ssh_head_line(hostname: str) -> str | None:
        cmd = [
            "ssh", "-p", str(SSH_PORT),
            "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            f"root@{hostname}", f"head -n1 {LOG_PATH}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.stdout.strip() if result.returncode == 0 else None

    @staticmethod
    def _ssh_fetch_lines(hostname, start_line, filename=LOG_PATH):
        cmd = [
            "ssh", "-p", str(SSH_PORT),
            "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            f"root@{hostname}", rf"sed -n {start_line},\$p {filename}"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"[{hostname}] SSH error: {result.stderr.strip()}")
            return []
        return result.stdout.splitlines()

    @staticmethod
    def _find_latest_rotated_log(hostname: str) -> str | None:
        cmd = [
            "ssh", "-p", str(SSH_PORT),
            "-o", "StrictHostKeyChecking=no", "-o", "UserKnownHostsFile=/dev/null",
            f"root@{hostname}", "ls -1t /var/log/nginx/shadow_pipeline.log-*"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode != 0:
            logger.warning(f"[{hostname}] Failed to list rotated logs: {result.stderr.strip()}")
            return None
        for line in result.stdout.splitlines():
            if line.strip().endswith(".log") or ".log-" in line:
                return line.strip()
        return None
