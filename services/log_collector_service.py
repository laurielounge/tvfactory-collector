# services/log_collector_service.py
import hashlib
import json
import subprocess
from urllib.parse import urlparse, parse_qs

from redis.exceptions import RedisError

from core.logger import logger
from infrastructure.async_factory import BaseAsyncFactory
from utils.timer import StepTimer

LOG_QUEUE = "loghit_queue"
STATE_KEY_PREFIX = "logstate:"
SSH_PORT = 822
LOG_PATH = "/var/log/nginx/shadow_pipeline.log"
VALID_PATHS = {"/client", "/response", "/impression", "/viewer"}

HOSTS = ["10.0.0.50", "10.0.0.51", "10.0.0.52", "10.0.0.53", "10.0.0.150", "10.0.0.151", "10.0.0.60", "10.0.0.61",
         "10.0.0.153", "10.0.0.154"]


# --- Utility Functions [unchanged] ---

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


# --- Collector Service Class (Refactored) ---

class LogCollectorService(BaseAsyncFactory):
    """
    Fetches log files from remote nginx servers over SSH and pushes them into Redis.

    Responsibilities:
    - SSH into hosts listed in `HOSTS`
    - Read lines from /var/log/nginx/shadow_pipeline.log
    - Detect log rotation via hash comparison of the first line
    - Maintain per-host progress state in Redis using `logstate:<hostname>`
    - Push each raw line as JSON payload into Redis list `loghit_queue`

    External Systems:
    - ✅ Reads from remote filesystem via SSH
    - ✅ Writes to Redis (`loghit_queue`)

    Downstream:
    - `LoghitWorkerService` pops from `loghit_queue` and pushes to RabbitMQ
    """

    def __init__(self):
        # Initialize with Redis dependency
        super().__init__(require_redis=True)
        self.timer = StepTimer()
        logger.info("LogCollectorService initialized")

    async def service_setup(self):
        """Service-specific setup after connections established."""
        logger.info("LogCollectorService setup complete")

    async def process_batch(self, batch_size: int = None, timeout: float = 30.0) -> int:
        """
        Process a batch of hosts, reading their logs and queueing entries.

        Args:
            batch_size: Ignored (processes all configured hosts)
            timeout: Maximum processing time (currently unused)

        Returns:
            int: Number of hosts successfully processed
        """
        return await self.process_hosts_batch()

    async def process_hosts_batch(self):
        """Processes a single batch of all configured hosts."""
        logger.info(f"Processing batch of {len(HOSTS)} hosts")
        processed = 0

        # Check health with backoff instead of exiting
        if not await self._registry.check_with_backoff(max_wait=30):
            logger.error("Health check failed - skipping processing cycle")
            return 0

        # Process all hosts with resilient error handling
        for host in HOSTS:
            try:
                await self._process_host(host)
                processed += 1
            except Exception as e:
                logger.error(f"Error processing host {host}: {e}")

        logger.info(f"Processed {processed}/{len(HOSTS)} hosts")
        return processed

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
