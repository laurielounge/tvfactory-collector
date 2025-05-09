# services/log_collector_service.py
import asyncio
import hashlib
import json
import subprocess

from core.logger import logger
from utils.redis_client import get_redis_client
from utils.timer import StepTimer

LOG_QUEUE = "loghit_queue"
STATE_KEY_PREFIX = "logstate:"
SSH_PORT = 822
LOG_PATH = "/var/log/nginx/shadow_pipeline.log"

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

        if not run_once:
            while True:
                await asyncio.sleep(interval_seconds)
                for host in HOSTS:
                    self._process_host(host)
