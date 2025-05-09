import subprocess
import hashlib
import json

from utils.redis_client import get_redis_client

LOG_QUEUE = "loghit_queue"
STATE_KEY_PREFIX = "logstate:"


def get_log_state(redis, hostname):
    return redis.hgetall(f"{STATE_KEY_PREFIX}{hostname}")


def set_log_state(redis, hostname, line_num, line_hash):
    redis.hset(f"{STATE_KEY_PREFIX}{hostname}", mapping={
        "last_line_num": line_num,
        "last_line_hash": line_hash
    })


def ssh_sed_lines(hostname, start_line):
    cmd = f"ssh {hostname} 'sed -n \"{start_line},$p\" /var/log/nginx/shadow_pipeline.log'"
    result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    return result.stdout.splitlines()


def hash_line(line):
    return hashlib.sha256(line.encode()).hexdigest()


def process_log_batch(hostname):
    redis = get_redis_client()
    state = get_log_state(redis, hostname)

    last_line = int(state.get("last_line_num", 1))
    last_hash = state.get("last_line_hash")

    lines = ssh_sed_lines(hostname, last_line)
    if not lines:
        return

    # Detect rotation
    if lines[0].strip() == "" or hash_line(lines[0]) != last_hash:
        last_line = 1
        lines = ssh_sed_lines(hostname, 1)

    for i, line in enumerate(lines, start=last_line + 1):
        redis.lpush(LOG_QUEUE, json.dumps({
            "host": hostname,
            "line_num": i,
            "raw": line
        }))
        final_line = line
        final_line_num = i

    # Save new state
    set_log_state(redis, hostname, final_line_num, hash_line(final_line))
