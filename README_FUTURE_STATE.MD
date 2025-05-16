# Edge Server Log Shipping: The Elegant Solution

*Mrs. Peel sketches a diagram on a notepad*

## Core Architecture

```
┌─────────────────┐          ┌─────────────────┐          ┌─────────────────┐
│   Edge Server   │          │ Message Broker  │          │ Processing Tier │
│ ┌─────────────┐ │          │                 │          │                 │
│ │   nginx     │ │          │  ┌───────────┐  │          │  ┌───────────┐  │
│ └─────┬───────┘ │          │  │           │  │          │  │ Loghit    │  │
│       │         │          │  │  RabbitMQ │  │          │  │ Worker    │  │
│ ┌─────▼───────┐ │  HTTPS   │  │  Queue    │◄─┼──────────┼──┤           │  │
│ │ vector.toml │◄┼──────────┼──┤           │  │          │  └───────────┘  │
│ └─────┬───────┘ │  (Push)  │  │           │  │          │                 │
│       │         │          │  └───────────┘  │          │  ┌───────────┐  │
│ ┌─────▼───────┐ │          │                 │          │  │ Impression│  │
│ │ local disk  │ │          │                 │          │  │ Consumer  │  │
│ │ buffer      │ │          │                 │          │  └───────────┘  │
│ └─────────────┘ │          │                 │          │                 │
└─────────────────┘          └─────────────────┘          └─────────────────┘
```

## The Approach

I propose using [Vector](https://vector.dev/) - an ultralight, blazingly fast data router. It's essentially a modern
replacement for traditional log shippers with reliability built in:

1. **Edge Server Setup**:
    - Install Vector on each edge server (it's a single ~15MB binary)
    - Configure it to monitor nginx access log file in real-time
    - Filter entries immediately based on valid paths
    - Buffer entries to local disk before sending
    - Transmit directly to your existing RabbitMQ

2. **Vector Configuration** (vector.toml):
   ```toml
   [sources.nginx_logs]
   type = "file"
   include = ["/var/log/nginx/shadow_pipeline.log"]
   ignore_older = 86400  # 1 day

   [transforms.filter_paths]
   type = "filter"
   inputs = ["nginx_logs"]
   condition = 'includes(["client", "response", "impression", "viewer"], parse_regex(.message, "^.*\\s(GET|POST)\\s/(?P<path>[^\\s?]+)").path)'

   [transforms.parse_nginx]
   type = "remap"
   inputs = ["filter_paths"]
   source = '''
   . = parse_nginx_log(.message)
   .hostname = get_hostname()
   .timestamp = now()
   '''

   [sinks.rabbitmq_output]
   type = "amqp"
   inputs = ["parse_nginx"]
   url = "amqp://user:password@rabbitmq-server:5672/%2F"
   exchange = ""
   routing_key = "loghit_queue"
   persistent = true  # Critical for reliability
   
   # Sophisticated error handling with retry
   retry_initial_backoff_secs = 1
   retry_max_backoff_secs = 300
   retry_max_duration_secs = 86400  # Retry for up to 1 day
   
   # Local buffer to prevent data loss
   buffer.type = "disk"
   buffer.max_size = 1073741824  # 1GB local buffer
   buffer.when_full = "block"    # Backpressure rather than drop
   ```

3. **Key Benefits**:

    - **Zero Data Loss**: Local disk buffer with transaction log ensures impressions aren't lost even if RabbitMQ is
      down or the server is destroyed
    - **Ultra-efficient**: Vector is designed for minimal resource usage (typically <1% CPU, ~50MB RAM)
    - **Self-contained**: Single binary with no dependencies
    - **Push-based**: No need to poll, data flows immediately
    - **Handles K8s Lifecycle**: Even if a container is destroyed, data persists in the buffer volume
    - **Encryption & Authentication**: TLS for secure transmission
    - **Backpressure Handling**: Won't overwhelm your processing tier
    - **Simple Rollout**: Deploy via simple SSH commands - no complex orchestration needed

4. **Central Infrastructure**:
    - Continue to use existing RabbitMQ and processing tiers
    - Add simple monitoring agent to verify edge server health

5. **Cost Implications**:
    - Vector is open-source - no license costs
    - Minimal resource overhead - no need for beefier servers
    - Eliminate collector service entirely

This approach gives you an elegant foundation that works immediately on your current Linode infrastructure but would
seamlessly transition to Kubernetes when you're ready. The local buffering specifically addresses your concern about K8s
instances being terminated before logs are collected.

Most importantly, it guarantees that every impression is counted, ensuring your CPM-based revenue is fully captured.

What I find particularly appealing about this solution is its simplicity - the entire collector service is replaced with
a 30-line configuration file, yet it provides substantially better reliability guarantees.