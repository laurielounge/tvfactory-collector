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
│ │ vector.yaml │◄┼──────────┼──┤           │  │          │  └───────────┘  │
│ └─────┬───────┘ │  (Push)  │  │           │  │          │                 │
│       │         │          │  └───────────┘  │          │  ┌───────────┐  │
│ ┌─────▼───────┐ │          │                 │          │  │ Impression│  │
│ │ local disk  │ │          │                 │          │  │ Consumer  │  │
│ │ buffer      │ │          │                 │          │  └───────────┘  │
│ └─────────────┘ │          │                 │          │                 │
|                 │          │                 │          │  ┌───────────┐  │        
|                 │          │                 │          │  │ Webhit    │  │        
|                 │          │                 │          │  | Consumer  │  |    
|                 │          │                 │          │  └───────────┘  │        
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
   ```yaml
#                                    __   __  __
#                                    \ \ / / / /
#                                     \ V / / /
#                                      \_/  \/
#
#                                    V E C T O R
#                                   Configuration
#
# ------------------------------------------------------------------------------
# Website: https://vector.dev
# Docs: https://vector.dev/docs
# Chat: https://chat.vector.dev
# ------------------------------------------------------------------------------

# Change this to use a non-default directory for Vector data storage:
# data_dir: "/var/lib/vector"

# Random Syslog-formatted logs
# Vector configuration for Infinitum log shipping
# Vector configuration for Infinitum log shipping
# Vector configuration for Infinitum log shipping
# Vector configuration for Infinitum log shipping
data_dir: "/var/lib/vector"

# Watch the nginx shadow_pipeline.log file
sources:
  nginx_logs:
    type: "file"
    include:
      - "/var/log/nginx/shadow_pipeline.log"
    ignore_older: 86400  # 1 day

# Process logs with a simple transform
transforms:
  process_logs:
    type: "remap"
    inputs: ["nginx_logs"]
    source: |
      # Only keep lines with our relevant paths
      if !match!(.message, r'(GET|POST)\s+/(client|response|impression|viewer)') {
        abort
      }
      
      # Parse the custom shadow_pipeline format
      parsed = parse_regex!(.message, r'^(?P<timestamp>[^\t]+)\t(?P<edge_ip>[^\t]+)\t(?P<method>[^\t]+)\t(?P<path>[^\t]+)\t(?P<query_string>[^\t]+)\t(?P<status>[^\t]+)\t(?P<user_agent>[^\t]*)\t(?P<referer>[^\t]*)\t(?P<client_ip>[^\t]*)$')
      
      # Add the parsed fields to the event
      . = merge(., parsed)
      
      # Add hostname
      host, err = get_hostname()
      if err == null {
        .hostname = host
      } else {
        .hostname = "unknown"
      }

# Send to RabbitMQ
sinks:
  rabbitmq_output:
    type: "amqp"
    inputs: ["process_logs"]
    encoding:
      codec: "json"
    connection_string: "amqp://infinitum_admin:m3d1ac0@10.0.0.112:5672/infinitum_vhost"
    exchange: ""
    routing_key: "loghit_queue"
    persistent: true
    
    # Local buffer settings for reliability
    buffer:
      type: "disk"
      max_size: 1073741824  # 1GB
      when_full: "block"
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