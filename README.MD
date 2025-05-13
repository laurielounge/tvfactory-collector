Of course — below is a full `README.md` tailored to your project, complete with a Mermaid diagram to illustrate the
elegant service-to-queue choreography.

---

````markdown
# TVFactory Collector Pipeline

This project forms the backend pipeline for collecting, classifying, resolving, and enriching log-derived traffic data —
particularly impressions and webhits — from bare-metal nginx servers. It transforms raw log entries into enriched
geographic and temporal data for downstream analytics.

---

## 📡 IP Addresses

| Name                      | IP Address       |
|---------------------------|------------------|
| collection-mediaserver    | 172.105.162.20   |
| processing-mediaserver    | 172.105.162.40   |
| datawarehouse-mediaserver | 172.105.162.93   |
| bvod-db                   | 194.195.126.89   |
| tvbvod1                   | 172.105.254.151  |
| tvbvod2                   | 172.105.255.194  |
| tvbvod3                   | 172.105.255.205  |
| tvbvod4                   | 172.105.255.36   |
| tvbvoddb1                 | 172.105.255.16   |
| tvbvoddb2                 | 45.79.239.60     |
| tvbvodsite1               | 194.195.121.207  |
| tvbvodsite2               | 172.105.175.91   |
| tvbvod5                   | 172.105.173.148  |
| tvbvod6                   | 172.105.173.170  |
| redis-edgeserver          | 172.105.189.75   |
| tvbvodsite3               | 192.46.221.193   |
| tvbvodsite4               | 172.105.187.245  |
| redis-processor           | 172.105.176.221  |

## 💡 Overview

This pipeline operates in discrete stages:

1. **Log Collection**  
   Logs are scraped from remote nginx hosts via SSH.
2. **Log Classification**  
   Log lines are parsed and classified into impressions or webhits.
3. **Message Queuing**  
   Classifications are pushed into RabbitMQ (`raw_*_queue`).
4. **Validation + Staging**  
   Consumer services validate and insert into MariaDB (assigning `id`s).
5. **Enrichment**  
   Another service enriches the DB records with geolocation and region intelligence using Redis + IP APIs.
6. **Resolved output** is emitted back into RabbitMQ (`resolved_*_queue`).

---

## 🔁 Data Flow Diagram (Mermaid)

```mermaid
flowchart TD
    subgraph Collect [Collector Stage]
        A[nginx Log Files] --> B[LogCollectorService]
        B -->|LPUSH| C[Redis loghit_queue]
    end

    subgraph Classify [Classifier Stage]
        C --> D[LoghitWorkerService]
        D -->|Publish| E[raw_impressions_queue]
        D -->|Publish| F[raw_webhits_queue]
    end

    subgraph Validate [Database Insertion Stage]
        E --> G[ImpressionConsumerService]
        F --> H[WebhitConsumerService]
        G -->|Insert + Publish| I[impressions_queue]
        H -->|Insert + Publish| J[webhits_queue]
    end

    subgraph Enrich [IP Resolver Stage]
        I --> K[ProcessingService]
        J --> K
        K -->|Geo resolve + Region| L[resolved_impressions_queue]
        K -->|Geo resolve + Region| M[resolved_webhits_queue]
    end
````

---

## 🚀 Services

| Service                     | Role                                  | Input                                | Output                                                  |
|-----------------------------|---------------------------------------|--------------------------------------|---------------------------------------------------------|
| `LogCollectorService`       | Scrapes log files from nginx servers  | SSH                                  | `loghit_queue` (Redis)                                  |
| `LoghitWorkerService`       | Parses and classifies logs            | `loghit_queue` (Redis)               | `raw_impressions_queue`, `raw_webhits_queue` (RabbitMQ) |
| `ImpressionConsumerService` | Validates and stores impressions      | `raw_impressions_queue` (RabbitMQ)   | `impressions_queue`                                     |
| `WebhitConsumerService`     | Validates and stores webhits          | `raw_webhits_queue` (RabbitMQ)       | `webhits_queue`                                         |
| `ProcessingService`         | Resolves IPs and attaches region info | `impressions_queue`, `webhits_queue` | `resolved_*_queue`                                      |

---

## 🗃️ Queues

| Queue                        | Type     | Description                         |
|------------------------------|----------|-------------------------------------|
| `loghit_queue`               | Redis    | Raw logs queued from SSH            |
| `raw_impressions_queue`      | RabbitMQ | Classified, unvalidated impressions |
| `raw_webhits_queue`          | RabbitMQ | Classified, unvalidated webhits     |
| `impressions_queue`          | RabbitMQ | DB-backed impressions with IDs      |
| `webhits_queue`              | RabbitMQ | DB-backed webhits with IDs          |
| `resolved_impressions_queue` | RabbitMQ | Fully enriched impressions          |
| `resolved_webhits_queue`     | RabbitMQ | Fully enriched webhits              |

---

## ⚙️ Running a Role

Each service is launched with:

```bash
python service.py --role=collector
```

Valid roles:

* `collector`
* `loghit`
* `impression`
* `webhit`
* `sequence` (runs all three in sequence: loghit → impression → webhit)

---

## 📌 Requirements

* Redis (for queueing and caching)
* RabbitMQ (for pub/sub message flow)
* MariaDB (for persistence)
* IP geolocation API (used in `ProcessingService`)
* `ssh` access to nginx boxes

---

## 🧪 Development Setup

1. Populate queues:

   ```bash
   rabbitmqadmin -u tvadmin -p secret -V /tvtag declare queue name=raw_impressions_queue durable=true
   ```

2. Drain a queue manually (for test):

   ```bash
   redis-cli RPOP loghit_queue
   ```

---

## 📎 Notes

* Queue names are hardcoded in some places. To change them, edit `LOG_QUEUE`, `raw_*_queue`, etc. in respective
  services.
* Use the `--once` flag to run a service in "batch and exit" mode for testing.

---

## 🕵️ Authored by Emma Peel

Exquisite pipelines for auditable traffic — shaken, not stirred.

```

---

Would you like this written to a file and returned to you directly?
```
