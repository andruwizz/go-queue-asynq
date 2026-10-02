# Go Queue Asynq

A comprehensive demonstration and reference implementation of distributed task queues in Go using [Asynq](https://github.com/hibiken/asynq) backed by Redis.

This project showcases asynchronous task processing patterns including priority queues, delayed jobs, scheduled cron-like tasks, unique task deduplication, retry policies with exponential backoff, panic recovery, dead-letter queue (DLQ) handling, CLI inspection, and a web-based monitoring dashboard.

## Features

- **Queue Priority Levels**: Strict priority processing across `critical`, `default`, and `low` queues.
- **Task Types & Handlers**:
  - `email:deliver`: Simulates transactional and notification emails.
  - `image:resize`: Demonstrates image processing tasks with context cancellation checks.
  - `webhook:deliver`: HTTP request dispatching with HTTP-status-aware retry decisions (`asynq.SkipRetry` on $4xx$, retry on $5xx$ or network errors).
  - `report:generate`: Periodic report generation with cancellation and deduplication support.
  - `data:sync`: Data synchronization tasks triggered by the scheduler.
- **Delivery Guarantees & Scheduling**:
  - Immediate execution.
  - Delayed processing (`ProcessIn` / `ProcessAt`).
  - Task deduplication via `Unique` TTL windows.
  - Task deadlines and timeouts.
  - Periodic cron-like scheduling with `asynq.Scheduler`.
- **Fault Tolerance & Reliability**:
  - Exponential backoff retry strategies (`RetryDelayFunc`).
  - Worker panic recovery and logging middleware.
  - Dead Letter Queue (DLQ) inspection, retry, and alerting capabilities (`DeadLetterQueueProcessor`).
  - Worker health check reporting.
- **Observability**:
  - Web UI using [Asynqmon](https://github.com/hibiken/asynqmon) on port `:8080/monitoring`.
  - CLI inspector tool for real-time queue metrics and task examination.

## Tech Stack

- **Language**: [Go](https://go.dev/) (1.26+)
- **Task Queue Library**: [Asynq](https://github.com/hibiken/asynq) (v0.26.0)
- **Broker / Storage**: [Redis](https://redis.io/) (7-alpine)
- **Monitoring UI**: [Asynqmon](https://github.com/hibiken/asynqmon) (v0.7.2)
- **Containerization**: [Docker](https://www.docker.com/) & Docker Compose
- **Cron / Scheduling**: [cron/v3](https://github.com/robfig/cron) (via Asynq Scheduler)

## Architecture Overview

```
                      +-------------------+
                      |   Task Producers  |
                      |   (cmd/client,    |
                      |   cmd/scheduler)  |
                      +---------+---------+
                                |
                                | Enqueue tasks
                                v
                      +-------------------+
                      |       Redis       |
                      | (Queues & States) |
                      +----+---------+----+
                           |         ^
           Poll & execute  |         | Inspect & manage
                           v         |
     +-----------------------+     +-----------------------+
     |      Task Workers     |     |  Monitoring & Admin   |
     |     (cmd/worker)      |     |  (cmd/webui: asynqmon |
     | - Concurrency control |     |   cmd/inspector: CLI  |
     | - Priority queues     |     |   DLQ management)     |
     | - Middleware pipeline |     +-----------------------+
     +-----------------------+
```

## Project Structure

```
├── cmd/
│   ├── client/
│   │   └── main.go          # Task producer: demonstrates immediate, delayed, unique, priority, & deadline tasks
│   ├── inspector/
│   │   └── main.go          # CLI queue inspector: real-time metrics for active, pending, retry, & scheduled tasks
│   ├── scheduler/
│   │   └── main.go          # Periodic task scheduler using cron expressions
│   ├── webui/
│   │   └── main.go          # Web monitoring dashboard (Asynqmon) and health check endpoint
│   └── worker/
│       └── main.go          # Task consumer server: concurrency control, strict queue priorities, & middleware
├── internal/
│   ├── config/
│   │   └── redis.go         # Redis client and cluster connection configuration
│   └── tasks/
│       ├── dlq_handler.go   # Dead-letter queue processor: inspection, batch re-enqueue, deletion, & alerting
│       ├── handlers.go      # Task handler implementations and worker middleware (logging, panic recovery)
│       ├── payloads.go      # Task payload definitions and task constructors
│       └── types.go         # Task type constants
├── docker-compose.yml       # Redis 7 Alpine service definition
├── Makefile                 # Make targets to execute each component
├── go.mod                   # Go module definition and dependencies
└── go.sum                   # Dependency checksums
```

---

## Prerequisites

- [Go](https://go.dev/dl/) 1.22+ (tested with Go 1.26)
- [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/) (for running Redis)

---

## Getting Started

### 1. Start Redis

Run the Redis service in the background using Docker Compose:

```bash
docker compose up -d
```

Verify Redis is healthy:

```bash
docker compose ps
```

### 2. Environment Variables (Optional)

The application connects to `localhost:6379` by default. You can customize the connection settings with environment variables:

| Variable | Default | Description |
|---|---|---|
| `REDIS_ADDR` | `localhost:6379` | Host and port of the Redis server |
| `REDIS_PASSWORD` | _(empty)_ | Authentication password for Redis |

---

## Running the Components

Use the provided `Makefile` targets or standard Go commands:

### Start the Worker Server
Starts the Asynq consumer server that processes tasks according to queue priorities.

```bash
make run-worker
# or: go run cmd/worker/main.go
```

### Enqueue Sample Tasks
Enqueues basic, delayed, prioritized, unique, and deadline-bound tasks to demonstrate producer functionality.

```bash
make run-client
# or: go run cmd/client/main.go
```

### Start the Scheduler
Runs the cron scheduler to periodically enqueue recurring tasks (such as hourly syncs or daily reports).

```bash
make run-scheduler
# or: go run cmd/scheduler/main.go
```

### Start the Web Monitoring UI
Launches the Asynqmon web interface.

```bash
make run-webui
# or: go run cmd/webui/main.go
```

Once running, access the dashboard at:
- Web Dashboard: [http://localhost:8080/monitoring](http://localhost:8080/monitoring)
- Health Check: [http://localhost:8080/health](http://localhost:8080/health)

### Inspect Queues via CLI
Queries Redis to display active, pending, retry, and scheduled task counts.

```bash
make run-inspector
# or: go run cmd/inspector/main.go
```

---

## Queue Configuration

The worker server defines three prioritized queues:

| Queue | Weight | Purpose |
|---|---|---|
| `critical` | 6 | High-priority jobs (e.g., password reset emails, health checks) |
| `default` | 3 | Standard background tasks (e.g., notifications, webhooks) |
| `low` | 1 | Background bulk workloads (e.g., scheduled reports, digests) |

With `StrictPriority: true`, tasks in the `critical` queue are always processed before tasks in `default`, which in turn take precedence over `low`.

---

## Dead Letter Queue (DLQ) & Error Handling

Failed tasks are managed through Asynq's built-in state transitions:

1. When a handler returns an error, Asynq retries it according to the task's `MaxRetry` setting.
2. If `asynq.SkipRetry` is wrapped in the returned error (e.g., invalid payload or HTTP 4xx response), retries are bypassed immediately.
3. If all retry attempts are exhausted, the task moves to the `archived` state (Dead Letter Queue).
4. The `DeadLetterQueueProcessor` in `internal/tasks/dlq_handler.go` provides methods to:
   - Inspect and count archived tasks.
   - Re-enqueue individual or all archived tasks.
   - Delete archived tasks.
   - Monitor archived volume and trigger alerts when thresholds are exceeded.

## Reference

This repository is a hands-on from this post  [OneUptime - How to Build a Job Queue in Go with Asynq and Redis](https://oneuptime.com/blog/post/2026-01-07-go-asynq-job-queue-redis/view#creating-the-worker-server)