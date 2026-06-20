# Distributed Job Scheduler

A distributed, fault-tolerant cron job scheduler built in Go — inspired by how systems like Google's internal cron infrastructure handle scheduling at scale. Built as a structured learning project to understand the core mechanics behind distributed systems: leader election, message queues, fault tolerance, and graceful failure recovery.

This is not a toy scheduler. It runs multiple scheduler instances that compete for leadership via `etcd`, survives process crashes, automatically retries failed jobs, and recovers from worker crashes without any manual intervention.

## Status

Actively in development. Phases 1–3 are complete and fully tested. Phase 4 (scalability) is in progress.

- [x] Phase 1 — Core scheduling engine
- [x] Phase 2 — Distributed queue with retries and dead-letter handling
- [x] Phase 3 — Leader election and fault tolerance
- [ ] Phase 4 — Sharding, rate limiting, backpressure *(queue sharding by job type already implemented)*
- [ ] Phase 5 — Observability (metrics, logging, tracing)
- [ ] Phase 6 — REST API, containerized deployment, load testing

## Architecture

```
                      ┌─────────────┐
                      │    etcd     │  ← leader election
                      └──────┬──────┘
                             │
              ┌──────────────┴──────────────┐
              │                              │
       ┌──────▼──────┐               ┌──────▼──────┐
       │ Scheduler 1  │               │ Scheduler 2  │
       │  (LEADER)    │               │ (follower)   │
       └──────┬───────┘               └──────────────┘
              │ publishes due jobs
              ▼
       ┌─────────────────────────────────────┐
       │              Redis                   │
       │  job_queue:short                     │
       │  job_queue:long                      │
       │  job_queue:default                   │
       │  job_queue_dead   (dead letter queue)│
       └───────┬───────────┬───────────┬──────┘
               │           │           │
        ┌──────▼───┐ ┌─────▼────┐ ┌────▼─────┐
        │ Worker    │ │ Worker   │ │ Worker   │
        │ (short)   │ │ (long)   │ │ (default)│
        └─────┬─────┘ └────┬─────┘ └────┬─────┘
              │            │            │
              └────────────┴────────────┘
                           │
                    ┌──────▼──────┐
                    │  PostgreSQL  │  ← jobs, job_runs
                    └──────────────┘

        ┌─────────────────────────┐
        │       Watchdog            │  ← detects crashed workers,
        │  (background goroutine)   │     re-queues stuck jobs
        └────────────────────────────┘
```

**Scheduler** — finds jobs due to run (`next_run_at <= NOW()`) and publishes them to a Redis queue. Never executes jobs directly. Only one scheduler instance is active at a time, decided by leader election.

**Workers** — pull jobs from their assigned queue and execute them. Each queue has a dedicated worker so a slow job type can't block a fast one.

**Watchdog** — runs independently, checking for job runs stuck in a `pending` state for too long (an indicator of a worker that crashed mid-execution). Re-queues them automatically.

**etcd** — coordinates leader election between scheduler instances. If the leader crashes, a follower takes over automatically within one TTL window (a few seconds).

## Key distributed systems concepts demonstrated

- **Leader election** — multiple scheduler instances race for a leadership key in etcd; only the winner publishes jobs
- **Automatic failover** — if the leader process dies, a standby instance detects this and takes over with no manual intervention
- **Split-brain prevention** — the scheduler watches its etcd session and immediately steps down if it loses leadership, even mid-execution, preventing two instances from acting as leader simultaneously
- **At-least-once delivery** — job execution is only marked complete after the worker finishes; a crash mid-job leaves a detectable trace rather than silently losing the job
- **Dead letter queue** — jobs that fail repeatedly (beyond a configurable retry limit) are routed to a separate queue for inspection, instead of retrying forever
- **Queue sharding** — jobs are routed to different queues by type, so long-running jobs can't starve fast ones of worker capacity
- **Self-healing goroutines** — worker and watchdog goroutines are wrapped in a supervisor loop that recovers from panics and restarts automatically, rather than permanently losing a worker to an unhandled crash
- **Graceful degradation under partial failure** — if Redis or Postgres becomes temporarily unavailable, workers back off and retry instead of busy-looping or crashing the whole process

## Tech stack

| Component | Technology | Why |
|---|---|---|
| Language | Go | Goroutines make concurrency simple; the language most distributed infrastructure (Kubernetes, Docker, etcd itself) is written in |
| Database | PostgreSQL | Persistent storage for job definitions and execution history |
| Queue | Redis | Lightweight pub/sub-style job queue, simpler to operate than Kafka at this scale |
| Coordination | etcd | Distributed key-value store used for leader election, same tool Kubernetes uses internally |

## Running locally

### Prerequisites
- Go 1.21+
- Docker Desktop

### 1. Start dependencies

```bash
docker run --name scheduler-db -e POSTGRES_PASSWORD=pass -p 5432:5432 -d postgres
docker run --name scheduler-redis -p 6379:6379 -d redis
docker run --name scheduler-etcd -p 2379:2379 -d quay.io/coreos/etcd:v3.5.0 /usr/local/bin/etcd --advertise-client-urls http://0.0.0.0:2379 --listen-client-urls http://0.0.0.0:2379
```

### 2. Set up the schema

Connect to Postgres (e.g. with TablePlus or `psql`) and run the contents of `schema.sql`.

### 3. Configure environment

Copy `.env.example` to `.env` and fill in your local values:

```bash
cp .env.example .env
```

### 4. Install dependencies and run

```bash
go mod download
go run main.go store.go scheduler.go queue.go election.go scheduler-1
```

### 5. Test failover (optional but recommended)

Open a second terminal and run a second instance:

```bash
go run main.go store.go scheduler.go queue.go election.go scheduler-2
```

`scheduler-2` will sit as a follower. Kill `scheduler-1` with `Ctrl+C` and watch `scheduler-2` take over leadership automatically within a few seconds.

## What's next

Continuing through the remaining phases — consistent hashing for dynamic worker scaling, rate limiting per job type, backpressure handling, Prometheus metrics, and eventually a REST API with Kubernetes deployment. Progress is tracked above and updated as each phase completes.
