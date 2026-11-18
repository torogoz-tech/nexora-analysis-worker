# Nexora Analysis Worker

Version 0.4.0. This service claims repository sync and indexing jobs from PostgreSQL and analyzes pinned Git commits with the selected language profiles.

RabbitMQ messages on the analysis queue are wake-ups only. The worker drains eligible database jobs before consuming messages and after broker reconnection. PostgreSQL leases coordinate replicas, and manual RabbitMQ acknowledgements happen after database work completes successfully.

The process requires `NEXORA_RABBITMQ_URL`, a database connections file, an operator-owned worker settings file, a private source workspace, and a Git network policy. Defaults are `--connections .nexora/local/connections.json`, `--settings .nexora/local/analysis-worker.json`, and `--schema nexora_platform`. The container includes Git and runs as a non-root user; deployment configuration and secrets must be mounted out of band.

This worker owns repository sync and indexing only. Workflow transitions, release composition, knowledge processing, and email delivery are separate services.
