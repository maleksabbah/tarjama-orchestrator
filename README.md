# tarjama-orchestrator

Job lifecycle and pipeline coordinator for the **Tarjama** Arabic ASR platform. It records jobs, drives the pipeline by publishing tasks to Kafka and consuming completion events, and hosts the live transcription WebSocket (`/ws/live`) backed by Redis sessions. It owns the `orchestrator_db` (jobs / tasks).

Built with FastAPI.

## Architecture

The service follows a clean, layered architecture where each layer has one responsibility and depends only on the layer beneath it:

- **Routes** — thin HTTP controllers. They translate between HTTP requests/responses and DTOs and call the service layer. No business logic lives here.
- **Services** — the business logic. Orchestrates the work, enforces rules, and coordinates repositories. Knows nothing about HTTP.
- **Repositories** — data access. Wraps the database (and external stores) behind a clean interface so the service layer never touches raw queries or clients directly.
- **Entities** — the domain / ORM models the repositories persist and return.
- **Dtos** — the request/response shapes exchanged at the API boundary, kept separate from internal entities.
- **Config** — wiring: database, Redis, Kafka, and other clients, plus environment configuration.

This separation keeps the HTTP layer swappable, the business logic testable in isolation, and the data layer free to change without touching the rest.

Beyond the HTTP routes, the orchestrator runs a background Kafka consumer loop (started in its lifespan) that advances the pipeline as completion events arrive, plus a WebSocket route for live sessions. The repository layer wraps the database, the Kafka producer/consumer, and Redis.

Part of a multi-service system — see the [platform overview](https://github.com/maleksabbah/tarjama-docker) for the full architecture, pipeline flow, and the other services.
