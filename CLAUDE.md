# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Test Commands

| Command | Description |
|---------|-------------|
| `mvn clean package` | Full build, compile + tests + package all modules |
| `mvn clean package -DskipTests` | Build without running tests |
| `mvn test` | Run all tests across all modules |
| `mvn test -pl redis-monitor-core` | Run tests for a single module |
| `mvn test -pl redis-monitor-core -Dtest=RedisConnectionFactoryTest` | Run a single test class |
| `cd redis-monitor-web && mvn spring-boot:run` | Start the Spring Boot web application |
| `mvn validate` | Validate project structure and POM files |

## Project Architecture

This is a Maven multi-module Java 8 project for Redis monitoring and analysis.

### Module Dependency Graph

```
redis-monitor-web
  -> redis-monitor-alert
  -> redis-monitor-analyzer
  -> redis-monitor-core
  -> redis-monitor-common

redis-monitor-alert
  -> redis-monitor-core
  -> redis-monitor-common

redis-monitor-analyzer
  -> redis-monitor-core
  -> redis-monitor-common

redis-monitor-core
  -> redis-monitor-common
```

**Important:** `redis-monitor-common` has no internal dependencies. All shared models, constants, and utilities live here to avoid circular dependencies.

### Module Responsibilities

- **redis-monitor-common** — Shared constants, data models, JSON utilities, and configuration classes. Keep this lightweight; no Spring or Redis client dependencies.
- **redis-monitor-core** — Redis connection management (Jedis + Lettuce), metrics collection (`INFO`, `STATS`, `MONITOR` sampling), and raw data retrieval. This is the only module that directly talks to Redis.
- **redis-monitor-analyzer** — Post-processing of collected data: slow log analysis, memory fragmentation analysis, key pattern detection, and command frequency statistics.
- **redis-monitor-alert** — Threshold-based alerting engine. Evaluates rules against metrics and triggers notifications. Depends on `core` for metric access.
- **redis-monitor-web** — Spring Boot 2.7 application serving REST APIs and WebSocket endpoints for the dashboard. Aggregates all other modules.

### Tech Stack

- **Java 8** — Language level and runtime target.
- **Maven 3.6+** — Build tool.
- **Spring Boot 2.7.18** — Web framework (Java 8 compatible).
- **Jedis 4.4.6** — Primary synchronous Redis client.
- **Lettuce 6.2.7.RELEASE** — Async/reactive Redis client (used for pub/sub and streaming).
- **JUnit 4.13.2** — Test framework.
- **Jackson 2.15.2** — JSON serialization.

### Key Conventions

- Base package: `com.yj.redis.monitor.{module-name}`
- Main Spring Boot class: `redis-monitor-web/src/main/java/com/yj/redis/monitor/web/RedisMonitorApplication.java`
- Application config: `redis-monitor-web/src/main/resources/application.yml`
- Redis connection settings are configured via `application.yml` under the `redis.monitor` prefix.

## Running the Application

1. Ensure Redis is running locally (default: `localhost:6379`).
2. Start the web module: `cd redis-monitor-web && mvn spring-boot:run`
3. The server starts on port `8080` by default.
