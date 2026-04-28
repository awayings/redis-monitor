# Redis Monitor

Redis monitoring and analysis platform built with Java 8.

## Modules

- **redis-monitor-common** - Common utilities, constants, and shared models.
- **redis-monitor-core** - Core monitoring engine: Redis connections, metrics collection, and data sampling.
- **redis-monitor-analyzer** - Slow query analysis, memory analysis, and key pattern analysis.
- **redis-monitor-alert** - Alert engine: rules, notifications, and thresholds.
- **redis-monitor-web** - Web API and dashboard backend (Spring Boot).

## Build

```bash
mvn clean package -DskipTests
```

## Run

```bash
cd redis-monitor-web
mvn spring-boot:run
```

## Test

```bash
mvn test
```

## Requirements

- Java 8+
- Maven 3.6+
- Redis 4.0+
