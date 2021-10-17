# Rusty tub

> [Testcontainers](https://www.testcontainers.org/)-inspired integration testing library atop of Docker containers.

## Early development

It's an immature library, feel free to fork it to fix and add new features like additional integrations.

## Features

- Initialization of a generic docker cotainer in a test exposing needed ports
- Using bridge Docker network to isolate test environment
- Container readiness checking
- Automatic image pulling
- Possibility to run tests in parallel
- Proper containers cleanup at the end of testing
- Possibility of leaving running containers for debugging purposes

## Supported integrations

- [x] [ClickHouse](https://hub.docker.com/r/yandex/clickhouse-server/)
- [ ] PostgreSQL

## Example

Please check `tests/`.
