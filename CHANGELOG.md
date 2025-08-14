# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

## [0.7.3](https://github.com/geofmureithi/apalis/releases/tag/v0.7.3)

### Fixed

- **deps**: update rust crate redis to `0.32` ([#584](https://github.com/geofmureithi/apalis/pull/584))
- **deps**: update rust crate sentry-core to 0.42.0 ([#585](https://github.com/geofmureithi/apalis/pull/585))
- **deps**: update rust crate criterion to 0.7.0 ([#591](https://github.com/geofmureithi/apalis/pull/591))
- **deps**: update actions/checkout digest to 08eba0b ([#592](https://github.com/geofmureithi/apalis/pull/592))
- **deps**: update actions/checkout action to v5 ([#593](https://github.com/geofmureithi/apalis/pull/593))

## [0.7.2](https://github.com/geofmureithi/apalis/releases/tag/v0.7.2)

### Fixed

- **RedisStorage** the `stats` script is now compatible with dragonfly
- **examples** Prometheus example ([#562](https://github.com/geofmureithi/apalis/pull/562))
- **SqliteStorage** Event::Idle never trigger ([#571](https://github.com/geofmureithi/apalis/pull/571))
- **deps** : update rust crate redis to 0.31 ([#555](https://github.com/geofmureithi/apalis/pull/555))
- **deps** : update rust crate sentry-core to 0.38.0 ([#569](https://github.com/geofmureithi/apalis/pull/569))
- **deps** : update rust crate criterion to 0.6.0 ([#574](https://github.com/geofmureithi/apalis/pull/574))
- **error-handling** : ease the error type that is returned by a worker function ([#577](https://github.com/geofmureithi/apalis/pull/577))
- **PostgresStorage**: fix type error when updating jobs ([#539](https://github.com/geofmureithi/apalis/issues/539))
- **workflows** : improve permissions for github workflows ([#578](https://github.com/geofmureithi/apalis/issues/578))
- **deps** : update rust crate pprof to 0.15 ([#579](https://github.com/geofmureithi/apalis/issues/579))

## [0.7.1](https://github.com/geofmureithi/apalis/releases/tag/v0.7.1)

### Changed

- **SqliteStorage**: Consistent state fetch via returning update query and remove transactions ([#549](https://github.com/geofmureithi/apalis/pull/549))

### Fixed

- **PostgresStorage**: remove old, conflicting apalis.push_job database function ([#543](https://github.com/geofmureithi/apalis/pull/543))
- **deps**: update rust crate sentry-core to `0.37.0` ([#542](https://github.com/geofmureithi/apalis/pull/542))
- **apalis-sql**: Fix schedule job on sqlite and mysql + tests ([#556](https://github.com/geofmureithi/apalis/issues/556))
- **deps**: update rust crate metrics-exporter-prometheus to 0.17 ([#550](https://github.com/geofmureithi/apalis/issues/550))

### Tests

- **execute_next**: fix(tests): avoid execute_next timeout ([#548](https://github.com/geofmureithi/apalis/pull/548))

## [0.7.0](https://github.com/geofmureithi/apalis/releases/tag/v0.7.0)

### Added

- **api** add associated types to the Backend trait ([#516](https://github.com/geofmureithi/apalis/pull/516))
- **retry layer**: Integrate retry logic with task handling ([#512](https://github.com/geofmureithi/apalis/pull/512))
- **generic retry**: Persist check for tasks ([#498](https://github.com/geofmureithi/apalis/pull/498))
- **native TLS**: Add `async-std-comp-native-tls` and `tokio-comp-native-tls` features ([#525](https://github.com/geofmureithi/apalis/pull/525))
- **cron** : Introduce CronContext ([#488](https://github.com/geofmureithi/apalis/pull/488))
- **stepped tasks** : adds ability to execute stepped tasks ([#478](https://github.com/geofmureithi/apalis/pull/478))
- **SQL** : add support for job priority to SQL storages ([#533](https://github.com/geofmureithi/apalis/pull/533/))

### Fixed

- **PostgresStorage**: PostgresStorage get_jobs status conditional ([#524](https://github.com/geofmureithi/apalis/pull/524))
- **cron heartbeat**: Refactor and fix cron heartbeat ([#513](https://github.com/geofmureithi/apalis/pull/513))
- **RedisStorage**: Correct `running_count` statistic ([#506](https://github.com/geofmureithi/apalis/pull/506))
- **orphaned tasks**: Re-enqueue orphaned jobs before starting streaming ([#507](https://github.com/geofmureithi/apalis/pull/507))
- **deps**: Update Rust crate `redis` to `0.28` ([#495](https://github.com/geofmureithi/apalis/pull/495))
- **deps**: Update Rust crate `redis` to `0.29` and `deadpool-redis` to `0.20` ([#527](https://github.com/geofmureithi/apalis/pull/527))
- **features**: fix: ease apalis-core default features ([538](https://github.com/geofmureithi/apalis/pull/538))

### Tests

- **Integration tests**: Aborting jobs and panicking workers ([#508](https://github.com/geofmureithi/apalis/pull/508))
- **Worker tests**: Run a real worker in `testrunner` ([#509](https://github.com/geofmureithi/apalis/pull/509))

---

## [0.6.4](https://github.com/geofmureithi/apalis/releases/tag/v0.6.4)

### Changed

- **Version bump**: Increment to `v0.6.4` ([#500](https://github.com/geofmureithi/apalis/pull/500))

### Fixed

- **deps**: Update Rust crate `cron` to `0.15.0` ([#499](https://github.com/geofmureithi/apalis/pull/499))
- **deps**: Update Rust crate `sentry-core` to `0.36.0` ([#493](https://github.com/geofmureithi/apalis/pull/493))
- **vacuuming**: Handle vacuuming correctly for backends ([#491](https://github.com/geofmureithi/apalis/pull/491))

### Added

- **Redis keep_alive**: Prevent permanent failure after Redis restarts ([#492](https://github.com/geofmureithi/apalis/pull/492))

---

## [0.6.3](https://github.com/geofmureithi/apalis/releases/tag/v0.6.3)

### Changed

- **Version bump**: Increment to `v0.6.3` ([#490](https://github.com/geofmureithi/apalis/pull/490))

### Fixed

- **deps**: Update Rust crate `cron` to `0.14.0` ([#486](https://github.com/geofmureithi/apalis/pull/486))
- **deps**: Update Rust crate `sentry-core` to `0.35.0`
- **examples**: Several improvements and fixes ([#484](https://github.com/geofmureithi/apalis/pull/484), [#489](https://github.com/geofmureithi/apalis/pull/489))

---

## [0.6.2](https://github.com/geofmureithi/apalis/releases/tag/v0.6.2)

### Changed

- **Version bump**: Increment to `v0.6.2` ([#482](https://github.com/geofmureithi/apalis/pull/482))

### Fixed

- **Waker**: Handle only the latest waker ([#481](https://github.com/geofmureithi/apalis/pull/481))

---

## [0.6.1](https://github.com/geofmureithi/apalis/releases/tag/v0.6.1)

### Changed

- **Version bump**: Increment to `v0.6.1` ([#479](https://github.com/geofmureithi/apalis/pull/479))

### Fixed

- **Redis MQ**: Restore missing `message_id` ([#475](https://github.com/geofmureithi/apalis/pull/475))
- **Worker readiness**: Allow polling only when the worker is ready ([#472](https://github.com/geofmureithi/apalis/pull/472))

---

## [0.6.0](https://github.com/geofmureithi/apalis/releases/tag/v0.6.0)

### Changed

- **Version bump**: Introduce `v0.6.0` ([#459](https://github.com/geofmureithi/apalis/pull/459))
- **Branch name**: Rename `master` to `main` ([#456](https://github.com/geofmureithi/apalis/pull/456))
- **deps**: Various dependency updates (e.g., `tower` to `0.5`, `thiserror` to `v2`, etc.)

### Fixed

- **Redis**: Minor ACK bug ([#463](https://github.com/geofmureithi/apalis/pull/463))

---

## [0.5.5](https://github.com/geofmureithi/apalis/releases/tag/v0.5.5)

### Changed

- **Version bump**: `v0.5.5` ([#408](https://github.com/geofmureithi/apalis/pull/408))

### Fixed

- **tower**: Adjust for breaking changes ([#407](https://github.com/geofmureithi/apalis/pull/407))

---

## [0.5.4](https://github.com/geofmureithi/apalis/releases/tag/v0.5.4)

### Changed

- **Version bump**: `v0.5.4` ([#405](https://github.com/geofmureithi/apalis/pull/405))

### Fixed

- **deps**: Update Rust crate `tower` to `0.5` ([#396](https://github.com/geofmureithi/apalis/pull/396))
- **sqlx**: Update to `0.8.1` ([#403](https://github.com/geofmureithi/apalis/pull/403))

---

## [0.5.3](https://github.com/geofmureithi/apalis/releases/tag/v0.5.3)

### Changed

- **Version bump**: `v0.5.3` ([#328](https://github.com/geofmureithi/apalis/pull/328))
- **Repository metadata**: Add repository information ([#345](https://github.com/geofmureithi/apalis/pull/345))

### Fixed

- **Job context**: Ensure job context is tracked properly ([#289](https://github.com/geofmureithi/apalis/pull/289))

---

## [0.5.2](https://github.com/geofmureithi/apalis/releases/tag/v0.5.2)

### Changed

- **Version bump**: `v0.5.2` ([#322](https://github.com/geofmureithi/apalis/pull/322))

### Fixed

- **Timestamp**: Correct timestamp type for Postgres ([#321](https://github.com/geofmureithi/apalis/pull/321))
- **SQL config**: Allow SQL config values to be modified ([#320](https://github.com/geofmureithi/apalis/pull/320))

---

## [0.5.1](https://github.com/geofmureithi/apalis/releases/tag/v0.5.1)

### Changed

- **Version bump**: `v0.5.1` ([#265](https://github.com/geofmureithi/apalis/pull/265))

### Added

- **Shutdown terminator**: Graceful shutdown improvements ([#263](https://github.com/geofmureithi/apalis/pull/263))

### Fixed

- **deps**: Update Rust crate `redis` to `0.25`
- **deps**: Update Rust crate `cron` to `0.12.1`

---

## [0.5.0](https://github.com/geofmureithi/apalis/releases/tag/v0.5.0)

### Changed

- **Version bump**: `v0.5.0` ([#247](https://github.com/geofmureithi/apalis/pull/247))
- **Tokio**: Update to `1.36.0`

### Fixed

- **Examples**: Fix missing data in example ([#245](https://github.com/geofmureithi/apalis/pull/245))

---

## [0.4.9](https://github.com/geofmureithi/apalis/releases/tag/v0.4.9)

### Changed

- **Version bump**: `v0.4.9` ([#232](https://github.com/geofmureithi/apalis/pull/232))

### Fixed

- **deps**: Update Rust crate `futures-lite` to `2.2.0` ([#230](https://github.com/geofmureithi/apalis/pull/230))
- **MQ builder**: Various bug fixes and improvements ([#231](https://github.com/geofmureithi/apalis/pull/231))

---

## [0.4.8](https://github.com/geofmureithi/apalis/releases/tag/v0.4.8)

### Changed

- **Version bump**: `v0.4.8` ([#229](https://github.com/geofmureithi/apalis/pull/229))

### Added

- **Configurable concurrency** per worker ([#222](https://github.com/geofmureithi/apalis/pull/222))

### Fixed

- **deps**: Update Rust crate `smol` to `v2` ([#228](https://github.com/geofmureithi/apalis/pull/228))
- **deps**: Update Rust crate `async-trait` to `0.1.77` ([#225](https://github.com/geofmureithi/apalis/pull/225))

---

## [0.4.6](https://github.com/geofmureithi/apalis/releases/tag/v0.4.6)

### Changed

- **Version bump**: `v0.4.6` ([#197](https://github.com/geofmureithi/apalis/pull/197))

### Fixed

- **features**: Prevent forcing unintended dependencies by default ([#196](https://github.com/geofmureithi/apalis/pull/196))
- **worker spawn**: Use `executor.spawn` from within worker to start heartbeat tasks ([#195](https://github.com/geofmureithi/apalis/pull/195))

---

## [0.4.5](https://github.com/geofmureithi/apalis/releases/tag/v0.4.5)

### Changed

- **Version bump**: `v0.4.5` ([#181](https://github.com/geofmureithi/apalis/pull/181))

### Fixed

- **deps**: Update Rust crate `tokio` to `1.33.0` ([#180](https://github.com/geofmureithi/apalis/pull/180))
- **expose migrations**: Expose migrations for Postgres and SQLite ([#179](https://github.com/geofmureithi/apalis/pull/179))

---

## [0.4.4](https://github.com/geofmureithi/apalis/releases/tag/v0.4.4)

### Changed

- **Version bump**: `v0.4.4`
- **sqlx**: Update to `v0.7`
- **MySQL**: Fix typographical errors in queries

---

## [0.4.3](https://github.com/geofmureithi/apalis/releases/tag/v0.4.3)

### Changed

- **Version bump**: `v0.4.3`
- **deps**: Various updates (e.g., `tokio` to `1.29.1`, `pprof` to `0.12`, etc.)

---

## [0.4.2](https://github.com/geofmureithi/apalis/releases/tag/v0.4.2)

### Changed

- **Version bump**: `v0.4.2`
- **Publishing**: Fix missing `Cargo.toml` fields and workflows

### Fixed

- **Redis reconnection**: Resolve issues with Redis reconnect logic

---

## [0.4.1](https://github.com/geofmureithi/apalis/releases/tag/v0.4.1)

### Changed

- **Version bump**: `v0.4.1`

### Added

- Benchmarks for various storages (Postgres, MySQL, SQLite, Redis)

---

## [0.3.6](https://github.com/geofmureithi/apalis/releases/tag/v0.3.6)

### Changed

- **Version bump**: `v0.3.6`
- **Dependencies**: Various updates (e.g., `dashmap`, `env_logger`)

---

## [0.3.5](https://github.com/geofmureithi/apalis/releases/tag/v0.3.5)

### Changed

- **Version bump**: `v0.3.5`

### Fixed

- Removed `time` 0.1 from dependency tree
- Minor clippy fixes

---

## [0.3.4](https://github.com/geofmureithi/apalis/releases/tag/v0.3.4)

### Changed

- **Version bump**: `v0.3.4`
- **MySQL**: Implement heartbeat logic in `ReenqueueOrphaned` pulse

---

## [0.3.3](https://github.com/geofmureithi/apalis/releases/tag/v0.3.3)

### Changed

- **Version bump**: `v0.3.3`
- **Cron**: Introduce cron-based scheduling

---

## [0.3.1](https://github.com/geofmureithi/apalis/releases/tag/v0.3.1)

### Changed

- **Version bump**: `v0.3.1`
- **Docs & examples**: Various improvements

---

## [0.3.0](https://github.com/geofmureithi/apalis/releases/tag/v0.3.0)

### Changed

- **Major overhaul**: Removed Actix-based approach and introduced a tower-based job system
- **SQL**: Added Postgres, MySQL, SQLite support with `sqlx`
- **Redis**: Added Redis-based storage
- **Sentry**: Integrated Sentry for error tracking
- **Cron**: Basic cron scheduling support

---

> This **CHANGELOG** is a distilled view of the many commits shown in the log. If you need more granular details (e.g., every single merge or patch commit), you can expand each section to list additional commits or consult the full Git history.
