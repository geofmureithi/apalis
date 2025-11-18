# apalis-sql

SQL utilities for background job processing with apalis

## Overview

This crate contains basic utilities shared by different sql backed storages.
| Backend | Crate | Docs | Features |
|---------|-------|------|----------|
| SQLite | `apalis-sqlite` | [docs](https://docs.rs/apalis-sqlite) | Stable |
| PostgreSQL | `apalis-postgres` | [docs](https://docs.rs/apalis-postgres) | Stable |
| MySQL | `apalis-mysql` | [docs](https://docs.rs/apalis-mysql) | Stable |
| SurrealDB | `apalis-surreal` | [docs](https://docs.rs/apalis-surreal) | Under Development |
| Diesel Compat | `apalis-sql` | [docs](https://docs.rs/apalis-sql#custom) | Under Development |

## Usage

You should no longer depend on this crate directly, the specific features have been moved eg for sqlite:

```toml
[dependencies]
apalis = "1"
apalis-sqlite = "1"
```

## Observability

You can track your jobs using [apalis-board](https://github.com/apalis-dev/apalis-board).
![Task](https://github.com/apalis-dev/apalis-board/raw/master/screenshots/task.png)

## Licence

MIT
