CREATE TABLE IF NOT EXISTS workers (
    id varchar(36) NOT NULL,
    worker_type varchar(200) NOT NULL,
    storage_name varchar(200) NOT NULL,
    layers VARCHAR(1000) NOT NULL DEFAULT '',
    last_seen datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX Idx ON workers(id);

CREATE INDEX WTIdx ON workers(worker_type);

CREATE INDEX LSIdx ON workers(last_seen);

CREATE TABLE IF NOT EXISTS jobs (
    job JSON NOT NULL,
    id varchar(36) NOT NULL,
    job_type varchar(200) NOT NULL,
    status varchar(20) NOT NULL DEFAULT 'Pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 25,
    run_at datetime DEFAULT CURRENT_TIMESTAMP,
    last_error varchar(1000) DEFAULT NULL,
    lock_at datetime DEFAULT NULL,
    lock_by varchar(36) DEFAULT NULL,
    done_at datetime DEFAULT NULL,
    PRIMARY KEY (id),
    FOREIGN KEY (lock_by) REFERENCES workers(id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE INDEX job_id ON jobs(id);

CREATE INDEX job_status ON jobs(status);

CREATE INDEX LIdx ON jobs(lock_by);

CREATE INDEX JTIdx ON jobs(job_type);