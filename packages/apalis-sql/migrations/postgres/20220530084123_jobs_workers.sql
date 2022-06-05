    CREATE SCHEMA apalis;
    
    CREATE TABLE IF NOT EXISTS apalis.workers (
        id TEXT NOT NULL,
        worker_type TEXT NOT NULL,
        storage_name TEXT NOT NULL,
        layers TEXT NOT NULL DEFAULT '',
        last_seen timestamptz not null default now()
    );

    CREATE INDEX IF NOT EXISTS Idx ON apalis.workers(id);

    CREATE UNIQUE INDEX IF NOT EXISTS unique_worker_id ON apalis.workers (id);

    CREATE INDEX IF NOT EXISTS WTIdx ON apalis.workers(worker_type);

    CREATE INDEX IF NOT EXISTS LSIdx ON apalis.workers(last_seen);

    CREATE TABLE IF NOT EXISTS apalis.jobs (
        job JSONB NOT NULL,
        id TEXT NOT NULL,
        job_type TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'Pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 25,
        run_at timestamptz NOT NULL default now(),
        last_error TEXT,
        lock_at timestamptz,
        lock_by TEXT,
        done_at timestamptz,
        CONSTRAINT fk_worker_lock_by FOREIGN KEY(lock_by) REFERENCES apalis.workers(id)
    );

    CREATE INDEX IF NOT EXISTS TIdx ON apalis.jobs(id);

    CREATE INDEX IF NOT EXISTS SIdx ON apalis.jobs(status);

    CREATE UNIQUE INDEX IF NOT EXISTS unique_job_id ON apalis.jobs (id);

    CREATE INDEX IF NOT EXISTS LIdx ON apalis.jobs(lock_by);

    CREATE INDEX IF NOT EXISTS JTIdx ON apalis.jobs(job_type);

    CREATE OR replace FUNCTION apalis.get_job( 
                worker_id TEXT,
                v_job_type TEXT
            ) returns apalis.jobs AS $$
            DECLARE
                v_job_id text;
                v_job_row apalis.jobs;
            BEGIN
                SELECT   id, job_type
                INTO     v_job_id, v_job_type
                FROM     apalis.jobs
                WHERE    status = 'Pending'
                AND      run_at < now()
                AND      job_type = v_job_type
                ORDER BY run_at ASC limit 1 FOR UPDATE skip LOCKED;
                
                IF v_job_id IS NULL THEN
                    RETURN NULL;
                END IF;

                UPDATE apalis.jobs
                    SET 
                        status = 'Running',      
                        lock_by = worker_id,
                        lock_at = now()
                    WHERE     id = v_job_id
                returning * INTO  v_job_row;
                RETURN v_job_row;
        END;
        $$ LANGUAGE plpgsql volatile;

        CREATE FUNCTION apalis.notify_new_jobs() returns trigger as $$
            BEGIN
                 perform pg_notify('apalis::job', 'insert');
                 return new;
            END;
        $$ language plpgsql;

        CREATE TRIGGER notify_workers after insert on apalis.jobs for each statement execute procedure apalis.notify_new_jobs();

