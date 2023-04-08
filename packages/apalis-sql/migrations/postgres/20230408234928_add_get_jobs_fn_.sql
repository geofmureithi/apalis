DROP FUNCTION apalis.get_job(
        worker_id TEXT,
        v_job_type TEXT
    );

CREATE OR replace FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) returns setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE status = 'Pending'
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY run_at ASC
        limit v_job_count FOR
        UPDATE skip LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;