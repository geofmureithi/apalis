DROP FUNCTION IF EXISTS apalis.push_job(
    job_type text,
    job json,
    job_id  text,
    status  text,
    run_at timestamptz,
    max_attempts integer
);