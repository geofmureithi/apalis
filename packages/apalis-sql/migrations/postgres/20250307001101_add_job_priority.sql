ALTER TABLE apalis.jobs
ADD COLUMN priority INTEGER DEFAULT 0;

DROP FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer
    );

CREATE OR REPLACE FUNCTION apalis.get_jobs(
        worker_id TEXT,
        v_job_type TEXT,
        v_job_count integer DEFAULT 5 :: integer
    ) RETURNS setof apalis.jobs AS $$ BEGIN RETURN QUERY
UPDATE apalis.jobs
SET status = 'Running',
    lock_by = worker_id,
    lock_at = now()
WHERE id IN (
        SELECT id
        FROM apalis.jobs
        WHERE (status='Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at < now()
            AND job_type = v_job_type
        ORDER BY priority DESC, run_at ASC
        LIMIT v_job_count FOR
        UPDATE SKIP LOCKED
    )
returning *;
END;
$$ LANGUAGE plpgsql volatile;

CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer,
    priority integer DEFAULT 0 :: integer
) RETURNS apalis.jobs AS $$ 

        DECLARE 
            v_job_row apalis.jobs;
            v_job_id text;

        BEGIN
        IF job_type is not NULL and length(job_type) > 512 THEN raise exception 'Job_type is too long (max length: 512).' USING errcode = 'APAJT';
        END IF;

        IF max_attempts < 1 THEN raise exception 'Job maximum attempts must be at least 1.' USING errcode = 'APAMA';
        end IF;

        SELECT
            generate_ulid() INTO v_job_id;
        INSERT INTO
            apalis.jobs
        VALUES
            (
                job,
                v_job_id,
                job_type,
                status,
                0,
                max_attempts,
                run_at,
                NULL,
                NULL,
                NULL,
                NULL,
                priority
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;
