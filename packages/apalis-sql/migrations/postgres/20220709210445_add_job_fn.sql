CREATE OR REPLACE FUNCTION apalis.push_job(
    job_type text,
    job json DEFAULT NULL :: json,
    job_id text DEFAULT NULL :: text,
    status text DEFAULT 'Pending' :: text,
    run_at timestamptz DEFAULT NOW() :: timestamptz,
    max_attempts integer DEFAULT 25 :: integer
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
            uuid_in(
                md5(random() :: text || now() :: text) :: cstring
            ) INTO v_job_id;
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
                NULL
            )
            returning * INTO v_job_row;
            RETURN v_job_row;
END;
$$ LANGUAGE plpgsql volatile;


