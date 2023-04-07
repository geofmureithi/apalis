CREATE OR replace FUNCTION apalis.get_jobs( 
                worker_id TEXT,
                v_job_type TEXT
                v_jobs_count NUMERIC
            ) returns apalis.jobs AS $$
            DECLARE
                v_job_id text;
                v_job_rows[] apalis.jobs;
            BEGIN
                FOR counter IN 1 .. v_jobs_count
                    LOOP
                        SELECT * INTO v_job_rows from apalis.get_job();
                    END LOOP;
                
                RETURN v_job_rows;
        END;
        $$ LANGUAGE plpgsql volatile;