DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    IF NEW.run_at <= now() THEN
        PERFORM pg_notify(
            'apalis::job::insert',
            json_build_object(
                'job_type', NEW.job_type,
                'id', NEW.id
            )::text
        );
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers 
AFTER INSERT ON apalis.jobs 
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();
