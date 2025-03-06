DROP TRIGGER IF EXISTS notify_workers ON apalis.jobs;
DROP FUNCTION IF EXISTS apalis.notify_new_jobs;

CREATE FUNCTION apalis.notify_new_jobs() RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify('apalis::job::insert', NEW.job_type || '::' || NEW.id );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER notify_workers 
AFTER INSERT ON apalis.jobs 
FOR EACH ROW EXECUTE FUNCTION apalis.notify_new_jobs();
