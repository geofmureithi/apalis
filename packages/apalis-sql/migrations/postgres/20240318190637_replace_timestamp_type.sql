alter table apalis.jobs
    alter column run_at
        drop default;

alter table apalis.jobs
alter column run_at type bigint using (extract(epoch from run_at) * 1000);

alter table apalis.jobs
    alter column run_at
        set default (extract(epoch from now()) * 1000);