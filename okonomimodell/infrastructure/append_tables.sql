use role sysadmin
;
use schema okonomimodell_raw.oebs
;

create table if not exists segment as
select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_segment_v where 1=2;
create table if not exists hierarki as
select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_hierarki_v where 1=2;
create table if not exists period_status as
select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_period_status_v where 1=2;

create or replace task segment
    warehouse = okonomimodell_transformer
    schedule = 'using cron 0 21 * * * CET'
    as
    insert into okonomimodell_raw.oebs.segment
    select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_segment_v
;
alter task segment resume;

create or replace task hierarki
    warehouse = okonomimodell_transformer
    schedule = 'using cron 0 21 * * * CET'
    as
    insert into okonomimodell_raw.oebs.hierarki
    select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_hierarki_v
;
alter task hierarki resume;

create or replace task period_status
    warehouse = okonomimodell_transformer
    schedule = 'using cron 0 21 * * * CET'
    as
    insert into okonomimodell_raw.oebs.period_status
    select *, current_timestamp() as _loaded_at from tlost__oebs_prod.xxoko.xxrtv_gl_period_status_v
;
alter task period_status resume;
