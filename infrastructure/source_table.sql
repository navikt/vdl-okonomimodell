use role sysadmin;

create or replace view okonomimodell.oebs.xxrtv_gl_segment_v as
select * from tlost__oebs_prod.apps.xxrtv_gl_segment_v;
create or replace view okonomimodell.oebs.xxrtv_gl_hierarki_v as 
select * from regnskap_raw.oebs.xxrtv_gl_hierarki_v__transient;
create or replace view okonomimodell.oebs.xxrtv_gl_hierarki_v__v1 as 
select * from tlost__oebs_prod.apps.xxrtv_gl_hierarki_v;