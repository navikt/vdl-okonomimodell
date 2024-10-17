{% docs __xxrtv_gl_hierarki_v__ %}
## ORA View definisjon
```
SELECT x.flex_value_set_id
    , x.hierarchy_code
    , x.flex_value_id
    , x.flex_value
	, x.description
    , DECODE(x.flex_value,'T',NULL,x.flex_value_id_parent) flex_value_id_parent
    , DECODE(x.flex_value,'T',NULL,ffvparent.flex_value) flex_value_parent
	, DECODE(x.flex_value,'T',NULL,ffvtlparent.description) description_parent
	, ffvs.flex_value_set_name
  FROM
  (
  select ffv.flex_value_set_id
    , NULL hierarchy_code
    , ffv.flex_value_id
    , ffv.flex_value
	, ffvtl.description
    , xxrtv_fist_rapport_hjelp.get_hierarchy_parent(ffv.flex_value_set_id, ffv.flex_value_id, NULL, ffv.summary_flag)  flex_value_id_parent
from fnd_flex_values ffv
	, fnd_flex_values_tl ffvtl 
where ffv.summary_flag='N'
and ffvtl.flex_value_id=ffv.flex_value_id
and ffvtl.language='N'
UNION
select ffv.flex_value_set_id
    , ffh.hierarchy_code hierarchy_code
    , ffv.flex_value_id
    , ffv.flex_value
	, ffvtl.description
    , xxrtv_fist_rapport_hjelp.get_hierarchy_parent(ffv.flex_value_set_id, ffv.flex_value_id, ffh.hierarchy_id, ffv.summary_flag)  flex_value_id_parent
from fnd_flex_values ffv
    , fnd_flex_hierarchies ffh
	, fnd_flex_values_tl ffvtl 
where ffv.summary_flag='N'
and ffv.flex_value_set_id=ffh.flex_value_set_id
and ffvtl.flex_value_id=ffv.flex_value_id
and ffvtl.language='N'
and exists(
    select 1 
    from fnd_flex_values ffvparent 
        , fnd_flex_value_norm_hierarchy ffvnh
    where ffvparent.structured_hierarchy_level=ffh.hierarchy_id
    and ffvparent.flex_value_set_id=ffh.flex_value_set_id
    and ffvparent.flex_value_set_id=ffv.flex_value_set_id
    and ffvparent.flex_value_set_id=ffvnh.flex_value_set_id
    and ffvparent.flex_value=ffvnh.parent_flex_value
    and ffvnh.range_attribute='C'
    and ffv.flex_value BETWEEN ffvnh.child_flex_value_low AND ffvnh.child_flex_value_high
    )
UNION
select ffv.flex_value_set_id
    , NULL hierarchy_code
    , ffv.flex_value_id
    , ffv.flex_value
	, ffvtl.description
    , xxrtv_fist_rapport_hjelp.get_hierarchy_parent(ffv.flex_value_set_id, ffv.flex_value_id, NULL, ffv.summary_flag)  flex_value_id_parent
from fnd_flex_values ffv
	, fnd_flex_values_tl ffvtl 
where ffv.summary_flag='Y'
and ffv.structured_hierarchy_level IS NULL
and ffvtl.flex_value_id=ffv.flex_value_id
and ffvtl.language='N'
UNION
select ffv.flex_value_set_id
    , ffh.hierarchy_code hierarchy_code
    , ffv.flex_value_id
    , ffv.flex_value
	, ffvtl.description
    , xxrtv_fist_rapport_hjelp.get_hierarchy_parent(ffv.flex_value_set_id, ffv.flex_value_id, ffh.hierarchy_id, ffv.summary_flag)  flex_value_id_parent
from fnd_flex_values ffv
    , fnd_flex_hierarchies ffh
	, fnd_flex_values_tl ffvtl 
where ffv.summary_flag='Y'
and ffv.structured_hierarchy_level IS NOT NULL
and ffv.flex_value_set_id=ffh.flex_value_set_id
and ffv.structured_hierarchy_level=ffh.hierarchy_id
and ffvtl.flex_value_id=ffv.flex_value_id
and ffvtl.language='N'
) x
, fnd_flex_values ffvparent
, fnd_flex_values_tl ffvtlparent 
, fnd_flex_value_sets ffvs
WHERE x.flex_value_id_parent = ffvparent.flex_value_id
AND x.flex_value_set_id = ffvparent.flex_value_set_id
and ffvtlparent.flex_value_id=ffvparent.flex_value_id
and ffvtlparent.language='N'
and ffvs.flex_value_set_id=ffvparent.flex_value_set_id
AND exists --sørg for å bare ta med verdisett som inneholder verdien T. Dette viewet benyttes for kontostrengsverdisett
(SELECT 1
FROM fnd_flex_values ffvT
WHERE ffvT.flex_value_set_id=x.flex_value_set_id
AND ffvT.flex_value='T')
order by x.flex_value_set_id
    , x.hierarchy_code
    , x.flex_value_id
```
{% enddocs %}