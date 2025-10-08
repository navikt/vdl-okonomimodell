
{% docs __xxrtv_fist_gl_segment_v__ %}
# Hva inneholder tabellen?
Tabellen inneholder alle dimensjonsdata for alle dimensjoner. Hver `flex_value_set_name` er et sett av dimensjonsdata. 
# Hvordan lastes tabellen? 
Tabellen lastes i sin helhet til vårt `RAW` lage.
## ORA View definisjon
```
SELECT 
    ffvs.flex_value_set_name,
    ffv.flex_value_id,
    ffv.flex_value,
    ffvt.description,
    ffv.enabled_flag,
    ffv.start_date_active,
    ffv.end_date_active,
    ffv.summary_flag,
    SUBSTR( ffv.compiled_value_attributes, 3, 1) POSTERBAR, 
    SUBSTR( ffv.compiled_value_attributes, 1, 1) BUDSJETTERBAR,
    fifs.application_column_name,
    fifs.segment_num,
    ffv.attribute1,
    ffv.attribute2,
    ffv.attribute3,
    ffv.attribute4,
    ffv.attribute5,
    ffv.attribute6,
    ffv.attribute7,
    ffv.attribute8,
    ffv.attribute9,
    ffv.attribute10,
    ffv.attribute11,
    ffv.attribute12,
    ffv.attribute13,
    ffv.attribute14,
    ffv.attribute15,
    ffv.attribute16,
    ffv.attribute17,	-- lagt til attributtfelt til og med 17 for Ã¥ fÃ¥ med nytt attributt for avgiftskodene. 22.08.2014 Jan Meling
    (select h.hierarchy_name
     from   fnd_flex_hierarchies_vl h
     where  h.flex_value_set_id = ffv.flex_value_set_id
     and ffv.structured_hierarchy_level = h.hierarchy_id) gruppe,
    ffv.created_by,
    ffv.creation_date,
    ffv.last_updated_by,
    ffv.last_update_date
  FROM fnd_flex_values ffv,
    fnd_flex_values_tl ffvt,
    fnd_flex_value_sets ffvs,
    fnd_id_flex_segments fifs
  WHERE fifs.id_flex_num     = 50302 --gjeldene kontostruktur
  AND fifs.application_id    = 101
  AND fifs.id_flex_code      = 'GL#'
  AND ffvs.flex_value_set_id = fifs.flex_value_set_id
  AND ffv.flex_value_set_id  = ffvs.flex_value_set_id
  AND FFVT.LANGUAGE          = 'N'
  AND ffv.flex_value_id      = ffvt.flex_value_id
```
{% enddocs %}