{% docs __xxrtv_fist_map_kontoplan__ %}
## ORA View definisjon
```
select
  CASE flv.lookup_type
  WHEN 'XXRTV_MAP_OA_ART_NY'
  THEN 'ABETAL'
  WHEN 'XXRTV_MAP_OA_BEVILGNING_NY'
  THEN 'ABETAL'
  WHEN 'XXRTV_MAP_UR_NY'
  THEN 'UR'
  END Kilde
  ,flv.lookup_type AS Mapping_type
  ,flv.lookup_code AS forsystem_kode
  ,decode(flv.lookup_type, 'XXRTV_MAP_OA_BEVILGNING_NY', flv.attribute15, flv.attribute2) AS OeBS_kode
  ,flv.attribute9 intern_artskonto_motpostering
  ,flv.start_date_active gyldig_fra_dato
  ,flv.end_date_active gyldig_til_dato
  ,flv.enabled_flag Aktivert
  ,flv.created_by
  ,flv.creation_date
  ,flv.last_updated_by
  ,flv.last_update_date
from fnd_lookup_values flv
where flv.language = 'N'
  AND flv.lookup_type in ('XXRTV_MAP_OA_BEVILGNING_NY','XXRTV_MAP_OA_ART_NY','XXRTV_MAP_UR_NY')
```
{% enddocs %}