{% snapshot snapshot__xxrtv_fist_gl_segment_v %}

    {{
        config(
          target_schema='oebs',
          strategy='timestamp',
          unique_key='flex_value_id',
          updated_at='last_update_date',
        )
    }}

    select 
    {{
        dbt_utils.star(
            from=source("oebs","xxrtv_gl_segment_v"),
            quote_identifiers=false,
            except=[
                "_inbound__source_env",
                "_inbound__run_id",
                "_inbound__job_name",
                "_inbound__version",
                "_inbound__load_time",
            ]
        )
    }},
    current_date as lastet_dato,
    'snapshot' as meta_source
    from {{ source("oebs", "xxrtv_gl_segment_v") }}

{% endsnapshot %}