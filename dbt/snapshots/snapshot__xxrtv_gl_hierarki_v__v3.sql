{% snapshot snapshot__xxrtv_gl_hierarki_v__v3 %}

    {{
        config(
            target_schema="oebs",
            strategy="check",
            unique_key="id",
            check_cols="all",
            hard_deletes="new_record",
        )
    }}
    with
        src as (
            select
                {{
                    dbt_utils.star(
                        from=source("oebs", "xxrtv_gl_hierarki_v"),
                        quote_identifiers=false,
                        except=[
                            "_inbound__source_env",
                            "_inbound__run_id",
                            "_inbound__job_name",
                            "_inbound__version",
                            "_inbound__load_time",
                        ],
                    )
                }}

            from {{ source("oebs", "xxrtv_gl_hierarki_v") }}
        ),

        meta as (
            select
                {{
                    dbt_utils.generate_surrogate_key(
                        ["flex_value_id", "hierarchy_code"]
                    )
                }} as id, *, current_date as lastet_dato, 'snapshot' as meta_source
            from src
        ),

        final as (select * from meta)
    select *
    from final

{% endsnapshot %}
