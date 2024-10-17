{% snapshot snapshot__xxrtv_fist_gl_segment_v %}

    {{
        config(
            target_schema="oebs",
            strategy="timestamp",
            updated_at="lastet_dato",
            unique_key="id"
        )
    }}

    with

        source as (select {{
                dbt_utils.star(
                    from=source("oebs","xxrtv_gl_segment_v"),
                    quote_identifiers=false,
                )
            }}
        from {{ source("oebs", "xxrtv_gl_segment_v") }}),

        metadata as (
            select
                {{
                    dbt_utils.generate_surrogate_key(
                        ["flex_value", "flex_value_set_name"]
                    )
                }} as id, *,
                current_date as lastet_dato
            from source
        )

    select *
    from metadata

{% endsnapshot %}
