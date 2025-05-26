{{
    hist(
        from=source("oebs", "segment"),
        entity_key=["flex_value_id"],
        check_cols=["last_update_date"],
        loaded_at="_loaded_at",
    )
}}
