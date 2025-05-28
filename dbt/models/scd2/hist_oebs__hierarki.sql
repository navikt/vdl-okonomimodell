{{
    hist(
        from=ref("tmp_hist_src"),
        entity_key=["hierarchy_code", "flex_value_id"],
        check_cols=[
            "flex_value",
            "description",
            "flex_value_id_parent",
            "flex_value_parent",
            "description_parent",
            "flex_value_set_name",
        ],
        loaded_at="_loaded_at",
    )
}}
