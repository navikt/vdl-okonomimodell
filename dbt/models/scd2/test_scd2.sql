{{
    scd2(
        relation=ref("test_scd2_src"),
        entity_key=["id"],
        check_cols=["val"],
        loaded_at="t",
    )
}}
