{{
    scd2(
        relation=ref("test_scd2_src"),
        unique_key=["id"],
        check_cols=["val"],
        loaded_at="t",
    )
}}
