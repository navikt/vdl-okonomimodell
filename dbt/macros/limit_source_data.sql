{% macro limit_data() %}
    {% set limit_flag = var("limit_flag") %}
    {% if not is_incremental() and target.database.lower() != "regnskap" and limit_flag == "y" %}
        limit 100
    {% endif %}
{% endmacro %}
