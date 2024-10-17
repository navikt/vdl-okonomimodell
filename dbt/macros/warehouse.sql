{% macro warehouse(full_refresh=target.warehouse) %}
    {% if not should_full_refresh() %} {{ return(target.warehouse) }} {% endif %}
    {% set limit_flag = var("limit_flag") %}
    {% if limit_flag == "y" %} {{ return(target.warehouse) }} {% endif %}
    {{ return(full_refresh) }}
{% endmacro %}
