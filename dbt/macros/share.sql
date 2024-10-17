{% macro share_model(share) %}
    {% if target.database.lower() == "regnskap" %}

        {% set sql %}
          GRANT SELECT ON {{ target.database }}.{{ this.schema }}.{{ this.table }} TO SHARE {{ share }}
        {% endset %}

        {% set table = run_query(sql) %}

    {% endif %}

{% endmacro %}
