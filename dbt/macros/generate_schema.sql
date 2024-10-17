{% macro generate_schema() %}
    {% set models_to_generate = codegen.get_models(
        directory="marts/dimensions", prefix="dim_"
    ) %}
    {{ codegen.generate_model_yaml(model_names=models_to_generate) }}
{% endmacro %}
