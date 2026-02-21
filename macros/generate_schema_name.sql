{% macro generate_schema_name(custom_schema_name, node) %}

  {% if custom_schema_name is none %}
    {# No explicit schema, so keep default behavior (do not overwrite target.schema) #}
    {{ target.schema }}
  {% else %}
    {# Explicit schema defined, apply environment prefix #}
    {{ env_var('DBT_ENV_NM') }}_{{ custom_schema_name }}
  {% endif %}

{% endmacro %}