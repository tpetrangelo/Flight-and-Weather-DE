{% macro whoami() %}
  {% set q %}
    select
      current_user() as user,
      current_role() as role,
      current_database() as database,
      current_schema() as schema;
  {% endset %}
  {% do log(run_query(q).columns[0].values()[0] ~ ' | ' ~ run_query(q).columns[1].values()[0] ~ ' | ' ~ run_query(q).columns[2].values()[0] ~ ' | ' ~ run_query(q).columns[3].values()[0], info=true) %}
{% endmacro %}
