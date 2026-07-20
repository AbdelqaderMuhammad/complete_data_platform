{% materialization iceberg_table, adapter='duckdb' %}

  {%- set target_relation = this -%}
  {%- set partition_by = config.get('partition_by', none) -%}

  {% call statement('drop_existing') %}
    drop table if exists {{ target_relation }};
  {% endcall %}
  {{ adapter.commit() }}

  {% call statement('main') %}
    create table {{ target_relation }} as (
      {{ sql }}
    );
  {% endcall %}
  {{ adapter.commit() }}

  {% if partition_by %}
    {% call statement('partition') %}
      alter table {{ target_relation }} set partitioned by ({{ partition_by }});
    {% endcall %}
    {{ adapter.commit() }}
  {% endif %}

  {{ run_hooks(post_hooks) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}