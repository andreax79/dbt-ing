insert into {{ target_schema }}.{{ target_table }}(
{% for col in columns -%}
  {{ col.target_column }}{{ ',' if not loop.last }}
{% endfor %})
with source_table as (
  select
    row_number() over (partition by "$path") as row_num
{% for col in columns -%}
{%- if remove_quotes -%}
    ,replace({{ col.source_column }}, '{{ remove_quotes }}') as {{ col.source_column }}
{%- elif col.data_type.startswith('array') -%}
    ,case when cardinality({{ col.source_column }}) = 0 then null else {{ col.source_column }} end as {{ col.source_column }}
{%- else -%}
    ,{{ col.source_column }}
{% endif %}
{%- endfor -%}
  from
    {{ source_schema }}.{{ source_table }}
{% for k, v in partitions.items() -%}
    {{ 'where' if loop.first }}
    {{ k }} = '{{ v }}'{{ ' and' if not loop.last }}
{%- endfor %}
)
select
{% for col in columns %}
{%- if col.data_type.startswith('decimal') and decimal_format == 'sap' -%}
try_cast(
  trim(
    replace(
      replace(
        if(substr({{ col.source_formula or col.source_column }}, length({{ col.source_formula or col.source_column }})) = '-',
           '-' || trim(substr({{ col.source_formula or col.source_column }}, 1, length({{ col.source_formula or col.source_column }})-1)),
           {{ col.source_formula or col.source_column }}),
        '.', ''),
      ',', '.'
    )
  )
  as {{ col.data_type }}
)
{%- elif col.data_type == 'timestamp' and col.format ==  'unixtime' -%}
  cast(try(from_unixtime({{ col.source_formula or col.source_column }})) as timestamp)
{%- elif col.data_type == 'timestamp' and col.format ==  'unixtime_ms' -%}
  cast(try(from_unixtime(cast({{ col.source_formula or col.source_column }} as bigint) / 1000)) as timestamp)
{%- elif col.data_type == 'timestamp' -%}
  cast(try(parse_datetime({{ col.source_formula or col.source_column }}, '{{ (col.format or datetime_format)|replace("'","''") }}')) as timestamp)
{%- elif col.data_type == 'date' -%}
  cast(try(parse_datetime({{ col.source_formula or col.source_column }}, '{{ (col.format or date_format)|replace("'","''") }}')) as date)
{%- else -%}
  try(cast({{ col.source_formula or col.source_column }} as {{ col.data_type }}))
{%- endif %}{{ "," if not loop.last }}
{% endfor -%}
from source_table
{%- if where_condition %}
where
   {{ where_condition }}
{%- endif -%}
