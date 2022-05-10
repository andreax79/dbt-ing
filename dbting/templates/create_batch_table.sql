create external table {{ source_schema }}.{{ source_table }} (
{% for column in columns | selectattr('partition', '!=', 'yes') -%}
  `{{ column.source_column }}` varchar(65535){{ "," if not loop.last }}
{% endfor %}
)
partitioned by (
{% for column in columns | selectattr('partition', '==', 'yes') -%}
  `{{ column.target_column }}` varchar(65535){{ "," if not loop.last }}
{% endfor %}
)
{% if source_format == 'json' %}
row format serde 'org.openx.data.jsonserde.JsonSerDe'
{% else %}
row format delimited
  fields terminated by '{{ field_delimiter }}'
{% endif %}
stored as inputformat
  'org.apache.hadoop.mapred.TextInputFormat'
outputformat
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location
  '{{ source_location }}'
tblproperties (
  'arecolumnsquoted'='false',
  'classification'='{{ source_format }}',
  'columnsordered'='true',
  'compressionType'='{{ compression_type or 'none' }}',
{%- if source_format != 'json' -%}
  'delimiter'='{{ field_delimiter }}',
{%- endif %}
  'typeOfData'='file'
);
