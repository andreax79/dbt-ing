create external table {{ target_schema }}.{{ target_table }} (
{% for column in columns | selectattr('partition', '!=', 'yes') -%}
  `{{ column.target_column }}` {{ column.data_type }}{{ "," if not loop.last }}
{% endfor %}
)
partitioned by (
{% for column in columns | selectattr('partition', '==', 'yes') -%}
  `{{ column.target_column }}` {{ column.data_type }}{{ "," if not loop.last }}
{% endfor %}
)
row format serde
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
stored as inputformat
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
outputformat
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
location
  '{{ target_location }}'
tblproperties (
  'classification'='parquet'
)
