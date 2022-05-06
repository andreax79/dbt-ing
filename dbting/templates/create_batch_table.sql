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
row format delimited
  fields terminated by '{{ field_delimiter }}'
stored as inputformat
  'org.apache.hadoop.mapred.TextInputFormat'
outputformat
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location
  '{{ source_location }}'
tblproperties (
  'arecolumnsquoted'='false',
  'classification'='csv',
  'columnsordered'='true',
  'compressiontype'='none',
  'delimiter'='{{ field_delimiter }}',
  'typeofdata'='file'
);
