from helpers import dict2

sql_select_columns = dict2(
    mysql = "select    table_catalog,    table_schema,    table_name,    column_name,    data_type,    ordinal_position,    column_default,    is_nullable,    character_maximum_length,    character_octet_length,    numeric_precision,    numeric_scale,    character_set_name,    collation_name,    column_type,    column_key,    extra,    privileges,    column_comment from    information_schema.columns where table_name = ? and table_schema = ? " + "order by table_catalog,table_schema,table_name,ordinal_position",

  sqlserver = "select    table_catalog,	table_schema,	table_name,	column_name,	data_type,	ordinal_position,	column_default,	is_nullable,	character_maximum_length,	character_octet_length,	numeric_precision,	numeric_precision_radix	numeric_scale,	datetime_precision from    information_schema.columns where table_name = ? and table_schema = ? order by table_catalog, table_schema, table_name, ordinal_position",

  postgres = "select    table_catalog,	table_schema,	table_name,	column_name,	data_type,	ordinal_position,	column_default,	is_nullable,	character_maximum_length,	character_octet_length,	numeric_precision,	numeric_precision_radix	numeric_scale,	datetime_precision from    information_schema.columns where table_name = ? and table_schema = ? order by table_catalog, table_schema, table_name, ordinal_position",

  oracle = "select owner as table_catalog, owner,	table_name,	column_name,	data_type,	column_id ,	data_type_mod,	data_type_owner,	data_length,	data_precision,	data_scale,	nullable from all_tab_columns where lower(table_name) = lower(?) and lower(owner) = lower(?) order by owner, table_name, column_id",
)

sql_insert = dict2(
  _into = "INTO {table} ({names}) VALUES {values}",
  oracle = "INSERT ALL {_into} SELECT 1 FROM DUAL",
  sqlserver = 'INSERT INTO {table} ({names}) VALUES {values} ',
  postgres = 'INSERT INTO {table} ({names}) VALUES {values} ',
  mysql = 'INSERT INTO {table} ({names}) VALUES {values} ',

)

sql_ddl = dict2(
  oracle = '''select dbms_metadata.get_ddl('{type}', '{object}', '{owner}') as "DDL" from dual''',
  oracle_ = '''select dbms_metadata.get_ddl('{type}', '{object}') as "DDL" from dual''',
  oracle_copy = '''create table {t_table} as select * from {s_owner}.{s_table} where ROWNUM < 100''',
)