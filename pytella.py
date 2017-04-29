
import sys, os, re, time, datetime
from com.ziclix.python.sql import zxJDBC
from collections import OrderedDict
import glob
import copy

from xml import etree
from xml.etree.ElementTree import Element
from scriptella.tools.launcher import EtlLauncher

from arg_parser import parser_args

from sql import *

from helpers import (
  settings,
  fetch_to_array_dict,
  log,
  load_workflow,
  get_elapsed_time,
  save_text_to_file,
  get_exception_message,
  get_rec,
  DIR,
)

j_print = 'java.lang.System.out.print({text});'
j_println = 'java.lang.System.out.println({text});'

xml_prefix = b'''
<!DOCTYPE etl SYSTEM "http://scriptella.javaforge.com/dtd/etl.dtd">
'''


class Conn:
  def __init__(self, name, cred):
    self.name = name
    self.url = cred['url']
    self.username = cred['username']
    self.password = cred['password']
    self.get_type()
    self.driver = settings['drivers'][self.type_]['class']
    self.classpath = settings['drivers'][self.type_]['path']
    self.conn = None

  def connect(self):
    self.conn = zxJDBC.connect(self.url, self.username, self.password, self.driver)
  
  def execute(self, sql, ignore_error=False):
    cursor = self.conn.cursor(True)
    if ignore_error:
      try:cursor.execute(sql)
      except:log(get_exception_message())
    else:
      cursor.execute(sql)
    cursor.close()
  
  def query_array_dict(self, sql, size=None):
    cursor = self.conn.cursor(True)
    cursor.execute(sql)

    headers = tuple([k[0] for k in cursor.description])
    if size: data = [get_rec(row, headers) for row in cursor.fetchmany(size)]
    else: data = [get_rec(row, headers) for row in cursor.fetchall()]
    
    cursor.close()

    return data
    
  def get_type(self):
    url = self.url

    if 'postgresql' in url:
      self.type = "PostgreSQL"
      self.type_ = "postgresql"
      self.limit_templ = 'LIMIT {}'
      self.name_qual = '"'
      
    elif 'oracle' in url:
      self.type = "Oracle"
      self.type_ = "oracle"
      self.limit_templ = 'AND ROWNUM <= {}'
      self.name_qual = '"'

    elif 'sqlserver' in url:
      self.type = "Microsoft SQL Server"
      self.type_ = "sqlserver"
      self.limit_templ = 'TOP {}'
      self.name_qual = '"'

    elif 'mysql' in url:
      self.type = "MySQL"
      self.type_ = "mysql"
      self.limit_templ = 'LIMIT {}'
      self.name_qual = '`'
    

class Scriptella(object):
  def __init__(self, name, workflow):
    self.name = name
    self.settings = settings
    self.workflow = workflow
    self.create_temp_script()
  
  def create_temp_script(self):
    "Generate a timestamped script path"
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')
    self.etl_file_path = '{}/pythella_{}.xml'.format(settings['temp_path'], timestamp)

  def create_etl_file(self):
    '''
    Creates a Pass-through ETL for each table
    '''
    etl_branch = Element('etl')
    where_clause = ''
    # where_clause = 'WHERE ROWNUM < 100002'
    
    # script connection
    '''<connection id="script" driver="script"/>'''
    etl_branch.append(
      Element(
        'connection',id="script", driver="script"
      )
    )
    
    # Add Connections
    for conn_name, conn in connections.items():
      # for ETL transfer
      # print('Adding connection ' + conn_name)
      conn_branch = self.create_connection_branch(conn_name, conn)
      etl_branch.append(conn_branch)
      
      # for truncating target | need a separate connection
      conn2 = copy.deepcopy(conn)
      conn2.name = conn_name + '_'
      conn_branch = self.create_connection_branch(conn2.name, conn2, allow_truncate = True)
      # etl_branch.append(conn_branch)
    
    # Add CSV Connections
    if self.workflow.source_conn.lower() == 'csv':
      conn_branch = Element(
        'connection',
        id='csv_in',
        url=self.workflow.csv_file,
        driver="csv",
      )
      conn_branch.text = '''
      quote={quote}
      separator={delimiter}
      '''.format(
        quote=self.workflow.csv_quote,
        delimiter=self.workflow.csv_delimiter,
      )
      etl_branch.append(conn_branch)

    elif self.workflow.target_conn.lower() == 'csv':
      conn_branch = Element(
        'connection',
        id='csv_out',
        url=self.workflow.csv_file,
        driver="csv",
      )
      conn_branch.text = '''
      quote={quote}
      separator={delimiter}
      '''.format(
        quote=self.workflow.csv_quote,
        delimiter=self.workflow.csv_delimiter,
      )
      etl_branch.append(conn_branch)
      
    
    # Add Queries
    for mapping in self.workflow.mappings:
      source_target = mapping.get_combos()
      
      # Add status text
      etl_branch.append(
        self.create_script_branch("script", j_println.format(
          text="' --> Processing " + source_target[0] + "  to  " +  source_target[1] + "'"
          )
        )
      )

      source_arr = source_target[0].split('.')
      target_arr = source_target[1].split('.')
      source = {
        'database' : source_arr[0],
        'schema' : source_arr[1],
        'table' : source_arr[2],
      }
      target = {
        'database' : target_arr[0],
        'schema' : target_arr[1],
        'table' : target_arr[2],
      }
      
      src_sql = '''select * from {SCHEMA}.{TABLE} WHERE 1=1 {WHERE_CLAUSE} '''.format(
        SCHEMA=source['schema'],
        TABLE=source['table'],
        WHERE_CLAUSE=where_clause,
      )
      
      if mapping.sql:
        src_sql = mapping.sql

      if self.workflow.source_conn.lower() == 'csv':
        src_sql = ''
        source['database'] = 'csv_in'
      
      conn = connections[target['database']]
      options = '/*+ APPEND NOLOGGING */' if conn.type_ == 'oracle' else ''
      
      target_fields = self.get_table_fields(
        conn,
        target['schema'],
        target['table'],
      )

      if not target_fields:
        # need to create table
        # Get DDL
        s_conn = get_conn(source['database'])
        if parser_args.limited_perm:
          if source['schema'] != s_conn.username:
            sql = sql_ddl.oracle_copy.format(
              s_owner = source['schema'],
              s_table = source['table'],
              t_table = source['table'],
            )
            s_conn.execute(sql, ignore_error=True)

          sql = sql_ddl.oracle_.format(
            type='TABLE',
            object=source['table'],
          )
        else:
          sql = sql_ddl.oracle.format(
            type='TABLE',
            owner=source['schema'],
            object=source['table'],
          )
        data = s_conn.query_array_dict(sql)

        if not data:
          log('ERROR getting DDL for {}.{}.'.format(
            source['schema'],
            source['table']
          ))
          return None
        
        if not parser_args.create_table:
          log("Table {} does not exists on {}. Add flag '-createTable'".format(
            source['table'],
            target['database'],
          ))
          return None
        
        ddl = data[0]['ddl']
        print(ddl)
        sql = ddl.replace(source['schema'], target['schema'], 1).\
          replace(source['table'], target['table'], 1)
        # remove TABLESPACE
        sql = '\n'.join([l for l in sql.splitlines() if not l.strip().startswith('TABLESPACE ')])

        # Execute on target
        conn.execute(sql)

      if mapping.truncate:
        self.truncate_table(
          conn,
          target['schema'],
          target['table']
        )

      def get_new_field(f):
        if '*' in mapping.fields and not f in mapping.fields:
          new_field = "?{etl.getParameter('" + f.strip() + "')}"
        else:
          new_field = mapping.fields[f].strip().format(**self.workflow.expressions)
          if new_field == mapping.fields[f].strip():
            new_field = "?{etl.getParameter('" + new_field + "')}"
        return new_field
      
      if self.workflow.source_conn.lower() == 'csv':
        date_format = {}
        for col_format in self.workflow.csv_date_cols:
          field, fmt = col_format.split('=')
          date_format[field.lower()] = fmt
        
        date_enclose = lambda field: "TO_DATE(?{etl.getParameter('%s')}, '%s')" % (field, date_format[field.lower()]) \
            if field.lower() in date_format else "?{etl.getParameter('%s')}" % (field,)
        
        variable_fields = ','.join([date_enclose(f) for f in target_fields])
      elif len(mapping.fields) > 0:
        target_fields2 = mapping.fields.keys()
        if '*' in target_fields2:
          target_fields += [f for f in target_fields2 if not f in target_fields and f != '*']
        else:
          target_fields = target_fields2
        variable_fields = ','.join([get_new_field(f) for f in target_fields])
      else:
        variable_fields = ','.join(["?{etl.getParameter('" + f.strip() + "')}" for f in target_fields])
      
      tgt_sql = '''
      INSERT {OPTIONS} INTO {SCHEMA}.{TABLE}
      ({TGT_FIELDS})
      VALUES
      ({VAR_FIELDS})
      ;
      '''.format(
        OPTIONS=options,
        SCHEMA=target['schema'],
        TABLE=target['table'],
        TGT_FIELDS=', '.join(target_fields),
        VAR_FIELDS=variable_fields,
      )

      query_branch = self.create_query_branch(
          source['database'],
          src_sql,
          target['database'],
          tgt_sql
      )
      
      etl_branch.append(query_branch)
      etl_branch.append(self.create_script_branch("script", j_println.format(text="' Done!'")))
            
    xml_body = xml_prefix + etree.ElementTree.tostring(etl_branch)
    save_text_to_file(xml_body.decode('utf-8'), self.etl_file_path)

    return 1
  
  def create_connection_branch(self, name, conn, allow_truncate = False):
    '''
    <connection id="PRD" url="jdbc:oracle:thin:@//db1.com:1521/DB1_SERVICE" user="USER" password="pass" classpath="...lib/ojdbc6.jar" lazy-init="true">
    statement.fetchSize = 20000
    statement.batchSize = 20000
    transaction.isolation=SERIALIZABLE
    </connection>
    '''
    
    conn_branch = Element(
      'connection',
      id=name,
      url=conn.url,
      user=conn.username,
      password=conn.password,
      classpath="{}/{}".format(DIR,conn.classpath)
    )

    conn_branch.attrib['lazy-init'] = "true"

    if(not allow_truncate):
      # conn_branch.text = '''
      # statement.fetchSize = 20000
      # statement.batchSize = 20000
      # transaction.isolation=SERIALIZABLE
      # '''
      batchSize = int(parser_args.batchSize) if 'batchSize' in parser_args else 10000
      conn_branch.text = '''
      statement.fetchSize = {batchSize}
      statement.batchSize = {batchSize}
      '''.format(batchSize=batchSize)
    
    return conn_branch
      
  
  def create_query_branch(self, src_conn_id, src_sql, tgt_conn_id, tgt_sql ):
    '''<script connection-id="DBPRD">'''
    query_branch = Element('query')
    query_branch.attrib['connection-id'] = src_conn_id
    query_branch.text = src_sql
    
    # Row-num status
    text = j_print.format(text="'row#' + rownum + '  |'")
    script_branch = self.create_script_branch('script', text)
    script_branch.attrib['if'] = "rownum % 10000"
    query_branch.append(script_branch)
    
    # INSERT INTO
    script_branch = self.create_script_branch(tgt_conn_id, tgt_sql)
    query_branch.append(script_branch)
    
    return query_branch
  
  def create_script_branch(self, conn_id, text):
    '''<query connection-id="DBPRD">'''
    script_branch = Element('script')
    script_branch.attrib['connection-id'] = conn_id
    script_branch.text = text
    
    return script_branch
  
  def truncate_table(self, conn, schema, table):
    sql = '''TRUNCATE TABLE {OWNER}.{TABLE_NAME}'''.format(
      OWNER=schema,
      TABLE_NAME=table
    )
    log(sql)
    db_live_connections[conn.name].execute(sql)
  
  def get_table_fields(self, conn, schema, table):
    sql = '''
    SELECT
    OWNER, TABLE_NAME, COLUMN_NAME, TABLE_NAME || '.' || COLUMN_NAME as COMBO, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, COLUMN_ID
    from ALL_TAB_COLUMNS WHERE OWNER = '{OWNER}' and TABLE_NAME = '{TABLE_NAME}'
    '''.format(
      OWNER=schema,
      TABLE_NAME=table
    )
    instance=conn.name
    
    if conn.name not in db_live_connections:
      print('Connecting to ' + conn.name)
      db_live_connections[conn.name] = conn
      conn.connect()
    
    print('Getting Table schema for ' + schema + '.' + table)
    data = db_live_connections[conn.name].query_array_dict(sql)
    
    fields = [row['column_name'] for row in data]
    
    return fields
  

  def execute(self):
    log('Executing!')
    self.launcher = EtlLauncher()
    etl_file = self.launcher.resolveFile(None, self.etl_file_path)
    try:
      self.launcher.execute(etl_file)
      log('>')
      log('> ETL finished succesfully!')
      os.remove(self.etl_file_path)
    except:
      log(get_exception_message())
      log('>')
      log('> ETL failed! See XML -> ' + self.etl_file_path)

    finally:
      
      pass


class Workflow:
  
  def __init__(self, w_spec):
    get_val = lambda d,k,n: d[k] if k in d else n

    self.source_conn = w_spec['source']
    self.target_conn = w_spec['target']
    self.expressions = get_val(w_spec, 'expressions_db', {})
    self.truncate = w_spec['truncate'] if 'truncate' in w_spec else False
    self.mappings = []

    # CSV
    if self.source_conn.lower() == 'csv' or self.target_conn.lower() == 'csv':
      self.csv_file = w_spec['csv_file']
      self.csv_delimiter = w_spec['csv_delimiter'] if 'csv_delimiter' in w_spec else ','
      self.csv_quote = w_spec['csv_quote'] if 'csv_quote' in w_spec else '"'
      self.csv_date_cols = w_spec['csv_date_cols'].split(',') if 'csv_date_cols' in w_spec else []

    for table_map in w_spec['mappings']:
      mapping_spec = dict(
        truncate = self.truncate,
        s_table = None,
        t_table = None,
      )

      if isinstance(table_map, dict):
        for k,v in table_map.values()[0].items():
          mapping_spec[k] = v
        table_map = table_map.keys()[0]

      if '>' in table_map:
        s_table, t_table = [t.strip() for t in table_map.split('>')]
      else:
        s_table = t_table = table_map
      
      mapping_spec['s_table'] = s_table
      mapping_spec['t_table'] = t_table
      mapping = Mapping(mapping_spec, self)
      if mapping.valid: self.mappings.append(mapping)
      

class Mapping:
  
  def __init__(self, m_spec, workflow):
    self.valid = True
    self.source_conn = workflow.source_conn
    self.target_conn = workflow.target_conn
    self.source_table = m_spec['s_table']
    self.target_table = m_spec['t_table']
    self.truncate = m_spec['truncate']
    self.fields = OrderedDict()
    self.sql = m_spec['sql'] if 'sql' in m_spec else None

    if not all(['.' in self.source_table, '.' in self.target_table]):
      log('> ERROR: table names need "."! (SCHEMA.TABLE_NAME)')
      log('> Skipping mapping for {} to {}'.format(self.source_table, self.target_table))
      self.valid = False

    if 'fields' in m_spec:
      for field_map in m_spec['fields']:
        s_field, t_field = [f.strip() for f in field_map.split('>')]
        self.fields[t_field.upper()] = s_field

    self.s_combo = '{}.{}'.format(self.source_conn, self.source_table)
    self.t_combo = '{}.{}'.format(self.target_conn, self.target_table)
    
  def get_combos(self):
    return (self.s_combo, self.t_combo)


def get_conn(name):
  if name not in db_live_connections:
    print('Connecting to ' + name)
    db_live_connections[name] = connections[name]
    connections[name].connect()
  
  return db_live_connections[name]


limit = 0
db_live_connections={}

connections = {name: Conn(name, conn) for name, conn in settings['databases'].items()}

if parser_args.workflow:
  if not('\\' in parser_args.workflow or '/' in parser_args.workflow):
    parser_args.workflow = DIR + '/' + parser_args.workflow
  workflows = load_workflow(parser_args.workflow)

  for wf_name, wf_spec  in workflows.items():
    if wf_name.startswith('w_'):
      workflow = Workflow(wf_spec)
      log("Processing workflow {}".format(wf_name))
      etl = Scriptella(wf_name, workflow)
      if etl.create_etl_file():
        etl.execute()

