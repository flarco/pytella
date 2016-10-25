
import sys, os, re, time, datetime
from com.ziclix.python.sql import zxJDBC
import glob
import copy

from xml import etree
from xml.etree.ElementTree import Element
from scriptella.tools.launcher import EtlLauncher

from helpers import (
  settings,
  fetch_to_array_dict,
  log,
  get_elapsed_time,
  save_text_to_file,
  get_exception_message,
  get_rec,
  DIR,
)

status_text = "java.lang.System.out.println({text});"

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

    if 'postgres' in url:
      self.type = "PostgreSQL"
      self.type_ = "postgres"
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
  def __init__(self, name, sources_targets):
    self.name = name
    self.settings = settings
    self.sources_targets = sources_targets
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
      print('Adding connection ' + conn_name)
      conn_branch = self.create_connection_branch(conn_name, conn)
      etl_branch.append(conn_branch)
      
      # for truncating target | need a separate connection
      conn2 = copy.deepcopy(conn)
      conn2.name = conn_name + '_'
      conn_branch = self.create_connection_branch(conn2.name, conn2, allow_truncate = True)
      # etl_branch.append(conn_branch)
        
    
    # Add Queries
    for source_target in self.sources_targets:
      print('Adding query '  + source_target[0] + '  to  ' +  source_target[1])
      
      # Add status text
      etl_branch.append(
        self.create_script_branch("script", status_text.format(
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
      
      src_sql = '''select * from {SCHEMA}.{TABLE} {WHERE_CLAUSE} ;'''.format(
        SCHEMA=source['schema'],
        TABLE=source['table'],
        WHERE_CLAUSE=where_clause,
      )
      
      
      
      conn = connections[target['database']]
      options = '/*+ APPEND NOLOGGING */' if conn.type_ == 'oracle' else ''
      
      target_fields = self.get_table_fields(
        conn,
        target['schema'],
        target['table']
      )
      # variable_fields = ','.join(['?' + f.strip()for f in target_fields.split(',')])
      # ?{etl.getParameter('column one')}
      variable_fields = ','.join(["?{etl.getParameter('" + f.strip() + "')}" for f in target_fields.split(',')])
      
      tgt_sql = '''
      INSERT {OPTIONS} INTO {SCHEMA}.{TABLE}
      ({TGT_FIELDS})
      VALUES
      ({VAR_FILES})
      ;
      '''.format(
        OPTIONS=options,
        SCHEMA=source['schema'],
        TABLE=source['table'],
        TGT_FIELDS=target_fields,
        VAR_FILES=variable_fields,
      )
      
      query_branch = self.create_query_branch(
          source['database'],
          src_sql,
          target['database'],
          tgt_sql
      )
      
      etl_branch.append(query_branch)
            
    xml_body = xml_prefix + etree.ElementTree.tostring(etl_branch)
    save_text_to_file(xml_body.decode('utf-8'), self.etl_file_path)
  
  def create_connection_branch(self, name, conn, allow_truncate = False):
    '''
    <connection id="STGPRD" url="jdbc:oracle:thin:@//stgprd.saic.com:1521/STGPRD" user="ETL_USER" password="pass" classpath="/informatica/powercenter/pcenter/etl/scriptella-1.1/lib/ojdbc6.jar" lazy-init="true">
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
      conn_branch.text = '''
      statement.fetchSize = 2000
      statement.batchSize = 2000
      '''
    
    return conn_branch
      
  
  def create_query_branch(self, src_conn_id, src_sql, tgt_conn_id, tgt_sql ):
    '''<script connection-id="DBPRD">'''
    query_branch = Element('query')
    query_branch.attrib['connection-id'] = src_conn_id
    query_branch.text = src_sql
    
    # Row-num status
    text = status_text.format(text="'row#' + rownum")
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
    
    fields = ','.join([row['column_name'] for row in data])
    
    return fields
  

  def execute(self):
    self.launcher = EtlLauncher()
    etl_file = self.launcher.resolveFile(None, self.etl_file_path)
    try:
      self.launcher.execute(etl_file)
      log('>')
      log('> ETL finished succesfully!')
    except:
      log(get_exception_message())
      log('>')
      log('> ETL failed!')
    finally:
      os.remove(etl_file)
      pass



limit = 0
db_live_connections={}

connections = {name: Conn(name, conn) for name, conn in settings['databases'].items()}

    
tables = '''RMS.RSRC_REQ
RMS.RSRC_REQ_QUE'''.splitlines()

sources_targets_d = [ ('STGQA.' + table, 'STGDEV.' + table) for table in tables]


etl = Scriptella('etl_', sources_targets_d)
etl.create_etl_file()
etl.execute()
