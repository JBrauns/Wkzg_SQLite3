import sqlite3
import re

class Sqlite3SchemaDeserializer(object):
  """ Deserialize an sql query into a Sqlite3Schema instance
  """
  
  __re_str__ = r'^CREATE\sTABLE\s' \
    '([a-zA-Z0-9]{1}[a-zA-Z0-9_]*)\s' \
    '\(([a-zA-Z0-9_,\s]+)\)[;]?$'
      
  @staticmethod
  def get(
    sql_schema
  ):
    if(sql_schema == ""):
      raise ValueError("Schema must not be empty")
    
    m = re.search(Sqlite3SchemaDeserializer.__re_str__, sql_schema)
    if(m is None):
      raise Exception("Invalid schema string")
    
    schema = Sqlite3Schema()
    for column in m.group(2).split(","):
      elements = column.strip().split(" ", 2)
      if(len(elements) == 3):
        schema.push(elements[0], elements[1], True)
      elif(len(elements) == 2):
        schema.push(elements[0], elements[1])
      else:
        raise ValueError("Column has unexpected elements count")
    
    return schema
    
class Sqlite3Schema(object):
  """ -- Missing --
  """
  
  NAME = 0
  TYPE = 1
  PKEY = 2
  
  SQLITE3_NONE    = 'NULL'
  SQLITE3_INT     = 'INTEGER'
  SQLITE3_LONG    = 'INTEGER'
  SQLITE3_FLOAT   = 'REAL'
  SQLITE3_STRING  = 'TEXT'
  SQLITE3_UNICODE = 'TEXT'
  SQLITE3_BUFFER  = 'BLOB'
  
  def __init__(
    self
  ):
    """ -- Missing --
    """
    self.columns = []
    self.has_primary_key = False
  
  def push(
    self,
    column_name,
    column_type,
    is_primary_key=False
  ):
    """ Append a new column for with the given column name and type
    """
    entry = None
    if(is_primary_key):
      assert not self.has_primary_key, "Primary key already specified"
      self.has_primary_key = True
      entry = (column_name, column_type, True)
    else:
      entry = (column_name, column_type, False)
    self.columns.append(entry)
    
  def __column_convert__(
    self,
    index
  ):
    column = self.columns[index]
    result = '{0} {1}'.format(column[self.NAME], column[self.TYPE])
    if(column[self.PKEY]):
      result = '{0} {1}'.format(result, "PRIMARY KEY")
    return result
    
  def create(
    self,
    table_name
  ):
    """ Serialize the sqlite3 schema as an sql create query
    """
    count = len(self.columns)
    assert count > 0, "At least 1 column must be appended"
    
    columns = []
    for i in range(0, count):
      columns.append(self.__column_convert__(i))
    values = ", ".join(columns)
    return 'CREATE TABLE {0} ({1});'.format(table_name, values)
    
  def insert(
    self,
    table_name,
    columns
  ):
    """ Serialize the sqlite3 schema as an sql insert query
    """
    count = len(self.columns)
    assert count > 0, "At least one column must be set"
    
    use_subset = False
    columns_subset = []
    if(columns[0] == 'all'):
      valid_columns = self.columns
    else:
      indexes = []
      for column in columns:
        indexes.append(self.columns.index(column))
      valid_columns = [self.columns[i] for i in indexes]
    
    query = ''
    if(self.has_primary_key or use_subset):
      # NOTE(joshua): 1. If the primary key is given it should not be specified (
      # either not set or set to NULL). All other columns have to be specified namely
      
      columns = [valid_columns[i][self.NAME] for i in range(1, count)]
      names  = ','.join(columns)
      values = ','.join([ '?' ] * (count - 1))
      query = 'INSERT INTO {0}({1}) VALUES ({2})'.format(table_name, names, values)
    else:
      # NOTE(joshua): 2. If no primary key is given the columns to set have not to
      # be specified (only valid in this case because all columns have to be set)
      
      values = ','.join([ '?' ] * count)
      query = 'INSERT INTO {0} VALUES ({1})'.format(table_name, values)
    print(query)
    return query
