""" Module Documentation -- missing --
"""

import re
import os
import sqlite3

from schemasqlite3 import Sqlite3Schema, Sqlite3SchemaDeserializer

SQLITE3_PROJECT_NAME=r"^[A-Za-z]{1}[A-Za-z0-9_]*$"

def Enum(*sequential, **named):
  enums = dict(zip(sequential, range(len(sequential))), **named)
  return type('Enum', (), enums)

class Sqlite3Project(object):
  def __init__(
    self,
    storage_dir,
    storage_name
  ):
    assert os.path.isdir(storage_dir), "Invalid storage directory"
    
    if(re.match(SQLITE3_PROJECT_NAME, storage_name) is None):
      raise ValueError("The parameter storage name must match project name restrictions")
      
    self.db_name = "{0}.sqlite.db".format(storage_name)
    self.db_path = os.path.join(storage_dir, self.db_name)
    
    self.db_conn = None
    if(self.open_project() is None):
      raise 
    db_cur = self.get_cursor()
    
    # Create the sub-project master table
    schema = Sqlite3Schema()
    schema.push('id', Sqlite3Schema.SQLITE3_INT, True)
    schema.push('type', Sqlite3Schema.SQLITE3_STRING)
    schema.push('name', Sqlite3Schema.SQLITE3_STRING)
    schema.push('count', Sqlite3Schema.SQLITE3_INT)
    db_cur.execute(schema.create("sub_projects"))
    self.db_conn.commit()
    
    # Create the sub-projects schema
    schema = Sqlite3Schema()
    schema.push('id', Sqlite3Schema.SQLITE3_INT, True)
    schema.push('entry', Sqlite3Schema.SQLITE3_STRING)
    schema.push('entry_schema', Sqlite3Schema.SQLITE3_STRING)
    schema.push('count', Sqlite3Schema.SQLITE3_INT)
    self.sub_project_schema = schema
    
    self.close_project()
    
  def open_project(
    self
  ):
    db_conn = None
    # Check if the project is not already open
    if(self.db_conn is None):
      db_conn = sqlite3.connect(self.db_path)
      self.db_conn = db_conn
    return db_conn
    
  def is_open(
    self
  ):
    result = True
    if(self.db_conn is None):
      result = False
    return result
    
  def get_cursor(
    self
  ):
    assert self.is_open(), "Project expected to be open"
    return self.db_conn.cursor()
  
  def close_project(
    self
  ):
    result = False
    # Check if the project is opened
    if(self.db_conn is not None):
      self.db_conn.close()
      self.db_conn = None
      result = True
    return result
    
  def new_sub_project(
    self,
    type,
    sub_project_name,
    sub_project_schema
  ):
    assert self.is_open(), "Project has to be open"
    assert isinstance(sub_project_schema, Sqlite3Schema), "Schema has unexpected type"
    
    db_cur = self.get_cursor()
    
    # NOTE(joshua): 1. Add entry into the project master table
    schema = self.get_schema("sub_projects")
    assert schema is not None, "Invalid schema retreived"
    
    db_data = [( sub_project_name, sub_project_schema.create("_"), 0 )]
    db_cur.executemany(schema.insert("sub_projects", ['all']), db_data)
    
    # NOTE(joshua): 2. Add a new sub-project master table
    schema = self.sub_project_schema
    db_cur.execute(schema.create(sub_project_name))
    
    self.db_conn.commit()
    
  def insert_into_sub_project(
    self,
    sub_project_name,
    values,
    force_update,
    columns=['all']
  ):
    count = len(columns)
    assert count > 0, "At least one column must be set"
    
    if(not isinstance(columns, list)):
      raise TypeError("Parameter columns must be of type <class 'list'>")
    if(not isinstance(columns[0], tuple)):
      raise TypeError("Elements of parameter columns must be of type <class 'tuple'>")
    
  def get_schema(
    self,
    sub_project_name
  ):
    """ Return the schema for the given sub-project
    
    :param sub_project_name:
      The name of the sub-project
    :type sub_project_name: ``str``
    """
    assert self.is_open(), "Project expected to be open"
    
    schema = None
    db_cur = self.get_cursor()
    
    # NOTE(joshua): 1. Execute SQL query to retrieve the schema
    query = 'SELECT sql FROM sqlite_master ' \
      'WHERE type="table" '\
      'ORDER BY name;'
    for i, row in enumerate(db_cur.execute(query)):
      if(i > 0):
        raise IndexError("Should only return")
      schema = Sqlite3SchemaDeserializer.get(row[0])
    
    # NOTE(joshua): 2. Validate result before returning
    assert schema is not None, "Error retrieving the schema from the project"
    
    return schema