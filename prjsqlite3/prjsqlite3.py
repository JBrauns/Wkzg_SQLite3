""" Module Documentation -- missing --
"""

import re
import os
import sqlite3



def Enum(*sequential, **named):
  enums = dict(zip(sequential, range(len(sequential))), **named)
  return type('Enum', (), enums)
  
PerfmonSchemaType = Enum('ICache', 'DCache')

class Sqlite3Schema(object):
  @staticmethod
  def get_perfmon_schema(
    name,
    type
  ):
    if(name == ""):
      raise Exception("Invalid schema name")
    if(type == PerfmonSchemaType.ICache):
      pass
    if(type == PerfmonSchemaType.DCache):
      pass
    else:
      raise Exception("Unsupported schema type")

class Sqlite3Provider(object):
  def __init__(
    self,
    storage_dir,
    storage_name
  ):
    if(not os.path.isdir(storage_dir)):
      raise Exception("Sqlite3Provider: Invalid storage directory")
    if(re.match(r"^[A-Za-z]{1}[A-Za-z0-9_]*$", storage_name) is None):
      raise Exception("Sqlite3Provider: Invalid storage name")
      
    self.db_name = "{0}.sqlite.db".format(storage_name)
    self.db_path = os.path.join(storage_dir, self.db_name)
    
    self.db_conn = None
    
  def open_project(
    self
  ):
    result = False
    # Check if the project is not already open
    if(self.db_conn is None):
      self.db_conn = sqlite3.connect(self.db_path)
      result = True
    return result
    
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
    
  def add_subproject(
    self,
    subproject_name,
    
  ):
    raise Exception("Not implemented")