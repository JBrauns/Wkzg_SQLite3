""" Module documentation -- missing --
"""

import shutil
import os
__module_path__ = os.path.abspath(__file__)
__module_dir__  = os.path.dirname(__module_path__)

import context
from prjsqlite3 import Sqlite3Project
from schemasqlite3 import Sqlite3Schema, Sqlite3SchemaDeserializer

from pmonr import Pmonr

import unittest

class TestSqlite3Project(unittest.TestCase):
  def setup(
    self
  ):
    print("TestSqlite3Project: Setup")
    self.project_dir = os.path.abspath(os.path.join(__module_dir__, "test"))
    self.project_name = "test_prj"
    
    # Create the test directory
    try:
      if(os.path.isdir(self.project_dir)):
        shutil.rmtree(self.project_dir)
      os.makedirs(self.project_dir)
    except IOError:
      self.fail("Test directory creation failed")
     
  def shutdown(
    self
  ):
    print("TestSqlite3Project: Shutdown")
    self.fail("Not implemented")
  
  def test_init(self):
    print("TestSqlite3Project: Shutdown")
    
    self.setup()
    
    project = Sqlite3Project(self.project_dir, self.project_name)
    self.assertTrue(project.open_project() is not None)
    
    db_conn = project.db_conn
    db_cur = db_conn.cursor()
    
    # Sub-Project A
    schema = Sqlite3Schema()
    schema.push('id', Sqlite3Schema.SQLITE3_INT)
    schema.push('type', Sqlite3Schema.SQLITE3_STRING)
    schema.push('name', Sqlite3Schema.SQLITE3_STRING)
    schema.push('count', Sqlite3Schema.SQLITE3_INT)
    project.new_sub_project("type_a", "a_01_master", schema)
    
    self.assertTrue(project.close_project())
    
  def test_subproject_a_new(
    self
  ):
    self.fail("Not implemented")
    
  def test_subproject_a_import(
    self
  ):
    self.fail("Not implemented")
    
  def test_subproject_a_import_from_csv(
    self
  ):
    self.fail("Not implemented")
    
  def test_subproject_a_export_csv(
    self
  ):
    self.fail("Not implemented")
    
  def test_subproject_a_export_json(
    self
  ):
    self.fail("Not implemented")
    
if(__name__ == "__main__"):
  unittest.main()