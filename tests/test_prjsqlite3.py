""" Module documentation -- missing --
"""

import shutil
import os
__module_path__ = os.path.abspath(__file__)
__module_dir__  = os.path.dirname(__module_path__)

import context
from prjsqlite3 import Sqlite3Provider

import unittest

class TestSqlite3Provider(unittest.TestCase):
  def test_basic(self):
    project_dir = os.path.abspath(os.path.join(__module_dir__, "test"))
    project_name = "test_prj"
  
    # Create the test directory
    try:
      if(os.path.isdir(project_dir)):
        shutil.rmtree(project_dir)
      os.makedirs(project_dir)
    except IOError:
      self.fail("Test directory creation failed")
    
    provider = Sqlite3Provider(project_dir, project_name)
    self.assertTrue(provider.open_project())
    self.assertFalse(provider.open_project()) # Project should already be open
    
    subprojects = [ "perfmon", "analysis.models", "analysis." ]
    
    self.assertTrue(provider.close_project())
    self.assertFalse(provider.close_project()) # Project should have been closed
    
if(__name__ == "__main__"):
  unittest.main()