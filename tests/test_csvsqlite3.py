""" Module documentation -- missing --
"""

import shutil
import os
__module_path__ = os.path.abspath(__file__)
__module_dir__  = os.path.dirname(__module_path__)

import context
from csvsqlite3 import Sqlite3CsvProvider

import unittest
import csv

class TestSqlite3Project(unittest.TestCase):
  def setup(
    self
  ):
    project_dir = os.path.abspath(os.path.join(__module_dir__, "test_csvsqlite3"))
    try:
      if(os.path.isdir(project_dir)):
        self.clear(project_dir)
      os.makedirs(project_dir)
    except IOError:
      self.fail("Test directory creation failed")
    return project_dir
  
  def clear(
    self,
    file_directory
  ):
    shutil.rmtree(file_directory)

  def test_load_all(
    self
  ):
    dir = self.setup()
    
    csv_provider = Sqlite3CsvProvider(";")
    header_expected = ( 'String', 'Int', 'Float' )
    data_expected = [
      ( 'positive',  42,  42.0 ),
      ( 'negative', -42, -42.0 )
    ]
    
    # NOTE(joshua): 1. Manually store the data as a csv
    file_path = os.path.join(dir, 'test_load_all.csv')
    with open(file_path, 'w') as csv_file:
      writer = csv.writer(csv_file, delimiter=';')
      
      writer.writerow(list(header_expected))
      for data in data_expected:
        writer.writerow(list(data))
    
    # NOTE(joshua): 2. Compare expected and actual data
    (header_actual, data_actual) = csv_provider.load_data(file_path, ['all'])
    
    # Validate types
    self.assertTrue(isinstance(header_actual, tuple))
    self.assertTrue(isinstance(data_actual, list))
    self.assertEqual(len(data_actual), len(data_expected))
    self.assertTrue(isinstance(data_actual[0], tuple))
    
    # Validate data
    self.assertEqual(header_expected, header_actual)
    for a, b in zip(data_expected, data_actual):
      self.assertEqual(a, b)
    
    self.clear(dir)
    
  def test_store_all(
    self
  ):
    self.fail('Not implemented')
  
if(__name__ == "__main__"):
  unittest.main()