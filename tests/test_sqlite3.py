import sqlite3
import csv

import os
__module_path__ = os.path.abspath(__file__)
__module_dir__  = os.path.dirname(__module_path__)

SQLITE3_NONE    = 'NULL'
SQLITE3_INT     = 'INTEGER'
SQLITE3_LONG    = 'INTEGER'
SQLITE3_FLOAT   = 'REAL'
SQLITE3_STRING  = 'TEXT'
SQLITE3_UNICODE = 'TEXT'
SQLITE3_BUFFER  = 'BLOB'

class Sqlite3Csv(object):
  def __init__(
    self,
    delimiter=";"
  ):
    self.delimiter = delimiter
    self.header = None
    self.rows = []
    
  def read_csv_file(
    self,
    file_path
  ):
    assert os.path.isfile(file_path), "Sqlite3Csv: File is invalid"
    with open(file_path, 'rb') as csv_file:
      reader = csv.reader(csv_file, delimiter=self.delimiter, quotechar='"')
      for row_i, row in enumerate(reader):
        if(row_i == 0):
          self.header = row
        else:
          self.rows.append(row)
        
  def get_csv_columns(
    self,
    columns
  ):
    assert self.header is not None, "Sqlite3Csv: Missing header"
    assert len(self.rows) > 0, "Sqlite3Csv: At least one row should be available"
    
    do_filter = True
    if(columns[0] == 'all'):
      do_filter = False
  
    filter = []
    if(do_filter):
      for column in columns:
        filter.append(self.header.index(column))
    
    result = []
    for row in self.rows:
      if(do_filter):
        result.append(tuple([row[i] for i in filter]))
      else:
        result.append(tuple(row))
    return result
    
  def write_csv_file(
    self,
    file_path
  ):
    raise Exception("Not implemented")
    
class Sqlite3Scheme(object):
  def __init__(
    self,
    name
  ):
    self.name = name
    self.columns = []
    
    self.__sql__ = {
      'create' : 'CREATE TABLE {0} ({1});',
      'insert' : 'INSERT INTO {0} VALUES ({1})'
    }
    
  def count(
    self
  ):
    return len(self.columns)
    
  def push(
    self,
    name,
    type
  ):
    entry = "{0} {1}".format(name, type)
    self.columns.append(entry)
    
  def get_sql_string(
    self,
    operation
  ):
    sql_string = ""
    if(operation == 'create'):
      assert len(self.columns) > 0, "At least 1 column must be appended"
      values = ", ".join(self.columns)
      sql_string = self.__sql__[operation].format(self.name, values)
    elif(operation == 'insert'):
      assert len(self.columns) > 0, "At least 1 column must be appended"
      values = ",".join(["?"] * self.count())
      sql_string = self.__sql__[operation].format(self.name, values)
    else:
      raise Exception("Invalid operation")
    return sql_string

def sql_print(
  cursor,
  query
):
  if(not isinstance(query, str)):
    raise Exception("SQL Query must be a string")
  
  print("\n===================\nSQL Query: '{0}'".format(query))
  for row_i, row in enumerate(cursor.execute(query)):
    print("SQL Query result({0}): {1}".format(row_i, ", ".join(map(lambda r: "{0}".format(r), row))))
    
if(__name__ == "__main__"):
  db1_setup = True
  db1_path = os.path.realpath(os.path.join(__module_dir__, "db1.db"))
  if(os.path.exists(db1_path)):
    db1_setup = False

  db1_conn = sqlite3.connect(db1_path)
  db1_cur = db1_conn.cursor()

  if(db1_setup):
    # Queue SQL commands
    movies_scheme = Sqlite3Scheme("Movies")
    movies_scheme.push('Id', SQLITE3_INT)
    movies_scheme.push('Title', SQLITE3_STRING)
    movies_scheme.push('Director', SQLITE3_STRING)
    movies_scheme.push('Year', SQLITE3_INT)
    movies_scheme.push('Length_minutes', SQLITE3_INT)
    db1_cur.execute(movies_scheme.get_sql_string('create'))
    
    movies_data_r = Sqlite3Csv()
    movies_data_r.read_csv_file("movies.csv")
    movies_data = movies_data_r.get_csv_columns(['all'])
    db1_cur.executemany(movies_scheme.get_sql_string('insert'), movies_data)
    db1_conn.commit()
    
    cities_scheme = Sqlite3Scheme("North_american_cities")
    cities_scheme.push('City', SQLITE3_STRING)
    cities_scheme.push('Country', SQLITE3_STRING)
    cities_scheme.push('Population', SQLITE3_INT)
    cities_scheme.push('Latitude', SQLITE3_FLOAT)
    cities_scheme.push('Longitude', SQLITE3_FLOAT)
    db1_cur.execute(cities_scheme.get_sql_string('create'))
    
    cities_data = [
      ( "Guadalajara", "Mexico", 1500800, 20.659699, -103.349609 ),
      ( "Toronto", "Canada", 2795060, 43.653226, -79.383184 ),
      ( "Houston", "United States", 2195914, 29.760427, -95.369803 ),
      ( "New York", "United States", 8405837, 40.712784, -74.005941 ),
      ( "Philadelphia", "United States", 1553165, 39.952584, -75.165222 ),
      ( "Havana", "Cuba", 2106146, 23.05407, -82.345189 ),
      ( "Mexico City", "Mexico", 8555500, 19.432608, -99.133208 ),
      ( "Phoenix", "United States", 1513367, 33.448377, -112.074037 ),
      ( "Los Angeles", "United States", 3884307, 34.052234, -118.243685 ),
      ( "Ecatepec de Morelos", "Mexico", 1742000, 19.601841, -99.050674 ),
      ( "Montreal", "Canada", 1717767, 45.501689, -73.567256 ),
      ( "Chicago", "United States", 2718782, 41.878114, -87.629798 )
    ]
    db1_cur.executemany(cities_scheme.get_sql_string('insert'), cities_data)
    db1_conn.commit()
    
    # Boxoffice table
    boffice_scheme = Sqlite3Scheme("Boxoffice")
    boffice_scheme.push('Movie_id', SQLITE3_INT)
    boffice_scheme.push('Rating', SQLITE3_FLOAT)
    boffice_scheme.push('Domestic_sales', SQLITE3_INT)
    boffice_scheme.push('International_sales', SQLITE3_INT)
    db1_cur.execute(boffice_scheme.get_sql_string('create'))
    
    boffice_data_r = Sqlite3Csv()
    boffice_data_r.read_csv_file("boxoffice.csv")
    boffice_data = boffice_data_r.get_csv_columns(['all'])
    db1_cur.executemany(boffice_scheme.get_sql_string('insert'), boffice_data)
    db1_conn.commit()
  
  sql_print(db1_cur, "SELECT Id, Title FROM Movies WHERE (Year NOT BETWEEN 2000 AND 2010);")
  
  # Part 2
  sql_query = [
    'SELECT *',
    'FROM Movies',
    'WHERE (Title LIKE "%Toy Story%");'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  sql_query = [
    'SELECT *',
    'FROM Movies',
    'WHERE (Director == "John Lasseter");'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  # Part 3
  sql_query = [
    'SELECT DISTINCT Director',
    'FROM Movies',
    'ORDER BY Director ASC;'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  sql_query = [
    'SELECT *',
    'FROM Movies',
    'ORDER BY Year DESC',
    'LIMIT 4;'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  sql_query = [
    'SELECT *',
    'FROM Movies',
    'ORDER BY Year DESC',
    'LIMIT 4;'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  # PART 5: Select queries review
  sql_query = [
    'SELECT City, Population',
    'FROM North_american_cities',
    'WHERE (Country == "Canada");'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  # PART 6: Multi-table queries with INNER JOINs
  sql_query = [
    'SELECT Title, Domestic_sales, International_sales',
    'FROM Movies',
    'INNER JOIN Boxoffice ON (Movies.Id == Boxoffice.Movie_id);'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  sql_query = [
    'SELECT Title, Domestic_sales, International_sales',
    'FROM Movies',
    'INNER JOIN Boxoffice ON (Movies.Id == Boxoffice.Movie_id)',
    'WHERE (International_sales > Domestic_sales);'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  # PART 6: Multi-table queries with INNER JOINs
  sql_query = [
    'SELECT Title, Domestic_sales, International_sales',
    'FROM Movies',
    'INNER JOIN Boxoffice ON (Movies.Id == Boxoffice.Movie_id)',
    'WHERE (International_sales > Domestic_sales);'
  ]
  sql_print(db1_cur, " ".join(sql_query))
  
  db1_conn.close()