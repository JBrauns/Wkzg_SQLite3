import csv
import os

class Sqlite3CsvProvider(object):
  def __init__(
    self,
    delimiter=";"
  ):
    self.delimiter = delimiter
    
  def is_long(
    self,
    value
  ):
    try:
      long(value)
      return True
    except ValueError:
      return False
      
  def is_float(
    self,
    value
  ):
    try:
      float(value)
      return True
    except ValueError:
      return False
    
  def load_data(
    self,
    file_path,
    columns
  ):
    if(not os.path.isfile(file_path)):
      raise ValueError("Parameter file_path is required to be a valid file")
      
    # NOTE(joshua): 1. Read in the complete csv file
    csv_header = []
    csv_rows = []
    with open(file_path, 'rb') as csv_file:
      reader = csv.reader(csv_file, delimiter=self.delimiter, quotechar='"')
      for row_i, row in enumerate(reader):
        if(row_i == 0):
          csv_header = row
        else:
          csv_rows.append(row)
          
    # NOTE(joshua): 2. Make sure there is enough data
    csv_row_count = len(csv_rows)
    assert csv_row_count > 0, "Not enough data in the csv file"
    
    # NOTE(joshua): 3. Check if all columns shall be returned or if the must be filtered
    csv_data = []
    if(columns[0] == 'all'):
      for row in csv_rows:
        row_converted = []
        for e in row:
          if(self.is_long(e)):
            row_converted.append(long(e))
          elif(self.is_float(e)):
            row_converted.append(float(e))
          else:
            row_converted.append(e)
        csv_data.append(tuple(row_converted))
    else:
      filter = []
      for column in columns:
        filter.append(csv_header.index(column))
      for row in csv_rows:
        row_converted = []
        row_filtered  = [row[i] for i in filter]
        for e in row_filtered:
          if(self.is_long(e)):
            row_converted.append(long(e))
          elif(self.is_float(e)):
            row_converted.append(float(e))
          else:
            row_converted.append(e)
        csv_data.append(tuple(row_converted))
      csv_header = filter
      
    return (tuple(csv_header), csv_data)
    
  def store_data(
    self,
    file_path,
    columns,
    data
  ):
    if(not os.path.isfile(file_path)):
      raise ValueError("Parameter file_path must be a valid file")
      
    data_count = len(data)
    assert data_count > 0, "Not enough data to store"
    if(not isinstance(data, list)):
      raise ValueError("Parameter data is expected to be a list")
    if(not isinstance(data[0], tuple)):
      raise ValueError("Entries of parameter data are expected to be tuples")
    
    # NOTE(joshua): 1. If necessary filter columns
    raise NotImplementedError()