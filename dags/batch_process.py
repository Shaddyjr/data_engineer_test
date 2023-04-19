# helpful sources: 
# https://www.learnpythonwithrune.org/how-to-setup-a-mysql-server-in-docker-for-your-python-project/
# https://medium.com/@rajat.mca.du.2015/airflow-and-mysql-with-docker-containers-80ed9c2bd340
import re
import csv
from datetime import datetime
from airflow.hooks.mysql_hook import MySqlHook
import os
import logging

log = logging.getLogger(__name__)
# get dag directory path
path = os.getcwd()

DATA_FILE = f'{path}/data/BING_MultiDays.csv'

def execute_sql(sql, params=None, execute_many = False):
  hook = MySqlHook(mysql_conn_id='mysql_default', schema='airflow')
  conn = hook.get_conn()
  cursor = conn.cursor()
  # TODO: improve error handling
  if execute_many:
    cursor.executemany(sql, params)
  else:
    cursor.execute(sql, params)
  cursor.close()
  conn.commit()
  conn.close()

def parse_metadata(metadata_rows):
  """Return metadata dict from csv metadata rows using raw key:value pairing"""
  str_split_re = "^(.*?):(.*)$" # "KEY: VALUE" format (lazy/non-greedy capture)
  metadata_dict = {}
  
  for metadata_row in metadata_rows:
    re_result = re.search(str_split_re, metadata_row[0].strip())
    log.info("METADATA_ROW", metadata_row[0].strip())
    key, value = re_result.groups()
    metadata_dict[key.strip()] = value.strip()

  return metadata_dict

def parse_csv_file():
  """Returns metadata dictionary, headers, and rows from CSV"""
  # assumed encoding
  with open(DATA_FILE, "r", encoding='utf-8-sig') as f:
    csv_reader = csv.reader(f)
    records = list(csv_reader)

    metadata_rows = records[:8]
    metadata_dict = parse_metadata(metadata_rows)
    headers = [col_name.strip() for col_name in records[9]]

    row_count = metadata_dict.get('Rows') # TODO: what if row_count is None => can deduce row_count?
    # skipping over empty line, so starting at row 10
    data_rows = records[10: int(row_count) + 10]
  return metadata_dict, headers, data_rows

def get_or_create_tables(metadata_dict, headers):
  """
  Handles creation of new metadata table and/or new data table
  
  Returns the names of the metadata table, the new data table, and their respective headers.
  """
  # TODO: handle conflict from parallelization
  now = datetime.now()

  # Creating metadata table, if needed
  metadata_table = "csv_extracts"
  metadata_headers = metadata_dict.keys()
  
  create_metadata_table_sql = """
  CREATE TABLE IF NOT EXISTS {metadata_table} (
    csv_extract_id INT AUTO_INCREMENT PRIMARY KEY,
    processed_at DATETIME NOT NULL,
    `Report Name` VARCHAR(50),
    `Report Time` VARCHAR(50),
    `Time Zone` VARCHAR(50),
    `Last Completed Available Day` VARCHAR(50),
    `Last Completed Available Hour` VARCHAR(50),
    `Report Aggregation` VARCHAR(50),
    `Report Filter` VARCHAR(50),
    `Rows` INT
  );
  """.format(metadata_table=metadata_table)
  log.info(f"Executing SQL: {create_metadata_table_sql}")
  execute_sql(create_metadata_table_sql)

  # Creating new data table
  # TODO: Daily runs should ensure unique name, but potential conflicts if need to rerun/backfill
  today_str = now.strftime("%Y_%m_%d")
  data_table = f"csv_extract_{today_str}"
  create_data_table_sql = """
  DROP TABLE IF EXISTS {data_table};
  CREATE TABLE {data_table} (
    processed_at DATETIME NOT NULL,
    `Gregorian date` DATE,
    `Customer` VARCHAR(50),
    `Account number` VARCHAR(50),
    `Account name` VARCHAR(50),
    `Account status` VARCHAR(50),
    `Campaign name` VARCHAR(100),
    `Campaign status` VARCHAR(50),
    `Ad group ID` VARCHAR(50),
    `Ad group` VARCHAR(50),
    `Ad group status` VARCHAR(50),
    `Ad ID` VARCHAR(50),
    `Ad description` VARCHAR(200),
    `Ad distribution` VARCHAR(50),
    `Ad status` VARCHAR(50),
    `Ad title` VARCHAR(50),
    `Ad type` VARCHAR(50),
    `Tracking Template` VARCHAR(100),
    `Custom Parameters` VARCHAR(50),
    `Final Mobile URL` VARCHAR(200),
    `Final URL` VARCHAR(500),
    `Top vs. other` VARCHAR(50),
    `Display URL` VARCHAR(200),
    `Final App URL` VARCHAR(200),
    `Destination URL` VARCHAR(200),
    `Device type` VARCHAR(50),
    `Device OS` VARCHAR(50),
    `Delivered match type` VARCHAR(50),
    `BidMatchType` VARCHAR(50),
    `Language` VARCHAR(50),
    `Network` VARCHAR(50),
    `Currency code` VARCHAR(50),
    `Impressions` INT,
    `Clicks` DOUBLE,
    `Spend` DOUBLE,
    `Avg. position` DOUBLE,
    `Conversions` INT,
    `Assists` INT
  );
  """.format(data_table=data_table)
  log.info(f"Executing SQL: {create_data_table_sql}")
  execute_sql(create_data_table_sql)

  return {
    "metadata_table": metadata_table,
    "metadata_headers": metadata_headers,
    "data_table": data_table,
    "data_headers": headers,
  }

def add_data(table_dict, metadata_dict, data_rows):
  """
  Adds metadata and data rows to their respective tables
  """
  now = datetime.now()
  formatted_datetime = now.strftime("%Y-%m-%d %H:%M:%S")

  ### INSERT METADATA ROW ###
  metadata_table = table_dict.get("metadata_table")
  insert_metadata_row_sql = """
  INSERT INTO {metadata_table} (
    processed_at,
    `Report Name`,
    `Report Time`,
    `Time Zone`,
    `Last Completed Available Day`,
    `Last Completed Available Hour`,
    `Report Aggregation`,
    `Report Filter`,
    `Rows`
    )
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
  """.format(metadata_table=metadata_table)

  log.info(f"Executing SQL: {insert_metadata_row_sql}")
  metadata_values = (
    formatted_datetime,
    metadata_dict.get("Report Name") or None,
    metadata_dict.get("Report Time") or None,
    metadata_dict.get("Time Zone") or None,
    metadata_dict.get("Last Completed Available Day") or None,
    metadata_dict.get("Last Completed Available Hour") or None,
    metadata_dict.get("Report Aggregation") or None,
    metadata_dict.get("Report Filter") or None,
    metadata_dict.get("Rows") or None,
  )
  execute_sql(insert_metadata_row_sql, metadata_values)

  ### INSERT DATA ROWS ###
  # TODO: if this fails we should rollback previous changes (could change order of operations,)
  # TODO: include some validation here
  data_table = table_dict.get("data_table")
  insert_data_rows_sql = """
  INSERT INTO {data_table} (
    processed_at,
    `Gregorian date`,
    `Customer`,
    `Account number`,
    `Account name`,
    `Account status`,
    `Campaign name`,
    `Campaign status`,
    `Ad group ID`,
    `Ad group`,
    `Ad group status`,
    `Ad ID`,
    `Ad description`,
    `Ad distribution`,
    `Ad status`,
    `Ad title`,
    `Ad type`,
    `Tracking Template`,
    `Custom Parameters`,
    `Final Mobile URL`,
    `Final URL`,
    `Top vs. other`,
    `Display URL`,
    `Final App URL`,
    `Destination URL`,
    `Device type`,
    `Device OS`,
    `Delivered match type`,
    `BidMatchType`,
    `Language`,
    `Network`,
    `Currency code`,
    `Impressions`,
    `Clicks`,
    `Spend`,
    `Avg. position`,
    `Conversions`,
    `Assists`
    )
  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
  """.format(data_table=data_table)

  def parse_data_row(data_row):
    data_values = (
      formatted_datetime, # processed_at DATETIME NOT NULL,
      data_row[0] or None, # `Gregorian date` DATE,
      data_row[1] or None, # `Customer` VARCHAR(50),
      data_row[2] or None, # `Account number` VARCHAR(50),
      data_row[3] or None, # `Account name` VARCHAR(50),
      data_row[4] or None, # `Account status` VARCHAR(50),
      data_row[5] or None, # `Campaign name` VARCHAR(100),
      data_row[6] or None, # `Campaign status` VARCHAR(50),
      data_row[7] or None, # `Ad group ID` VARCHAR(50),
      data_row[8] or None, # `Ad group` VARCHAR(50),
      data_row[9] or None, # `Ad group status` VARCHAR(50),
      data_row[10] or None, # `Ad ID` VARCHAR(50),
      data_row[11] or None, # `Ad description` VARCHAR(200),
      data_row[12] or None, # `Ad distribution` VARCHAR(50),
      data_row[13] or None, # `Ad status` VARCHAR(50),
      data_row[14] or None, # `Ad title` VARCHAR(50),
      data_row[15] or None, # `Ad type` VARCHAR(50),
      data_row[16] or None, # `Tracking Template` VARCHAR(100),
      data_row[17] or None, # `Custom Parameters` VARCHAR(50),
      data_row[18] or None, # `Final Mobile URL` VARCHAR(200),
      data_row[19] or None, # `Final URL` VARCHAR(500),
      data_row[20] or None, # `Top vs. other` VARCHAR(50),
      data_row[21] or None, # `Display URL` VARCHAR(200),
      data_row[22] or None, # `Final App URL` VARCHAR(200),
      data_row[23] or None, # `Destination URL` VARCHAR(200),
      data_row[24] or None, # `Device type` VARCHAR(50),
      data_row[25] or None, # `Device OS` VARCHAR(50),
      data_row[26] or None, # `Delivered match type` VARCHAR(50),
      data_row[27] or None, # `BidMatchType` VARCHAR(50),
      data_row[28] or None, # `Language` VARCHAR(50),
      data_row[29] or None, # `Network` VARCHAR(50),
      data_row[30] or None, # `Currency code` VARCHAR(50),
      int(data_row[31]) if data_row[31] else None, # `Impressions` INT,
      float(data_row[32]) if data_row[32] else None, # `Clicks` DOUBLE,
      float(data_row[33]) if data_row[33] else None, # `Spend` DOUBLE,
      float(data_row[34]) if data_row[34] else None, # `Avg. position` DOUBLE,
      int(data_row[35]) if data_row[35] else None, # `Conversions` INT,
      int(data_row[36]) if data_row[36] else None, # `Assists` INT
    )
    return data_values

  log.info(f"Executing SQL: {insert_data_rows_sql}")
  data_values_list = [parse_data_row(data_row) for data_row in data_rows]
  execute_sql(insert_data_rows_sql, data_values_list, execute_many=True)

def extract_csv_data():
  """  
  Parses CSV file, creates tables accordingly, and inserts data into tables.

  # TODO: Could use validation, handle header duplications, row duplications, etc.
  """
  
  log.info(f"Parsing CSV file...")
  metadata_dict, headers, data_rows = parse_csv_file()
  
  log.info(f"Creating Tables in DB...")
  table_dict = get_or_create_tables(metadata_dict, headers)

  log.info(f"Inserting data into tables...")
  add_data(table_dict, metadata_dict, data_rows)

  log.info(f"CSV Data Extraction Complete!")
