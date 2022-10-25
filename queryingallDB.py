import logging
import re
import os
import srsly
from mi_utils import get_postgresql_connection, get_input_file_name, ensure_path, get_output_file_name
from pandas import DataFrame, Series, to_datetime, to_numeric
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import DictCursor
from pathlib import Path
from datetime import datetime, date
from dateutil.relativedelta import relativedelta
from psycopg2 import DatabaseError

file_name = ensure_path(Path.home(), '/Users/kinnaripatel/Desktop/LEPM/epm.json')
epm_config = srsly.read_json(file_name)

# Select a site

for i in range(len(epm_config)):
# Get credentials for a particular site (site variable above)
    credentials = [x for x in epm_config][i]
    print(credentials)
    user = credentials['username']
    password = credentials['password']
    db_name = credentials['dbname']
    host = credentials['host']
    port = credentials['port']
    print('User: %s, db_name: %s, host: %s, port: %s ' % (user, db_name, host, port))
    query = """
        select id as facility_id, name, description, code from facilities where active = true and deleted_at is null order by name
    """


    selected_columns = ["facility_id", "name","description","code"]
    epm_connection = connect(host=host, database=db_name, user=user, password=password, cursor_factory=DictCursor)

    with epm_connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchall()

    df = DataFrame(result, columns= selected_columns)
    
    outputfilename='/Users/kinnaripatel/Desktop/LEPM/output.xlsx'

    with pd.ExcelWriter(outputfilename, engine='openpyxl',mode='a',if_sheet_exists='overlay') as writer:
        df.to_excel(outputfilename, sheet_name = site,index=False)

#print (df)
