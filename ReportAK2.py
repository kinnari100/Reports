import os
import srsly
#from mi_utils import get_postgresql_connection, get_input_file_name, ensure_path, get_output_file_name
from pandas import DataFrame, Series, to_datetime, to_numeric
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import DictCursor
from pathlib import Path
from datetime import datetime, date
from psycopg2 import DatabaseError
import xlsxwriter
import openpyxl
import matplotlib.pyplot as plt
import numpy as np
import datetime
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-s', "--startdate",
    help="The Start Date - format YYYY-MM-DD",
    required=True)
parser.add_argument('-e', "--enddate",
    help="The End Date format YYYY-MM-DD (Inclusive)",
    required=True)
args=parser.parse_args()
print(args)

file_name =('/Users/kinnaripatel/Desktop/LEPM/dw.json')
epm_config = srsly.read_json(file_name)
# Select a site
site = 'eondw'

#assigning schema, by default it takes public schema
schema= 'epm'

# Get credentials for a particular site (site variable above)
credentials = [x for x in epm_config if x['sitename'] == site][0]

user = credentials['username']
password = credentials['password']
db_name = credentials['dbname']
host = credentials['host']
port = credentials['port']

print('User: %s, db_name: %s, host: %s, port: %s ' % (user, db_name, host, port))
#query
query = """

select site, facility, MaleCount
,round(((maleCount *100)/totalFacCount), 2) as "male_patientperc"
,avg_age_male, femalecount
,round(((femalecount *100)/totalFacCount), 2) as "Female_patientperc"
,avg_age_female, totalFacCount, othercount, unknowncount
,round(((totalFacCount *100)/totalSiteCount), 2) as "Total Patient perc by Facility"
, totalSiteCount
from (
select distinct site, facility
,count(case when sex = 'Male' then 1 else null end) over siteFac as MaleCount
,avg(case when sex = 'Male' then patient_age else null end) over siteFac::int as Avg_Age_Male
,count(case when sex = 'Female' then 1 else null end) over siteFac as FemaleCount
,avg(case when sex = 'Female' then patient_age else null end) over siteFac::int as Avg_Age_Female
,count(case when sex = 'Other' then 1 else null end) over siteFac as othercount
,count(case when sex is null or sex ='Unknown' or sex ilike any (array['3']) then 1 else null end) over siteFac as unknowncount
,count(1) over siteFac as TotalFacCount
,count(1) over site as TotalSiteCount
from (
select distinct p.site, f.name as Facility, sex, patient_age, master_patient_id, p.display_name
from patients p join facilities f
on p.site = f.site and p.facility_id = f.facility_id
where (p.insert_datetime >= %s and p.insert_datetime <= %s)
) cnt
window siteFac as (partition by site, facility), site as (partition by site)
) pct

"""

#where (p.insert_datetime >= '20221021' and p.insert_datetime <= '20221022' )
#start_date='20221021'
#end_date='20221024'
#args=(start_date,end_date)
# where (p.insert_datetime >= '20221021' and p.insert_datetime <= '20221022' )
epm_connection = connect(host=host, database=db_name, user=user, password=password,cursor_factory=DictCursor)

with epm_connection.cursor() as cursor:
    #assigning schema name
    cursor.execute("set search_path to {schema};".format(schema=schema))
    cursor.execute(query,(args.startdate,args.enddate))
    print(query)
    #cursor.execute(query)
    #for row in cursor.fetchall():
        #print(row)
    result = cursor.fetchall()
    print(result)

selected_columns=["site","facility","malecount","male_patient","avg_age_male", "femalecount","female_patient_%","avg_age_female","totalfaccount","othercount","unknowncount","Total Patient % by Facility","totalsitecount"]      
df = DataFrame(result,columns = selected_columns)
print(df)
df.to_excel('/Users/kinnaripatel/Desktop/LEPM/kp1.xlsx')
#plot the all columns
#df.set_index(['site']).plot.bar(rot=45)

#plot certain columns
