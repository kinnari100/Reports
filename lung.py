import psycopg2
import json
import pandas as pd
from helper import aggregate, process_df, save_to_excel, connect_to_db, upload
import os
import glob
from notification import send_notification
import datetime


path = "D:\Eon\Data Science\Reporting\Report Automation\eon-ds-report-automation\site_configs"
#path = "/home/murtaza/eon-ds-report-automation/site_configs"
for filename in glob.glob(os.path.join(path, '*.json')):     
    with open(filename, mode='r') as f:
        data = json.load(f)
        connection = connect_to_db(data['site_info']['db_name'])
        print (data['site_info']['has_facilities'])
        cursor = connection.cursor()

        if data['site_info']['has_facilities'] == True:
            for facility in data['facility_info']:
                postgreSQL_select_Query_with_facility = """select messages."messageId","{}","dateOfBirth","patientVisitFacility","{}","universalServiceIdentifier1","{}","{}","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality" 
from messages inner join spacy_incidentals as ipn on messages."messageId" = ipn."messageId" 
where report is not null and "esIndex" ilike '{}' and  "{}" >=  CURRENT_DATE - INTERVAL '3 months'""".format(facility['mrn_column_id'],facility['procedure_desc_column_id'],facility['exam_date_column_id'],facility['accession_column_id'],facility['facility_id'], facility['exam_date_column_id'])

                #data_tuple = (facility['facility_id'])
                cursor.execute(postgreSQL_select_Query_with_facility)
                
                print("Selecting rows from exams table")
                exams = cursor.fetchall()
                df=pd.DataFrame(list(exams)) 
                file_name = facility['facility_id']+'.xlsx'
                exam_df,exam_df_aggregated,patient_df,patient_df_aggregated = process_df(df)
                save_to_excel(exam_df,exam_df_aggregated,patient_df,patient_df_aggregated,file_name)
                upload(file_name)
                #df.to_csv('sample'+facility['facility_id']+'.csv')
        else:
            postgreSQL_select_Query = """select messages."messageId","{}","dateOfBirth","patientVisitFacility","{}","universalServiceIdentifier1","{}","{}","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality" 
from messages inner join spacy_incidentals as ipn on messages."messageId" = ipn."messageId" 
where report is not null and  "{}" >=  '2021-05-01' and "{}" < '2021-06-01' """.format(data['site_info']['mrn_column_id'],data['site_info']['procedure_desc_column_id'],data['site_info']['exam_date_column_id'],data['site_info']['accession_column_id'],data['site_info']['exam_date_column_id'],data['site_info']['exam_date_column_id'])

            cursor.execute(postgreSQL_select_Query)
            print("Selecting rows from exams table")
            exams = cursor.fetchall()
            df=pd.DataFrame(list(exams)) 
            df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Procedure Code","Exam Date","Accession Number","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality"]
            df.to_excel ("D:\Eon\Data Science\Reporting\Report Automation\eon-ds-report-automation\SalinasValley_May2021.xlsx", index = False, header=True)
            quit()
            site = data['site_info']['site_id']
            now = datetime.datetime.now()
            duration = str(now.month-3)+'-'+str(now.year)+'_'+str(now.month-1)+'-'+str(now.year)
            file_name = data['site_info']['site_id']+duration+'.xlsx'
            exam_df,exam_df_aggregated,patient_df,patient_df_aggregated = process_df(df)
            save_to_excel(exam_df,exam_df_aggregated,patient_df,patient_df_aggregated,file_name,site,duration)
            #weblink=upload(file_name)
            #send_notification('ammar.ahmed@eonhealth.com',site,weblink,duration)
