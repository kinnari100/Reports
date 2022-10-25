from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient.http import MediaFileUpload
from apiclient import errors
from pandas import Series, ExcelWriter
from pandas import ExcelWriter
from pandas import Series
import psycopg2
import json




def connect_to_db(db_name):
    db_creds_file = open('/Users/kinnaripatel/Desktop/k/db_credential/db_creds.json',)
    db_file = json.load(db_creds_file)
    host = db_file['db_creds']['host']
    username = db_file['db_creds']['username']
    passw = db_file['db_creds']['password']
    port = db_file['db_creds']['port']
    connection = psycopg2.connect(host=host,port=port,database=db_name, user=username, password=passw) 
    return connection

def nodule_size(size) :
    if size < 1:
        return "less than 10 mm"
    elif size >= 1:
        return "Greater than equal to 10 mm"
    elif size == 0.0:
        return "Subcm"

def exam_filter(procedure):
    
    matches = ["US BREAST", "PET CT TUM SK BS MIDTHI INIT","PET CT TUM SK BS MIDTHI SUBS","PET TUM IMG MET EVAL","US ASP PUNCTURE ABSC HEMA CYST","CT BX FINE NEEDLE ASP W GUIDE","US BX NDL MUSCLE OR SOFT TIS"]

    if any(x in procedure for x in matches):
        return "low risk"
    else:
        return "high risk"

def aggregate(data):
    data  = { '2021' : data[data.year == '2021']}
    result = {}
    for key, value in data.items():
        result['jan-' + key] = (value.month == 'Jan').sum()
        result['feb-' + key] = (value.month == 'Feb').sum()
        result['mar-'+ key] = (value.month == 'Mar').sum()
        result['april-'+ key] = (value.month == 'Apr').sum()
        result['may-'+ key] = (value.month == 'May').sum()
        result['june-'+ key] = (value.month == 'Jun').sum()
        result['july-'+ key] = (value.month == 'Jul').sum()
        result['aug-'+ key] = (value.month == 'Aug').sum()
        result['sep-'+ key] = (value.month == 'Sep').sum()
        result['oct-'+ key] = (value.month == 'Oct').sum()
        result['nov-' + key] = (value.month == 'Nov').sum()
        result['dec-' + key] = (value.month == 'Dec').sum()
    result['Exams Count'] = len(data['2021']) 
    
    return Series(result)

def process_df(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Procedure Code","Exam Date","Accession Number","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    df['new_date'] = [d.date() for d in df['Exam Date']]
    df['new_time'] = [d.time() for d in df['Exam Date']]
    exam_df =df
    exam_df['risk_category'] = exam_df.max_measure.apply(nodule_size)
    df.loc[(df.max_nodule_lobe == 'upper') & (df.laterality == 'right'), 'risk_category'] = "high risk"
    df.loc[df.max_nodule_margin == 'spiculated', 'risk_category'] = "high risk"
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    return exam_df,patient_df

def process_df_breast(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Exam Date","Accession Number","report","has_measure","max_measure","laterality","max_measure_location","max_nodule_shape","max_nodule_calcification","max_nodule_density","max_nodule_margin","max_nodule_lobe","single_multiple_abnormality"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    exam_df =df
    exam_df['size_category'] = exam_df["max_measure"].apply(nodule_size)
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    print (df.size_category.value_counts())
    exam_df_aggregated = exam_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    patient_df_aggregated = patient_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    print(exam_df_aggregated)
    return  exam_df,exam_df_aggregated,patient_df,patient_df_aggregated

def process_df_LCSR(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Exam Date","Accession Number","report","patient_pack_years","smoking_status","years_since_quit","lung_rads_category"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    exam_df =df
    exam_df['size_category'] = exam_df["max_measure"].apply(nodule_size)
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    print (df.size_category.value_counts())
    exam_df_aggregated = exam_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    patient_df_aggregated = patient_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    print(exam_df_aggregated)
    return  exam_df,exam_df_aggregated,patient_df,patient_df_aggregated


def process_df_liver(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Modality","Exam Date","Accession Number","report","has_measure","max_measure"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    exam_df =df
    exam_df['size_category'] = exam_df["max_measure"].apply(nodule_size)
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    print (df.size_category.value_counts())
    exam_df_aggregated = exam_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    patient_df_aggregated = patient_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    print(exam_df_aggregated)
    return  exam_df,exam_df_aggregated,patient_df,patient_df_aggregated

def process_df_thyroid(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Exam Date","Accession Number","report","has_measure","max_measure"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    exam_df =df
    exam_df['size_category'] = exam_df["max_measure"].apply(nodule_size)
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    print (df.size_category.value_counts())
    exam_df_aggregated = exam_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    patient_df_aggregated = patient_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    print(exam_df_aggregated)
    return  exam_df,exam_df_aggregated,patient_df,patient_df_aggregated

def process_df_panc(df):
    df.columns =["ID","MRN","DateOfBirth","Facility","Procedure Description","Exam Date","Accession Number","report","has_measure","max_measure"]
    df['month'] = df["Exam Date"].dt.strftime('%b')
    df['year'] = df["Exam Date"].dt.strftime('%Y')
    exam_df =df
    exam_df['size_category'] = exam_df["max_measure"].apply(nodule_size)
    patient_df=exam_df.sort_values('Exam Date').drop_duplicates(subset=['MRN'], keep='last')
    print (df.size_category.value_counts())
    exam_df_aggregated = exam_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    patient_df_aggregated = patient_df.groupby('size_category').apply(aggregate).reset_index(drop = False)
    print(exam_df_aggregated)
    return  exam_df,exam_df_aggregated,patient_df,patient_df_aggregated

def save_to_excel(exam_df,patient_df,file_name):
    writer = ExcelWriter(file_name,engine='xlsxwriter')
    exam_df.to_excel(writer, 'All Exams', index = False,startrow=11)
    patient_df.to_excel(writer, 'All Patients', index = False,startrow=11)
    



def update_permission(service, file_id, permission_id, new_role, type, value):
  try:
    # First retrieve the permission from the API.
    permission = service.permissions().get(fileId=file_id, permissionId=permission_id).execute()
    permission['role'] = new_role
    permission['type'] = type
    permission['value'] = value
    return service.permissions().update(fileId=file_id, permissionId=permission_id, body=permission,transferOwnership=True).execute()
  except errors.HttpError as error:
    print('An error occurred:', error)
  return None



def upload(file_name):
    scope = ['https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('creds.json', scope)

# https://developers.google.com/drive/api/v3/quickstart/python
    service = build('drive', 'v3', credentials=credentials)

    file_metadata = {
    'name': file_name,
    'parents': ['1IXLK8HLXV57lrjg6WIOtNN39mC85M3iM'],
    }
    media = MediaFileUpload(file_name)
    file = service.files().create(body=file_metadata,
                                    media_body=media,
                                    fields='id, webViewLink, permissions').execute()
    file_id = file['id']
    permission_id = file['permissions'][0]['id']

    print(file_id)
    print(permission_id)

    # update permissions?  It doesn't work!
    update_permission(service, file_id, permission_id, 'owner', 'user','murtaza.ali@eonhealth.com') # https://stackoverflow.com/a/11669565
    return file.get('webViewLink')
#print(file.get('webViewLink'))   

def apply_formatting(writer,site,duration,exam_df):
    workbook  = writer.book
    worksheets = [writer.sheets['All Exams'], writer.sheets['All Patients']]
    summary_worksheets = [writer.sheets['Exams Summary'], writer.sheets['Patients Summary']]

    header_format = workbook.add_format({
    'bold': True,
    'text_wrap': True,
    'align': 'center',
    'valign': 'vcenter',
    'fg_color': '#189EF9',
    'border': 1})
    merge_format = workbook.add_format({
    'bold': 1,
    'border': 1,
    'align': 'center',
    'valign': 'vcenter',
    'fg_color': '##189EF9',
    'font_size': 20,
    'font_color': '#FFFFFF'})

    for worksheet in worksheets:
        worksheet.insert_image('A1', r'C:\Users\hp\Desktop\eon_logo.png')
        worksheet.merge_range('A5:H6', 'Cohort: Incidental Pulmonary Nodule', merge_format)
        worksheet.merge_range('A7:H8', 'Site: '+site, merge_format)
        worksheet.merge_range('A9:H10', 'Date Range: '+duration, merge_format)
    # Write the column headers with the defined format.
        for col_num, value in enumerate(exam_df.columns.values):
            worksheet.write(11, col_num, value, header_format)
        
        #for column in exam_df:
            #column_width = max(exam_df[column].astype(str).map(len).max(), len(column))
            #col_idx = exam_df.columns.get_loc(column)
            #worksheet.set_column(col_idx, col_idx, column_width)
    
    for summary_worksheet in summary_worksheets:
        summary_worksheet.merge_range('B1:C1', 'Risk Category', header_format)
        summary_worksheet.write('A1', 'Month', header_format)
        summary_worksheet.write('A2', '', header_format)
        summary_worksheet.write('B2', 'High Risk', header_format)
        summary_worksheet.write('C2', 'Low Risk', header_format)
    writer.save()


















