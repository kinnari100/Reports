
import os
import srsly
from mi_utils import get_postgresql_connection, get_input_file_name, ensure_path, get_output_file_name
from pandas import DataFrame, Series, to_datetime, to_numeric
import pandas as pd
from psycopg2 import connect
from psycopg2.extras import DictCursor
from pathlib import Path
from datetime import datetime, date
from psycopg2 import DatabaseError
import xlsxwriter
file_name = ensure_path(Path.home(), '/Users/kinnaripatel/Desktop/LEPM/epm.json')
epm_config = srsly.read_json(file_name)

# Select a site
site = 'stelizabeth'
# Get credentials for a particular site (site variable above)
credentials = [x for x in epm_config if x['sitename'] == site][0]

user = credentials['username']
password = credentials['password']
db_name = credentials['dbname']
host = credentials['host']
port = credentials['port']
print('User: %s, db_name: %s, host: %s, port: %s ' % (user, db_name, host, port))

query = """
    with cohort (id) as (
    (select id from cohorts where deleted_at is null and active = true and template = 'lcsr')
), constants (cohort_id, patient_ssn, patient_first_name, patient_middle_name, patient_last_name, facility_id, facility_npi,
   accession_number, date_scheduled, mrn, dob, dod, modc, omodc, cod, codnlc, dodip, sex, race, ethnicity,
   health_insurance, health_insurance_other, edu_lvl, edu_lvl_other, exposure_radon, exposure_occupational, exposure_occupational_other,
   history_of_cancer, history_of_cancer_other, family_history_cancer_first_degree, family_history_cancer_other_rel, copd, pulmonary_fibrosis,
   second_hand_smoke, date_performed, smoking_status, pack_years, number_of_years_since_quit, smoking_cessation_guidance, shared_decision_making,
   height, weight, comorbidities, comorbidities_other, rr_npi, op_npi, sign_symptoms_lung_cancer,indication_exam, modality,
   scanner_manufacturer, scanner_model, dose_index_vol, dose_length, tube_current_time, tube_voltage, scanning_time, scanning_vol,
   pitch, reconstructed_img_width, lung_rads, reason_recall, reason_recall_other_specify, lung_rads_version,
   significant_abnormalities, other_findings, other_clinical_significant_specify, other_interstitial_lung_disease,
   prior_history_of_lung_cancer, year_since_diagnosis_lung_cancer, old_medicare_id, new_medicare_id) as (
   values (
           (select id from cohort),
           (select id from fields where name = 'Patient_SSN' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'first_name' and cohort_id is null),
           (select id from fields where name = 'Patient_Middle_Name' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'last_name' and cohort_id is null),
           (select id from fields where name = 'Facility_ID' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Facility_npi' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'accession_number' and cohort_id is null),
           (select id from fields where name = 'date_scheduled' and cohort_id is null),
           (select id from fields where name = 'mrn' and cohort_id is null),
           (select id from fields where name = 'dob' and cohort_id is null),
           (select id from fields where name = 'Date_Of_Death' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'How_Cause_Was_Determined' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Other_Method_Of_Determining' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Cause_Of_Death' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Non_Lung_Cancer_Cause' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Death_Within_30_Days' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'sex' and cohort_id is null),
           (select id from fields where name = 'Patient_Race' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Patient_Ethnicity' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Health_Insurance' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Health_Insurance_Other_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Education_Level' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Education_Level_Other_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Radon_Exposure' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Occupational_Exposures' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Occupational_Exposures_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'History_Of_Cancers' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Other_Smoking_Cancers_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Lung_Cancer_In_First_Deg_Rel' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Lung_Cancer_Other_First_Deg_Rel' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'COPD' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Pulmonary_Fibrosis' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Second_Hand_Smoke_Exposure' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'date_performed' and cohort_id is null),
           (select id from fields where name = 'Smoking_Status' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Number_Of_Packs_Year_Smoking' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Number_Of_Years_Since_Quit' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Did_Physician_Provide_Guidance' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Doc_Of_Shared_Dec_Making' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Patient_Height' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Patient_Weight' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Patient_Other_Comorbidities' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Patient_Other_Comorbidities_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Radiologist_Reading_NPI' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Ordering_Practitioner_NPI' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Signs_Or_Symptoms_Of_Lung_Cancer' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Indication_Of_Exam' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Modality' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CT_Scanner_Manufacturer' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CT_Scanner_Model' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CTDIvol' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'DLP' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Tube_Current_Time' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Tube_Voltage' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Scanning_Time' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Scanning_Volume' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Pitch' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Reconstructed_Image_Width' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CT_Exam_Result_Lung_RADS' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Reason_For_Recall' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Reason_For_Recall_Spec' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Lung_RADS_Version' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CT_Exam_Result_Modifier_S' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'What_Were_The_Other_Findings' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Other_Abnormalities_Spec' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Other_Int_Lung_Disease' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'CT_Exam_Result_Modifier_C' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Years_Since_Prior_Diagnosis' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Medicare_Beneficiary_ID' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'New_Medicare_Beneficiary_ID' and cohort_id = (select cohort.id from cohort))
           )
)
select p.data->>(c.patient_ssn)::text as patient_ssn,
      p.data->>(c.patient_first_name)::text as patient_first_name,
      p.data->>(c.patient_middle_name)::text as patient_middle_name,
      p.data->>(c.patient_last_name)::text as patient_last_name,
      e.data->>(c.facility_id)::text as facility_id,
      e.data->>(c.facility_npi)::text as facility_npi,
      f.name as facility_name,
      e.data->>(c.accession_number)::text as exm_number,
      (e.data->>(c.date_scheduled)::text)::timestamptz as exm_reg_date,
      e.data->>(c.accession_number)::text as exm_unique_id,
      patient_id as patient_nrdr_id,
      concat('"', p.data->>(c.mrn)::text, '"') as patient_mrn,
      (p.data->>(c.dob)::text)::date as patient_dob,
      (p.data->>(c.dod)::text)::date as patient_dod,
      case
          when p.data->>(c.modc)::text = '1' then 'Autopsy Report'
          when p.data->>(c.modc)::text = '2' then 'Death Certificate'
          when p.data->>(c.modc)::text = '3' then 'Medical Record'
          when p.data->>(c.modc)::text = '4' then 'Physician'
          when p.data->>(c.modc)::text = '5' then 'Relative or Friend'
          when p.data->>(c.modc)::text = '6' then 'Social Security Death Index'
          when p.data->>(c.modc)::text = '8' then 'Other'
          end as patient_dod_method,
      p.data->>(c.omodc)::text as patient_dod_method_other,
      case
          when p.data->>(c.cod)::text = '1' then 'Lung cancer'
          when p.data->>(c.cod)::text = '2' then 'Non-lung cancer cause, specify if known'
          when p.data->>(c.cod)::text = '9' then 'Cannot determine'
          end as patient_death_cause,
      p.data->>(c.codnlc)::text as patient_death_cause_nonlc,
      p.data->>(c.dodip)::text as patient_dod_inv_proc,
      p.data->>(c.sex)::text as patient_sex,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.data->>(c.race)::text, '10', 'Unknown'), '1', 'American Indian or Alaskan Native'), '3', 'Asian'), '4', 'Black or African-American'), '5', 'Native Hawaiian or Other Pacific Islander'), '6', 'White'), '7', 'Other'), '8', 'Other'), '9', 'Not reported'), '"', ''), '[', ''), ']', '')  as patient_race,
      replace(replace(replace(replace(replace(replace(replace(p.data->>(c.ethnicity)::text, '0', 'Not Hispanic or Latino'), '1', 'Hispanic or Latino'), '8', 'Not Reported'), '9', 'Unknown'), '"', ''), '[', ''), ']', '') as patient_ethnicity,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.data->>(c.health_insurance)::text, '1', 'Medicare'), '2', 'Medicaid'), '3', 'Private Insurance'), '4', 'Self Pay'), '6', 'VA'), '7', 'Other, specify'), '5', 'Unknown'), '"', ''), '[', ''), ']', '') as health_insurance,
      p.data->>(c.health_insurance_other)::text as health_insurance_other,
      case
          when p.data->>(c.edu_lvl)::text = '1' then '8th grade or less'
          when p.data->>(c.edu_lvl)::text = '2' then '9-11th grade'
          when p.data->>(c.edu_lvl)::text = '3' then 'High school graduate/GED'
          when p.data->>(c.edu_lvl)::text = '4' then 'Post high school training, excluding college'
          when p.data->>(c.edu_lvl)::text = '5' then 'Associate degree/some college'
          when p.data->>(c.edu_lvl)::text = '6' then 'Bachelors Degree'
          when p.data->>(c.edu_lvl)::text = '7' then 'Graduate School'
          when p.data->>(c.edu_lvl)::text = '8' then 'Other, specify'
          when p.data->>(c.edu_lvl)::text = '99' then 'Unknown/ prefer not to answer'
          end as patient_education_level,
      p.data->>(c.edu_lvl_other)::text as patient_education_level_other,
      case
          when p.data->>(c.exposure_radon)::text = 'Y' then 'Yes'
          when p.data->>(c.exposure_radon)::text = 'N' then 'No'
          when p.data->>(c.exposure_radon)::text = 'U' then 'Unknown'
          end as exposures_radon,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.data->>(c.exposure_occupational)::text, '11', 'Coal smoke'), '12', 'Soot'), '10', 'None'), '0', 'Silica'), '1', 'Cadmium'), '2', 'Asbestos'), '3', 'Arsenic'), '4', 'Beryllium'), '5', 'Chromium'), '6', 'Diesel fumes'), '7', 'Nickel'), '8', 'Other occupational exposure'), '9', 'Unknown'), '"',''), '[', ''), ']', '') as exposures_occupational,
      p.data->>(c.exposure_occupational_other)::text as exposures_occupational_other,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.data->>(c.history_of_cancer)::text, '10', 'Kidney cancer'), '11', 'Pancreatic cancer'), '88', 'Unknown'), '99', 'No history of cancers associated with an increased risk of lung cancer'), '0', 'Prior history of lung cancer'), '1', 'Lymphoma'), '2', 'Head and neck cancer'), '3', 'Bladder cancer'), '4', 'Other smoking-related cancers, specify'), '5', 'Acute myeloid leukemia'), '6', 'Colorectal cancer'), '7', 'Esophageal cancer'), '8', 'Liver cancer'), '9', 'Gastric cancer'), '"', ''), '[', ''), ']', '') as cancer_hx_incrs_risk_lc,
      p.data->>(c.history_of_cancer_other)::text as cancer_hx_incrs_risk_lc_other,
      case
          when p.data->>(c.family_history_cancer_first_degree)::text = 'Y' then 'Yes'
          when p.data->>(c.family_history_cancer_first_degree)::text = 'N' then 'No'
          when p.data->>(c.family_history_cancer_first_degree)::text = 'U' then 'Unknown'
          end as family_lc_hx_first_degree,
      case
          when p.data->>(c.family_history_cancer_other_rel)::text = 'Y' then 'Yes'
          when p.data->>(c.family_history_cancer_other_rel)::text = 'N' then 'No'
          when p.data->>(c.family_history_cancer_other_rel)::text = 'U' then 'Unknown'
          end as family_lc_hx,
      case
          when p.data->>(c.copd)::text = 'Y' then 'Yes'
          when p.data->>(c.copd)::text = 'N' then 'No'
          when p.data->>(c.copd)::text = 'U' then 'Unknown'
          end as dx_copd,
      case
          when p.data->>(c.pulmonary_fibrosis)::text = 'Y' then 'Yes'
          when p.data->>(c.pulmonary_fibrosis)::text = 'N' then 'No'
          when p.data->>(c.pulmonary_fibrosis)::text = 'U' then 'Unknown'
          end as dx_pul_fib,
      case
          when p.data->>(c.second_hand_smoke)::text = 'Y' then 'Yes'
          when p.data->>(c.second_hand_smoke)::text = 'N' then 'No'
          when p.data->>(c.second_hand_smoke)::text = 'U' then 'Unknown'
          end as exposures_second_hand_smoke,
      (e.data->>(c.date_performed)::text)::timestamptz as exm_date,
      case
          when e.data->>(c.smoking_status)::text = '1' then 'Current smoker'
          when e.data->>(c.smoking_status)::text = '2' then 'Former smoker'
          when e.data->>(c.smoking_status)::text = '3' then 'Never/Passive smoker'
          when e.data->>(c.smoking_status)::text = '4' then 'Smoker, current status unknown'
          when e.data->>(c.smoking_status)::text = '9' then 'Unknown if ever smoked'
          end as smoking_status,
      e.data->>(c.pack_years)::text as smoking_pack_years,
      e.data->>(c.number_of_years_since_quit)::text as smoking_quit_years,
      case
          when e.data->>(c.smoking_cessation_guidance)::text  = 'Y' then 'Yes'
          when e.data->>(c.smoking_cessation_guidance)::text  = 'N' then 'No'
          when e.data->>(c.smoking_cessation_guidance)::text  = 'U' then 'Unknown'
          end as smoking_phys_cess,
      case
          when e.data->>(c.shared_decision_making)::text  = 'Y' then 'Yes'
          when e.data->>(c.shared_decision_making)::text  = 'N' then 'No'
          when e.data->>(c.shared_decision_making)::text  = 'U' then 'Unknown'
          end as smoking_shared_dec_doc,
      e.data->>(c.height)::text as patient_exm_height,
      e.data->>(c.weight)::text as patient_exm_weight,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(p.data->>(c.comorbidities)::text, '0', 'COPD'), '1', 'Emphysema'), '2', 'Pulmonary fibrosis'), '3', 'Coronary artery disease'), '4', 'Congestive heart failure'), '5', 'Peripheral vascular disease'), '6', 'Lung cancer'), '7', 'Cancer other than lung cancer'), '8', 'Other, please specify'), '"', ''), '[', ''), ']', '') as comorbidities,
      p.data->>(c.comorbidities_other)::text as comorbidities_other,
      case
          when (p.data->>(c.history_of_cancer)::text) similar to '%(0|1|2|3|4|5|6|7|"8"|"9"|10|11)%' then 'Yes'
          when (p.data->>(c.history_of_cancer)::text) = '["88"]' then 'Unknown'
          when (p.data->>(c.history_of_cancer)::text) = '["99"]' then 'No'
          end as cancer_hx,
      pr_rr.npi as reading_radiologist_npi,
      pr_op.npi as ordering_practitioner_npi,
      case
          when e.data->>(c.sign_symptoms_lung_cancer)::text = 'Y' then 'Yes'
          when e.data->>(c.sign_symptoms_lung_cancer)::text = 'N' then 'No'
          end as exm_signs_sympt_lc,
      case
          when e.data->>(c.indication_exam)::text = '1' then 'Baseline scan'
          when e.data->>(c.indication_exam)::text = '2' then 'Annual screen'
          end as exm_indication,
      case
          when e.data->>(c.modality)::text = '1' then 'Low dose chest CT'
          when e.data->>(c.modality)::text = '2' then 'Routine chest CT'
          end as exm_modality,
      e.data->>(c.scanner_manufacturer)::text as exm_scan_manufacturer,
      e.data->>(c.scanner_model)::text as exm_scan_model,
      e.data->>(c.dose_index_vol)::text as exm_scan_ctdlvol,
      e.data->>(c.dose_length)::text as exm_scan_dlp,
      e.data->>(c.tube_current_time)::text as exm_scan_tube_current_time,
      e.data->>(c.tube_voltage)::text as exm_scan_tube_voltage,
      e.data->>(c.scanning_time)::text as exm_scanning_time,
      e.data->>(c.scanning_vol)::text as exm_scanning_volume,
      e.data->>(c.pitch)::text as exm_scan_pitch,
      e.data->>(c.reconstructed_img_width)::text as exm_recontructed_image_width,
      e.data->>(c.lung_rads)::text as exm_result_lung_rads,
      case
          when e.data->>(c.reason_recall)::text = 'I' then 'Incomplete coverage'
          when e.data->>(c.reason_recall)::text = 'N' then 'Noise'
          when e.data->>(c.reason_recall)::text = 'M' then 'Respiratory motion'
          when e.data->>(c.reason_recall)::text = 'E' then 'Expiration'
          when e.data->>(c.reason_recall)::text = 'OBa' then 'Obscured by acute abnormality'
          when e.data->>(c.reason_recall)::text = 'UC' then 'Unable to complete'
          when e.data->>(c.reason_recall)::text = 'U' then 'Unknown'
          end  as exm_recall_reason,
      e.data->>(c.reason_recall_other_specify)::text as exm_recall_reason_other,
      e.data->>(c.lung_rads_version)::text as exm_lung_rads_version,
      case
          when e.data->>(c.significant_abnormalities)::text = 'Y' then 'Yes'
          when e.data->>(c.significant_abnormalities)::text = 'N' then 'No'
          end as exm_other_abnormalities_ided,
      replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(e.data->>(c.other_findings)::text, '0', 'Aortic aneurysm'), '1', 'Coronary arterial calcification moderate or severe'), '2', 'Pulmonary fibrosis'), '3', 'Mass (check neck, mediastinum, liver, kidneys, other)'),'4', 'Other interstitial lung disease'), '5', 'Other clinically significant abnormalities, specify'), '6', 'Emphysema, moderate or severe'), '9', 'Unknown'), '"', ''), '[', ''), ']', '') as exm_other_findings,
      e.data->>(c.other_clinical_significant_specify)::text as exm_other_findings_other,
      case
          when e.data->>(c.other_interstitial_lung_disease)::text = '1' then 'UIP/IPF'
          when e.data->>(c.other_interstitial_lung_disease)::text = '8' then 'ILD, Other specify'
          when e.data->>(c.other_interstitial_lung_disease)::text = '9' then 'ILD, unknown'
          end as exm_interstitial_disease,
      case
          when e.data->>(c.prior_history_of_lung_cancer)::text = 'Y' then 'Yes'
          when e.data->>(c.prior_history_of_lung_cancer)::text = 'N' then 'No'
          when e.data->>(c.prior_history_of_lung_cancer)::text = 'U' then 'Unknown'
          end as prior_hx_lc,
      e.data->>(c.year_since_diagnosis_lung_cancer)::text as prior_hx_lc_years,
      s.submitted_by_first_name as exm_comp_first_name,
      s.submitted_by_last_name as exm_comp_last_name,
      s.submitted_by_first_name as exm_sub_first_name,
      s.submitted_by_last_name as exm_sub_last_name,
--        s.submission_status as submission_status,
      s.submitted_at as exm_sub_date,
      s.submitted_at as last_transaction_date,
      p.data->>(c.old_medicare_id)::text as patient_old_mdcr_bid,
      p.data->>(c.new_medicare_id)::text as patient_new_mdcr_bid
from patients p
   join exams e on p.id = e.patient_id and e.deleted_at is null
   join cohort_exams ce on e.id = ce.exam_id and ce.deleted_at is null and ce.exam_status in ('active', 'in_active')
   join facilities f on e.facility_id = f.id and f.deleted_at is null
   join constants c on true
   left join (select distinct on (exam_id) es.exam_id, es.status as submission_status, es.submitted_at, u.first_name as submitted_by_first_name, u.last_name as submitted_by_last_name
           from (exam_submissions es join users u on es.submitted_by = u.id)
           where es.status in ('successful', 'resubmit')
           order by exam_id, status desc) as s on s.exam_id = e.id
   left join providers pr_rr on pr_rr.id::text = (e.data->>(c.rr_npi)::text)
   left join providers pr_op on pr_op.id::text = (e.data->>(c.op_npi)::text)
where (e.data->>(c.date_performed)::text)::date between '2022-04-01'::date and '2022-07-31'::date
 and ce.cohort_id = c.cohort_id
 and p.deleted_at is null;

"""
epm_connection = connect(host=host, database=db_name, user=user, password=password, cursor_factory=DictCursor)

with epm_connection.cursor() as cursor:
    cursor.execute(query)
    result = cursor.fetchall()

df = DataFrame(result,columns = list(result[0].keys()))
print(df)


df['exm_reg_date'] = df['exm_reg_date'].apply(lambda a: pd.to_datetime(a).date()) 
df['exm_date'] = df['exm_date'].apply(lambda a: pd.to_datetime(a).date()) 
df['last_transaction_date'] = df['last_transaction_date'].apply(lambda a: pd.to_datetime(a).date()) 
df['exm_sub_date'] = df['exm_sub_date'].apply(lambda a: pd.to_datetime(a).date()) 



#>= CURRENT_DATE - INTERVAL '3 months'
#between '2022-04-01'::date and '2022-07-31'::date



query1 = """
with cohort (id) as (
    (select id from cohorts where deleted_at is null and active = true and template = 'lcsr')
), constants (cohort_id, patient_ssn, patient_first_name, patient_middle_name, patient_last_name, facility_id, facility_npi,
   accession_number, mrn, date_performed, follow_up_type_other_specify,
   tissue_diag, tissue_diag_other, tissue_diag_method, location_sample, location_other,
   histology, histology_small_cell, other_non_small_cell_histology, stage_cp, overall_stage,
   t_status, n_status, m_status, ajcc) as (
   values (
           (select id from cohort),
           (select id from fields where name = 'Patient_SSN' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'first_name' and cohort_id is null),
           (select id from fields where name = 'Patient_Middle_Name' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'last_name' and cohort_id is null),
           (select id from fields where name = 'Facility_ID' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Facility_npi' and entity_type = 1 and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'accession_number' and cohort_id is null),
           (select id from fields where name = 'mrn' and cohort_id is null),
           (select id from fields where name = 'date_performed' and cohort_id is null),
           (select id from fields where name = 'Follow_Up_Diagnostic_Other_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Tissue_Diagnosis' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Tissue_Diagnosis_Other_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Tissue_Diagnosis_Method' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Location_From_Sample_Obtained' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Location_Other_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Histology' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Histology_Non_Small_Cell_LC' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Other_Non_Small_Cell_LC_Histology_Spec' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Stage_Clinical_Or_Pathologic' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'Overall_Stage' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'T_Status' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'N_Status' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'M_Status' and cohort_id = (select cohort.id from cohort)),
           (select id from fields where name = 'AJCC_Cancer_Staging_Edition' and cohort_id = (select cohort.id from cohort))
           )
)
select p.data->>(c.patient_ssn)::text as patient_ssn,
      f.patient_id as patient_nrdr_id,
      concat('"', p.data->>(c.mrn)::text, '"') as patient_mrn,
      p.data->>(c.patient_first_name)::text as patient_first_name,
      p.data->>(c.patient_middle_name)::text as patient_middle_name,
      p.data->>(c.patient_last_name)::text as patient_last_name,
      f.data->>(c.facility_id)::text as facility_id,
      f.data->>(c.facility_npi)::text as facility_npi,
      fac.name as facility_name,
      e.data->>(c.accession_number)::text as exm_number,
      f.data->>(c.accession_number)::text as fup_exm_unique_id,
      (f.data->>(c.date_performed)::text)::timestamptz as fup_exm_date,
      et.name as fup_exm_diagnostic,
      f.data->>(c.follow_up_type_other_specify)::text as fup_exm_diagnostic_other,
      case
          when f.data->>(c.tissue_diag)::text = '1' then 'Benign'
          when f.data->>(c.tissue_diag)::text = '2' then 'Malignant - invasive lung cancer'
          when f.data->>(c.tissue_diag)::text = '3' then 'Malignant - Minimally invasive lung cancer'
          when f.data->>(c.tissue_diag)::text = '4' then 'Malignant - NON-lung cancer'
          when f.data->>(c.tissue_diag)::text = '5' then 'Malignant - adenocarcinoma in situ'
          when f.data->>(c.tissue_diag)::text = '9' then 'Malignant - carcinoid'
          when f.data->>(c.tissue_diag)::text = '10' then 'Malignant - not adenocarcinoma, lung cancer, non invasive'
          when f.data->>(c.tissue_diag)::text = '11' then 'Malignant - not adenocarcinoma, lung cancer, invasive status unknown'
          when f.data->>(c.tissue_diag)::text = '6' then 'Premalignancy - atypical adenomatous hyperplasia'
          when f.data->>(c.tissue_diag)::text = '7' then 'Non-diagnostic'
          when f.data->>(c.tissue_diag)::text = '8' then 'Clinical - without histology'
          when f.data->>(c.tissue_diag)::text = '99' then 'Unknown'
          when f.data->>(c.tissue_diag)::text = '12' then 'Other, specify'
          end as fup_exm_tissue_diag,
      f.data->>(c.tissue_diag_other)::text as fup_exm_tissue_diag_other,
      case
          when f.data->>(c.tissue_diag_method)::text = '1' then 'Percutaneous (non-surgical)'
          when f.data->>(c.tissue_diag_method)::text = '2' then 'Bronchoscopic'
          when f.data->>(c.tissue_diag_method)::text = '3' then 'Surgical'
          when f.data->>(c.tissue_diag_method)::text = '99' then 'Unknown'
          end as fup_exm_tissue_diag_method,
      case
          when f.data->>(c.location_sample)::text = '0' then 'Left Hilum'
          when f.data->>(c.location_sample)::text = '1' then 'Lingula of the Lung'
          when f.data->>(c.location_sample)::text = '2' then 'Left Lower Lobe'
          when f.data->>(c.location_sample)::text = '3' then 'Left Upper Lobe'
          when f.data->>(c.location_sample)::text = '4' then 'Right Hilum'
          when f.data->>(c.location_sample)::text = '5' then 'Right Lower Lobe'
          when f.data->>(c.location_sample)::text = '6' then 'Right Middle Lobe'
          when f.data->>(c.location_sample)::text = '7' then 'Right Middle/Right Lower Lobes'
          when f.data->>(c.location_sample)::text = '8' then 'Right Upper/Right Middle Lobes'
          when f.data->>(c.location_sample)::text = '9' then 'Right Upper Lobe of Lung'
          when f.data->>(c.location_sample)::text = '10' then 'Other'
          when f.data->>(c.location_sample)::text = '11' then 'Unknown'
          end as fup_exm_smpl_coll_loc,
      f.data->>(c.location_other)::text as fup_exm_smpl_coll_loc_other,
      case
          when f.data->>(c.histology)::text = '1' then 'Non-small cell lung cancer'
          when f.data->>(c.histology)::text = '2' then 'High grade neuroendocrine tumor (small cell lung cancer)'
          when f.data->>(c.histology)::text = '3' then 'Low grade neuroendocrine tumor (carcinoid)'
          when f.data->>(c.histology)::text = '4' then 'Intermediate grade neuroendocrine tumor (Atypical carcinoid)'
          when f.data->>(c.histology)::text = '99' then 'Unknown'
          end as fup_exm_histology,
      case
          when f.data->>(c.histology_small_cell)::text = '1' then 'Invasive adenocarcinoma'
          when f.data->>(c.histology_small_cell)::text = '2' then 'Squamous cell carcinoma'
          when f.data->>(c.histology_small_cell)::text = '3' then 'Adenosquamous cell carcinoma'
          when f.data->>(c.histology_small_cell)::text = '4' then 'Undifferentiated or poorly differentiated carcinoma'
          when f.data->>(c.histology_small_cell)::text = '5' then 'Large cell carcinoma'
          when f.data->>(c.histology_small_cell)::text = '6' then 'Other, specify'
          end as fup_exm_nsclc_histology,
      f.data->>(c.other_non_small_cell_histology)::text as fup_exm_nsclc_histology_other,
      case
          when f.data->>(c.stage_cp)::text = '1' then 'Clinical'
          when f.data->>(c.stage_cp)::text = '2' then 'Pathologic'
          when f.data->>(c.stage_cp)::text = '9' then 'Unknown'
          end as fup_exm_stage_type,
      f.data->>(c.overall_stage)::text as fup_exm_overall_stage,
      case when f.data->>(c.t_status)::text = '99' then 'Unknown'
          else f.data->>(c.t_status)::text
          end as fup_exm_tstage,
      f.data->>(c.n_status)::text as fup_exm_nstage,
      f.data->>(c.m_status)::text as fup_exm_mstage,
      case
          when f.data->>(c.ajcc)::text = '7' then '7th edition'
          when f.data->>(c.ajcc)::text = '8' then '8th edition'
          when f.data->>(c.ajcc)::text = 'U' then 'Other / Unknown'
          end as fup_exm_ajcc_version,
      extract(year from age(((f.data->>c.date_performed::text)::timestamptz), (coalesce((select distinct on (lf.parent_exam_id) (lf.data->>(c.date_performed::text))::date
           from exams lf
           where lf.parent_exam_id = f.parent_exam_id and (lf.data->>(c.date_performed)::text)::date < (f.data->>(c.date_performed)::text)::date order by lf.parent_exam_id, (lf.data->>(c.date_performed)::text)::date desc), (e.data->>(c.date_performed)::text)::timestamptz)))) * 12
          + extract(month from age(((f.data->>c.date_performed::text)::timestamptz), (coalesce((select distinct on (lf.parent_exam_id) (lf.data->>(c.date_performed::text))::date
           from exams lf
           where lf.parent_exam_id = f.parent_exam_id and (lf.data->>(c.date_performed)::text)::date < (f.data->>(c.date_performed)::text)::date order by lf.parent_exam_id, (lf.data->>(c.date_performed)::text)::date desc), (e.data->>(c.date_performed)::text)::timestamptz)))) as fup_period_months,
      coalesce((select distinct on (lf.parent_exam_id) (lf.data->>(c.date_performed::text))::date
           from exams lf
           where lf.parent_exam_id = f.parent_exam_id and (lf.data->>(c.date_performed)::text)::date < (f.data->>(c.date_performed)::text)::date order by lf.parent_exam_id, (lf.data->>(c.date_performed)::text)::date desc), (e.data->>(c.date_performed)::text)::timestamptz)
          as previous_performed_followup_or_parent_exam_date,
      s.submitted_by_first_name as exm_comp_first_name,
      s.submitted_by_last_name as exm_comp_last_name,
      s.submitted_by_first_name as exm_sub_first_name,
      s.submitted_by_last_name as exm_sub_last_name,
--        s.submission_status as submission_status,
      s.submitted_at as exm_sub_date,
      s.submitted_at as last_transaction_date
from patients p
   join exams f on p.id = f.patient_id and f.deleted_at is null and f.parent_exam_id is not null
   join exams e on e.id = f.parent_exam_id and e.deleted_at is null
   join exam_types et on f.exam_type_id = et.id and et.deleted_at is null
   join cohort_exams ce on f.id = ce.exam_id and ce.deleted_at is null and ce.exam_status in ('active', 'in_active')
   join facilities fac on f.facility_id = fac.id and fac.deleted_at is null
   join constants c on true
   left join (select distinct on (exam_id) es.exam_id, es.status as submission_status, es.submitted_at, u.first_name as submitted_by_first_name, u.last_name as submitted_by_last_name
           from (exam_submissions es join users u on es.submitted_by = u.id)
           where es.status in ('successful', 'resubmit')
           order by exam_id, status desc) as s on s.exam_id = f.id
where (f.data->>(c.date_performed)::text)::date between '2022-04-01'::date and '2022-07-31'::date
 and ce.cohort_id = c.cohort_id
 and p.deleted_at is null;
"""
epm_connection = connect(host=host, database=db_name, user=user, password=password, cursor_factory=DictCursor)

with epm_connection.cursor() as cursor:
    cursor.execute(query1)
    result = cursor.fetchall()

df1 = DataFrame(result,columns = list(result[0].keys()))
#df = DataFrame(result)
df1.info
print(df1)
 
df1['fup_exm_date'] = df1['fup_exm_date'].apply(lambda a: pd.to_datetime(a).date()) 
df1['previous_performed_followup_or_parent_exam_date'] = df1['previous_performed_followup_or_parent_exam_date'].apply(lambda a: pd.to_datetime(a).date()) 
df1['exm_sub_date'] = df1['exm_sub_date'].apply(lambda a: pd.to_datetime(a).date()) 
df1['last_transaction_date'] = df1['last_transaction_date'].apply(lambda a: pd.to_datetime(a).date())

writer = pd.ExcelWriter('/Users/kinnaripatel/Desktop/k/KHC.xlsx', engine='xlsxwriter', datetime_format='mm-dd-yyyy')
workbook   = xlsxwriter.Workbook('/Users/kinnaripatel/Desktop/k/KP2.xlsx')
worksheet2 = workbook.add_worksheet('Followup Data')
worksheet1 = workbook.add_worksheet('Exam Data')

df.to_excel (writer, sheet_name='Exam Data', startrow=1, header=False,index = False)
df1.to_excel (writer, sheet_name='Followup Data', startrow=1, header=False,index = False)


workbook  = writer.book

worksheet2 = writer.sheets['Followup Data']
header_format = workbook.add_format({'font_name': 'Arial', 'font_color': 'white','font_size': 12, 'bold': True,'fg_color': '#003333'})

for col_num, value in enumerate(df1.columns.values):
    worksheet2.write(0, col_num + 0, value, header_format)
border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
worksheet2.conditional_format(xlsxwriter.utility.xl_range(0, 0, len(df1), len(df1.columns)), {'type': 'no_errors', 'format': border_fmt})

worksheet1 = writer.sheets['Exam Data']
header_format = workbook.add_format({'font_name': 'Arial', 'font_color': 'white','font_size': 12, 'bold': True,'fg_color': '#003333'})

for col_num, value in enumerate(df.columns.values):
    worksheet1.write(0, col_num + 0, value, header_format)
border_fmt = workbook.add_format({'bottom':1, 'top':1, 'left':1, 'right':1})
worksheet1.conditional_format(xlsxwriter.utility.xl_range(0, 0, len(df), len(df.columns)), {'type': 'no_errors', 'format': border_fmt})                               

writer.save()


