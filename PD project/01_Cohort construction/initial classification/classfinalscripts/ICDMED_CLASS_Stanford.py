import pandas as pd
from thunderpack import ThunderReader  # install thunderpack if not yet: pip install thunderpack
import pandas as pd
# import pyodbc
# import random
# import zipfile
# import io
import re 
# import csv

import pandas as pd
from thunderpack import ThunderReader  # install thunderpack if not yet: pip install thunderpack
import re

reader_ICD = ThunderReader('/home/bram/internal_expansion/HOSPITALS/Stanford/ICD')  # Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_ICD = list(reader_ICD.keys())

### count total number of files INITIALLY 

# Initialize the list to collect DataFrames
ICD_dfs_initial = []

# Loop through each partition and collect DataFrames
for i in range(1, len(all_keys_ICD) + 1):
    ICD_df = reader_ICD[f'diagnoses_partition_{i}']
    
    # Append the DataFrame to the list
    ICD_dfs_initial.append(ICD_df)

# Concatenate all initial DataFrames into a single DataFrame
ICD_df_initial = pd.concat(ICD_dfs_initial, axis=0, ignore_index=True)

# Count the total number of rows in the initial data
total_rows_initial = ICD_df_initial.shape[0]

# Print the total number of rows
print(f"Total number of rows in the initial data: {total_rows_initial}")

print(ICD_df_initial.head())

####icd+

# Define the regex patterns for ICD9 and ICD10 codes using non-capturing groups
icd9_pattern = re.compile(r'(?:^|,)\s*(3320|332\.0)\s*(?:,|$)', re.IGNORECASE)
icd10_pattern = re.compile(r'(?:^|,)\s*G20(?:\s*|,|$)', re.IGNORECASE)

# Initialize the list to collect filtered DataFrames
ICDPL_dfs = []
# icd9_code icd10_code
# Function to check if any of the codes in the cell match the pattern
def contains_pattern(cell, pattern):
    codes = [code.strip() for code in cell.split(',')]
    return any(pattern.match(code) for code in codes)

# Loop through each partition and filter the rows
for i in range(1, len(all_keys_ICD) + 1):
    ICDPL_df = reader_ICD[f'diagnoses_partition_{i}']
    
    # Strip leading/trailing spaces and convert to string for both ICD9 and ICD10 columns
    ICDPL_df['icd9_code'] = ICDPL_df['icd9_code'].astype(str).str.strip()
    ICDPL_df['icd10_code'] = ICDPL_df['icd10_code'].astype(str).str.strip()
    
    # Filter with regex pattern for both columns
    icd9_matches = ICDPL_df['icd9_code'].apply(lambda x: contains_pattern(x, icd9_pattern))
    icd10_matches = ICDPL_df['icd10_code'].apply(lambda x: contains_pattern(x, icd10_pattern))
    
    # Include rows that match either ICD9 or ICD10 patterns
    filtered_df = ICDPL_df[icd9_matches | icd10_matches]
    ICDPL_dfs.append(filtered_df)

# Concatenate all filtered DataFrames into a single DataFrame
ICDPL_df = pd.concat(ICDPL_dfs, axis=0, ignore_index=True)

# Print the 'ICD9' and 'ICD10' columns to verify the filtering
print(ICDPL_df[['icd9_code', 'icd10_code']])
print(ICDPL_df)

print('\n len before filter TW ICDPL')
print(len(ICDPL_df))
#define timewindow
start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')

ICDPL_df['shifted_date'] = pd.to_datetime(ICDPL_df['shifted_date'])  #only date available is in shifted_dat column

# FILTER BASED ON TIME WINDOW
filt_ICDPL_df = ICDPL_df[(ICDPL_df['shifted_date'] >= start_date) & (ICDPL_df['shifted_date'] < end_date)]

# print(filt_ICDPL_df['DiagnosisCode'])
print(filt_ICDPL_df)

print('\n len after filter TW ICDPL')
print(len(filt_ICDPL_df))

filt_ICDPL_df = filt_ICDPL_df.rename(columns={'shifted_date': 'CreateDate'})

filt_ICDPL_df.to_csv('STF_ICDPLUS.csv', index=False)

# ICD -

# Initialize ThunderReader and read keys
reader_ICD = ThunderReader('/home/bram/internal_expansion/HOSPITALS/Stanford/ICD')  # Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_ICD = list(reader_ICD.keys())

# Define the regex patterns for ICD9 and ICD10 codes using non-capturing groups
icd9_pattern = re.compile(r'(?:^|,)\s*(3320|332\.0)\s*(?:,|$)', re.IGNORECASE)
icd10_pattern = re.compile(r'(?:^|,)\s*G20(?:\s*|,|$)', re.IGNORECASE)

# Initialize the list to collect filtered DataFrames
ICDMN_dfs = []

# Function to check if any of the codes in the cell match the pattern
def contains_pattern(cell, pattern):
    codes = [code.strip() for code in cell.split(',')]
    return any(pattern.match(code) for code in codes)

# Loop through each partition and filter the rows
for i in range(1, len(all_keys_ICD) + 1):
    ICDMN_df = reader_ICD[f'diagnoses_partition_{i}']
    
    # Strip leading/trailing spaces and convert to string for both ICD9 and ICD10 columns
    ICDMN_df['icd9_code'] = ICDMN_df['icd9_code'].astype(str).str.strip()
    ICDMN_df['icd10_code'] = ICDMN_df['icd10_code'].astype(str).str.strip()
    
    # Filter with regex pattern for both columns
    icd9_matches = ICDMN_df['icd9_code'].apply(lambda x: contains_pattern(x, icd9_pattern))
    icd10_matches = ICDMN_df['icd10_code'].apply(lambda x: contains_pattern(x, icd10_pattern))
    
    # Exclude rows that match either ICD9 or ICD10 patterns
    excluded_df = ICDMN_df[~(icd9_matches | icd10_matches)]
    ICDMN_dfs.append(excluded_df)

# Concatenate all filtered DataFrames into a single DataFrame
ICDMN_df = pd.concat(ICDMN_dfs, axis=0, ignore_index=True)

# Print the 'ICD9' and 'ICD10' columns to verify the filtering
print(ICDMN_df[['icd9_code', 'icd10_code']])
print(ICDMN_df)

print('\n len before filter TW ICDMIN')
print(len(ICDMN_df))

#define timewindow
start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')

ICDMN_df['shifted_date'] = pd.to_datetime(ICDMN_df['shifted_date'])  #only date available is in shifted_dat column

# FILTER BASED ON TIME WINDOW
filt_ICDMN_df = ICDMN_df[(ICDMN_df['shifted_date'] >= start_date) & (ICDMN_df['shifted_date'] < end_date)]

# print(filt_ICDPL_df['DiagnosisCode'])
print(filt_ICDMN_df)

print('\n len AFTER filter TW ICDMIN')
print(len(filt_ICDMN_df))

##MED

reader_MED_ADM = ThunderReader('/home/bram/internal_expansion/HOSPITALS/Stanford/MED/MED_ADMIN') #Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_MED_ADM = list(reader_MED_ADM.keys())
print(all_keys_MED_ADM)

reader_MED_ORD = ThunderReader('/home/bram/internal_expansion/HOSPITALS/Stanford/MED/MED_ORDERS') #Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_MED_ORD = list(reader_MED_ORD.keys())
print(all_keys_MED_ORD)

# initial data admin 
MED_ADM_dfs_initial = []

# Loop through each partition and collect DataFrames
for i in range(1, len(all_keys_MED_ADM) + 1):
    MED_ADM_df = reader_MED_ADM[f'med_admin_partition_{i}']
    
    # Append the DataFrame to the list
    MED_ADM_dfs_initial.append(MED_ADM_df)

# Concatenate all initial DataFrames into a single DataFrame
MED_ADM_dfs_initial = pd.concat(MED_ADM_dfs_initial, axis=0, ignore_index=True)

# Count the total number of rows in the initial data
total_rows_initial_MED_ADM = MED_ADM_dfs_initial.shape[0]

# Print the total number of rows
print(f"Total number of rows in the initial med data admin: {total_rows_initial_MED_ADM}")

print(MED_ADM_dfs_initial.head())

MED_ADM_dfs_initial.to_csv('STF_MEDPLUS_ADM_initial.csv', index=False)
# initial data meds ordinal 

MED_ORD_dfs_initial = []

# Loop through each partition and collect DataFrames
for i in range(1, len(all_keys_MED_ORD) + 1):
    MED_ORD_df = reader_MED_ORD[f'med_orders_partition_{i}']
    
    # Append the DataFrame to the list
    MED_ORD_dfs_initial.append(MED_ORD_df)

# Concatenate all initial DataFrames into a single DataFrame
MED_ORD_dfs_initial = pd.concat(MED_ORD_dfs_initial, axis=0, ignore_index=True)

# Count the total number of rows in the initial data
total_rows_initial_MED_ORD = MED_ORD_dfs_initial.shape[0]

# Print the total number of rows
print(f"Total number of rows in the initial med data ordinal: {total_rows_initial_MED_ORD}")

print(MED_ORD_dfs_initial.head())

MED_ORD_dfs_initial.to_csv('STF_MEDPLUS_ORD_initial.csv', index=False)
## selection admin

print('\nmed admin')

MEDPL_ADM_dfs = []
for i in range(1, len(all_keys_MED_ADM)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDPL_ADM_df = reader_MED_ADM[f'med_admin_partition_{i}']
    MEDPL_ADM_df = MEDPL_ADM_df[MEDPL_ADM_df.medication.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex). case= false for capslock insensitivity
    MEDPL_ADM_dfs.append(MEDPL_ADM_df)
MEDPL_ADM_df = pd.concat(MEDPL_ADM_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe

print('\n medpladmin length')
print(len(MEDPL_ADM_df))

MEDPL_ADM_df['shifted_taken_date'] = pd.to_datetime(MEDPL_ADM_df['shifted_taken_date'])
#defined timewindow
filt_MEDPL_ADM_df = MEDPL_ADM_df[(MEDPL_ADM_df['shifted_taken_date'] >= start_date) & (MEDPL_ADM_df['shifted_taken_date'] < end_date)]
print('\n filtered medpladmin length')
print(len(filt_MEDPL_ADM_df))

filt_MEDPL_ADM_df = filt_MEDPL_ADM_df.rename(columns={'shifted_taken_date': 'CreateDate'})

filt_MEDPL_ADM_df.to_csv('STF_MEDPLUS_ADM.csv', index=False)

MEDMN_ADM_dfs = []
for i in range(1, len(all_keys_MED_ADM)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDMN_ADM_df = reader_MED_ADM[f'med_admin_partition_{i}']
    MEDMN_ADM_df = MEDMN_ADM_df[~MEDMN_ADM_df.medication.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex). case= false for capslock insensitivity
    MEDMN_ADM_dfs.append(MEDMN_ADM_df)
MEDMN_ADM_df = pd.concat(MEDMN_ADM_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe

# print(MEDMN_ADM_df)
print('\n medMNadmin length')
print(len(MEDMN_ADM_df))

MEDMN_ADM_df['shifted_taken_date'] = pd.to_datetime(MEDMN_ADM_df['shifted_taken_date'])
#defineD timewindow
filt_MEDMN_ADM_df = MEDMN_ADM_df[(MEDMN_ADM_df['shifted_taken_date'] >= start_date) & (MEDMN_ADM_df['shifted_taken_date'] < end_date)]
print('\n filtered medmnadmin length')
print(len(filt_MEDMN_ADM_df))


## selection medication orders

print('\nmed order')

MEDPL_ORD_dfs = []
for i in range(1, len(all_keys_MED_ORD)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDPL_ORD_df = reader_MED_ORD[f'med_orders_partition_{i}']
    MEDPL_ORD_df = MEDPL_ORD_df[MEDPL_ORD_df.medication.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex). case= false for capslock insensitivity
    MEDPL_ORD_dfs.append(MEDPL_ORD_df)
MEDPL_ORD_df = pd.concat(MEDPL_ORD_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe

print('\n medplorder length')
print(len(MEDPL_ORD_df))

MEDPL_ORD_df['shifted_start_date'] = pd.to_datetime(MEDPL_ORD_df['shifted_start_date'])
#defined timewindow
filt_MEDPL_ORD_df = MEDPL_ORD_df[(MEDPL_ORD_df['shifted_start_date'] >= start_date) & (MEDPL_ORD_df['shifted_start_date'] < end_date)]
print('\n filtered medplorder length')
print(len(filt_MEDPL_ORD_df))

filt_MEDPL_ORD_df = filt_MEDPL_ORD_df.rename(columns={'shifted_start_date': 'CreateDate'})

filt_MEDPL_ORD_df.to_csv('STF_MEDPLUS_ORD.csv', index=False)

MEDMN_ORD_dfs = []
for i in range(1, len(all_keys_MED_ORD)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDMN_ORD_df = reader_MED_ORD[f'med_orders_partition_{i}']
    MEDMN_ORD_df = MEDMN_ORD_df[~MEDMN_ORD_df.medication.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex). case= false for capslock insensitivity
    MEDMN_ORD_dfs.append(MEDMN_ORD_df)
MEDMN_ORD_df = pd.concat(MEDMN_ORD_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe

# print(MEDMN_ADM_df)
print('\n medMNorder length')
print(len(MEDMN_ORD_df))

MEDMN_ORD_df['shifted_start_date'] = pd.to_datetime(MEDMN_ORD_df['shifted_start_date'])
#defineD timewindow
filt_MEDMN_ORD_df = MEDMN_ORD_df[(MEDMN_ORD_df['shifted_start_date'] >= start_date) & (MEDMN_ORD_df['shifted_start_date'] < end_date)]
print('\n filtered medmnorder length')
print(len(filt_MEDMN_ORD_df))

take unique for icdpl icdmn, medpl (ord and adm), medmn (ord and adm)

# ICD 

unique_ids_ICD_plus = set(filt_ICDPL_df['bdsp_patient_id'].unique())
unique_ids_ICD_minus = set(filt_ICDMN_df['bdsp_patient_id'].unique())

print(len(unique_ids_ICD_plus))
print(len(unique_ids_ICD_minus))


#check for overlapping ids
common_patient_ids_before = unique_ids_ICD_plus.intersection(unique_ids_ICD_minus)
print('\n amount common patients before')
print(len(common_patient_ids_before))

# exlude overlapping Ids from mimus
unique_ids_ICD_minus = unique_ids_ICD_minus.difference(unique_ids_ICD_plus)

# evaluate
common_patient_ids_after= unique_ids_ICD_plus.intersection(unique_ids_ICD_minus)
print('\n amount common patients after')
print(len(common_patient_ids_after))

print(len(unique_ids_ICD_plus))
print(len(unique_ids_ICD_minus))

# # MED

# MEDPL
print(len(MEDPL_ADM_df['bdsp_patient_id'].unique()))
print(len(MEDPL_ORD_df['bdsp_patient_id'].unique()))

# Combine the values from both columns
combined_ids_MEDPL = pd.concat([MEDPL_ORD_df['bdsp_patient_id'], MEDPL_ADM_df['bdsp_patient_id']])

# Extract unique values and convert to a set
unique_ids_MEDPL = set(combined_ids_MEDPL.unique())

# Display the result
print(len(unique_ids_MEDPL))

# MEDMN
print(len(MEDMN_ADM_df['bdsp_patient_id'].unique()))
print(len(MEDMN_ORD_df['bdsp_patient_id'].unique()))

# Combine the values from both columns
combined_ids_MEDMN = pd.concat([MEDMN_ORD_df['bdsp_patient_id'], MEDMN_ADM_df['bdsp_patient_id']])

# Extract unique values and convert to a set
unique_ids_MEDMN = set(combined_ids_MEDMN.unique())

# Display the result
print(len(unique_ids_MEDMN))

#check for overlapping ids
print('\n CHECK FOR OVERLAP MED')
common_patient_ids_before_MED = unique_ids_MEDPL.intersection(unique_ids_MEDMN)
print('\n amount common patients before')
print(len(common_patient_ids_before_MED))

# exlude overlapping Ids from mimus
unique_ids_MEDMN = unique_ids_MEDMN.difference(unique_ids_MEDPL)

# evaluate
common_patient_ids_after_MED= unique_ids_MEDPL.intersection(unique_ids_MEDMN)
print('\n amount common patients after')
print(len(common_patient_ids_after_MED))

print(len(unique_ids_MEDPL))
print(len(unique_ids_MEDMN))

# get groups


print('\n plpl')
ICDPLMEDPL=unique_ids_ICD_plus.intersection(unique_ids_MEDPL)
print(len(ICDPLMEDPL))

ICDPLMEDPL_list = list(ICDPLMEDPL)

# # Create a DataFrame with a specified column name
ICDPLMEDPL_df = pd.DataFrame(ICDPLMEDPL_list, columns=['BDSPPatientID'])
print(len(ICDPLMEDPL_df))
ICDPLMEDPL_df.to_csv('ICDPLMEDPL_Stan.csv', index=False)

print('\n plmi')
ICDPLMEDMI=unique_ids_ICD_plus.intersection(unique_ids_MEDMN)
print(len(ICDPLMEDMI))

##CONVERSION TO CSV
#first to dataframe and then to csv (used method)
ICDPLMEDMI_list = list(ICDPLMEDMI)

# # Create a DataFrame with a specified column name
ICDPLMEDMI_df = pd.DataFrame(ICDPLMEDMI_list, columns=['BDSPPatientID'])
print(len(ICDPLMEDMI_df))
ICDPLMEDMI_df.to_csv('ICDPLMEDMI_Stan.csv', index=False)

print('\n mipl')
ICDMIMEDPL=unique_ids_ICD_minus.intersection(unique_ids_MEDPL)
print(len(ICDMIMEDPL))

#first to dataframe and then to csv (used method)
ICDMIMEDPL_list = list(ICDMIMEDPL)

# # Create a DataFrame with a specified column name
ICDMIMEDPL_df = pd.DataFrame(ICDMIMEDPL_list, columns=['BDSPPatientID'])
print(len(ICDMIMEDPL_df))
ICDMIMEDPL_df.to_csv('ICDMIMEDPL_Stan.csv', index=False)

print('\n mimi')
ICDMIMEDMI=unique_ids_ICD_minus.intersection(unique_ids_MEDMN)
print(len(ICDMIMEDMI))

#first to dataframe and then to csv (used method)
ICDMIMEDMI_list = list(ICDMIMEDMI)

# # Create a DataFrame with a specified column name
ICDMIMEDMI_df = pd.DataFrame(ICDMIMEDMI_list, columns=['BDSPPatientID'])
print(len(ICDMIMEDMI_df))
ICDMIMEDMI_df.to_csv('ICDMIMEDMI_Stan.csv', index=False)