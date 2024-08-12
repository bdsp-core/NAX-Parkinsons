import pandas as pd
from thunderpack import ThunderReader  # install thunderpack if not yet: pip install thunderpack
import pandas as pd
# import pyodbc
# import random
# import zipfile
# import io
import re 
import csv

reader_ICD = ThunderReader('/media/bram/Expansion/Bidmc/ICD')  # Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_ICD = list(reader_ICD.keys())


### count total number of files

# Initialize the list to collect DataFrames
ICDMN_dfs_initial = []

# Loop through each partition and collect DataFrames
for i in range(1, len(all_keys_ICD) + 1):
    ICDMN_df = reader_ICD[f'ICD_partition_{i}']
    
    # Append the DataFrame to the list
    ICDMN_dfs_initial.append(ICDMN_df)

# Concatenate all initial DataFrames into a single DataFrame
ICDMN_df_initial = pd.concat(ICDMN_dfs_initial, axis=0, ignore_index=True)

# Count the total number of rows in the initial data
total_rows_initial = ICDMN_df_initial.shape[0]

# Print the total number of rows
print(f"Total number of rows in the initial data: {total_rows_initial}")

## icdpl

ICDPL_dfs = []

pattern = re.compile(r'^(3320|G20)$', re.IGNORECASE)  # pattern = re.compile(r'^(332\.?0|G20)$', re.IGNORECASE)

for i in range(1, len(all_keys_ICD) + 1):
    ICDPL_df = reader_ICD[f'ICD_partition_{i}']
    
    # Debugging: Print unique DiagnosisCode values
    # print(f"Unique DiagnosisCode values in partition {i}:")
    # print(ICDPL_df['DiagnosisCode'].unique())
    
    # Strip leading/trailing spaces and convert to string
    ICDPL_df['DiagnosisCode'] = ICDPL_df['DiagnosisCode'].astype(str).str.strip()
    
    # Filter with regex pattern
    ICDPL_df = ICDPL_df[ICDPL_df.DiagnosisCode.str.match(pattern)]
    
    ICDPL_dfs.append(ICDPL_df)

ICDPL_df = pd.concat(ICDPL_dfs, axis=0, ignore_index=True)

# # Print the 'DiagnosisCode' column
print(ICDPL_df['DiagnosisCode'])
print(ICDPL_df)
# print(ICDPL_df['DiagnosisCode'])

# #keep only the required columns
ICDPL_df = ICDPL_df.drop([
    'BDSPEncounterID',
    'DiagnosisPoaInd',
    'DiagnosisCodeWithDots',
    'ShortDescription',
    'LongDescription',
    'DiagnosisType',
    'code_type',
    'BDSPLastModifiedDTS'
], axis=1)  #REMOVE UNWANTED COLUMNS. for med we want to keep: 'BDSPPatientID', 'createdate','StartDate', 'StopDate', 'MedName'
# print(ICDPL_df)
print(ICDPL_df['DiagnosisCode'])
print(ICDPL_df)


#define timewindow
start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')

ICDPL_df['AdmissionDate'] = pd.to_datetime(ICDPL_df['AdmissionDate'])

# FILTER BASED ON TIME WINDOW
filt_ICDPL_df = ICDPL_df[(ICDPL_df['AdmissionDate'] >= start_date) & (ICDPL_df['AdmissionDate'] < end_date)]

print(filt_ICDPL_df['DiagnosisCode'])
print(filt_ICDPL_df)
filt_ICDPL_df = filt_ICDPL_df.rename(columns={'AdmissionDate': 'CreateDate'})

filt_ICDPL_df.to_csv('BIDMC_ICDPLUS.csv', index=False)
### neg

# Initialize ThunderReader with the correct directory path
reader_ICD = ThunderReader('/media/bram/Expansion/Bidmc/ICD')
all_keys_ICD = list(reader_ICD.keys())

# Initialize the list to collect filtered DataFrames
ICDMN_dfs = []

# Compile the regex pattern for exact matching with case insensitivity
pattern = re.compile(r'^(3320|G20)$', re.IGNORECASE)

# Loop through each partition
for i in range(1, len(all_keys_ICD) + 1):
    ICDMN_df = reader_ICD[f'ICD_partition_{i}']
    
    # Debugging: Print unique DiagnosisCode values
    # print(f"Unique DiagnosisCode values in partition {i}:")
    # print(ICDMN_df['DiagnosisCode'].unique())
    
    # Strip leading/trailing spaces and convert to string
    ICDMN_df['DiagnosisCode'] = ICDMN_df['DiagnosisCode'].astype(str).str.strip()
    
    # Exclude rows where DiagnosisCode matches the specified pattern
    ICDMN_df = ICDMN_df[~ICDMN_df.DiagnosisCode.str.match(pattern)]
    
    # Append the filtered DataFrame to the list
    ICDMN_dfs.append(ICDMN_df)

# Concatenate all filtered DataFrames into a single DataFrame
ICDMN_df = pd.concat(ICDMN_dfs, axis=0, ignore_index=True)

# Print the 'DiagnosisCode' column
print(ICDMN_df['DiagnosisCode'])
print(ICDMN_df)

# keep only the required columns
ICDMN_df = ICDMN_df.drop([
    'BDSPEncounterID',
    'DiagnosisPoaInd',
    'DiagnosisCodeWithDots',
    'ShortDescription',
    'LongDescription',
    'DiagnosisType',
    'code_type',
    'BDSPLastModifiedDTS'
], axis=1)  #REMOVE UNWANTED COLUMNS. for med we want to keep: 'BDSPPatientID', 'createdate','StartDate', 'StopDate', 'MedName'

print(ICDMN_df['DiagnosisCode'])
print('\n The table for ICD- before time window is filtered')
print(ICDMN_df)

ICDMN_df['AdmissionDate'] = pd.to_datetime(ICDMN_df['AdmissionDate'])

# FILTER BASED ON TIME WINDOW
filt_ICDMN_df= ICDMN_df[(ICDMN_df['AdmissionDate'] >= start_date) & (ICDMN_df['AdmissionDate'] < end_date)]

print(ICDMN_df['DiagnosisCode'])
print(ICDMN_df)


##MEDICATION

reader_MED = ThunderReader('/media/bram/Expansion/Bidmc/MED') #Make sure the path provided to ThunderReader is the directory that contains the data managed by Thunderpack, not a path to an individual .MDB file or incorrect directory.
all_keys_MED = list(reader_MED.keys())

# Initialize the list to collect DataFrames
MED_dfs_initial = []

# Loop through each partition and collect DataFrames
for i in range(1, len(all_keys_MED) + 1):
    MED_df = reader_MED[f'meds_partition_{i}']
    
    # Append the DataFrame to the list
    MED_dfs_initial.append(MED_df)

# Concatenate all initial DataFrames into a single DataFrame
MED_df_initial = pd.concat(MED_dfs_initial, axis=0, ignore_index=True)

# Count the total number of rows in the initial data
total_MED_rows_initial = MED_df_initial.shape[0]

# Print the total number of rows
print(f"Total number of rows in the initial data MED: {total_MED_rows_initial}")


MEDPL_dfs = []
for i in range(1, len(all_keys_MED)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDPL_df = reader_MED[f'meds_partition_{i}']
    MEDPL_df = MEDPL_df[MEDPL_df.MedName.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex). case= false for capslock insensitivity
    MEDPL_dfs.append(MEDPL_df)
MEDPL_df = pd.concat(MEDPL_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe

print('\n LEN MEDPL BEFORE TIMEFILTERING')
print (len(MEDPL_df))

# print(MEDPL_df)
MEDPL_df = MEDPL_df.drop(['Strength', 'Route', 'Dispense', 'DoseForm', 'Refills', 'Duration', 'Sig', 'Frequency', 'PRN', 'TakeAmount', 'TakeUnit', 'TakeRoute', 'Instructions', 'AdditionalInstructions', 'DiscontinueDate', 'DiscontinueInd', 'DiscontinueReason', 'DiscontinueFiledDate', 'Action', 'TakingAsPrescribed', 'DosageUncertainInd', 'BDSPLastModifiedDTS'], axis=1)  #REMOVE UNWANTED COLUMNS. for med we want to keep: 'createdate','StartDate', 'StopDate', 'MedName'
# print(MEDPL_df)


MEDPL_df['CreateDate'] = pd.to_datetime(MEDPL_df['CreateDate'])
#define timewindow
start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')

filt_MEDPL_df = MEDPL_df[(MEDPL_df['CreateDate'] >= start_date) & (MEDPL_df['CreateDate'] < end_date)]

filt_MEDPL_df.to_csv('BIDMC_MEDPLUS.csv', index=False)
MEDMN_dfs = []
for i in range(1, len(all_keys_MED)+1):    # create a for loop to loop over all keys: meds_partition_1, meds_partition_2, ... You can find all keys by `print(list(reader.keys()))`, +1 in order to include final key as well
    MEDMN_df = reader_MED[f'meds_partition_{i}']
    MEDMN_df = MEDMN_df[~MEDMN_df.MedName.astype(str).str.contains('(?:Levodopa|Carbidopa|Dopamine|Dopamineagonists|Monoamineoxidase|Catechol-O-methyltransferase|Apomorphine|Rotigotine|Pramipexole|Ropinirole|Selegiline|Rasagiline|Entacapone|Tolcapone|Trihexyphenidyl|Benztropine|Amantadine|Stalevo|Neupro|Mirapex|Requip|Sinemet|Madopar|Comtan|Azilect|Eldepryl|Zelapar|Bromocriptine|Pergolide|Safinamide|Opicapone|Zonisamide|Duopa|Inbrija|Xadago|Nuplazid|Ongentys|Nourianz|RequipXL|Apokyn|Zydis|Tasmar|COMT|MAO|Istradefylline|Rytary)', regex=True, case=False)]  # for example, you want to filter and keep only meds that starts with .....  '^(?:G20|332)' is a regular expression (regex).
    MEDMN_dfs.append(MEDMN_df)
MEDMN_df = pd.concat(MEDMN_dfs, axis=0, ignore_index=True)  # concatenate all filtered dataframes from all partitions into one dataframe
print('\n LEN MEDMN BEFORE TIMEFILTERING')
print (len(MEDMN_df))
#keep only the required columns
MEDMN_df = MEDMN_df.drop(['Strength', 'Route', 'Dispense', 'DoseForm', 'Refills', 'Duration', 'Sig', 'Frequency', 'PRN', 'TakeAmount', 'TakeUnit', 'TakeRoute', 'Instructions', 'AdditionalInstructions', 'DiscontinueDate', 'DiscontinueInd', 'DiscontinueReason', 'DiscontinueFiledDate', 'Action', 'TakingAsPrescribed', 'DosageUncertainInd', 'BDSPLastModifiedDTS'], axis=1)  #REMOVE UNWANTED COLUMNS. for med we want to keep: 'BDSPPatientID', 'createdate','StartDate', 'StopDate', 'MedName'
# print(MEDMN_df)


#filter on time window (May 1st 2016-Jan 1st 2023)
MEDMN_df['CreateDate'] = pd.to_datetime(MEDMN_df['CreateDate'])
filt_MEDMN_df = MEDMN_df[(MEDMN_df['CreateDate'] >= start_date) & (MEDMN_df['CreateDate'] <= end_date)]

MED_plus = filt_MEDPL_df.rename(columns={'CreateDate': 'AdmissionDate'})
MED_minus = filt_MEDMN_df.rename(columns={'CreateDate': 'AdmissionDate'})
ICD_plus= filt_ICDPL_df
ICD_minus= filt_ICDMN_df

# make set of unique patient ID'S for MED+ AND MED-
unique_ids_MED_plus = set(MED_plus['BDSPPatientID'].unique())
unique_ids_MED_minus= set(MED_minus['BDSPPatientID'].unique())

#exclude overlappingg ids from med-

unique_ids_MED_minus = unique_ids_MED_minus.difference(unique_ids_MED_plus)

# print(unique_ids_MED_minus)
# print(unique_ids_MED_plus)

# make set of unique patient ID'S for icd+ AND icd-

unique_ids_ICD_plus = set(ICD_plus['BDSPPatientID'].unique())
unique_ids_ICD_minus = set(ICD_minus['BDSPPatientID'].unique())

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

# print(unique_ids_ICD_plus)
# print(unique_ids_ICD_minus)





# get the combinations between ICD and med groups
#first to dataframe and then to csv (used method)


print('\n plpl')
ICDPLMEDPL=unique_ids_ICD_plus.intersection(unique_ids_MED_plus)
print(len(ICDPLMEDPL))
ICDPLMEDPL_list = list(ICDPLMEDPL)
ICDPLMEDPL_df = pd.DataFrame(ICDPLMEDPL_list, columns=['BDSPPatientID'])
print(len(ICDPLMEDPL_df))
ICDPLMEDPL_df.to_csv('ICDPLMEDPL.csv', index=False)




print('\n plmi')
ICDPLMEDMI=unique_ids_ICD_plus.intersection(unique_ids_MED_minus)
print(len(ICDPLMEDMI))
ICDPLMEDMI_list = list(ICDPLMEDMI)
ICDPLMEDMI_df = pd.DataFrame(ICDPLMEDMI_list, columns=['BDSPPatientID'])
print(len(ICDPLMEDMI_df))
ICDPLMEDMI_df.to_csv('ICDPLMEDMI.csv', index=False)



print('\n mipl')
ICDMIMEDPL=unique_ids_ICD_minus.intersection(unique_ids_MED_plus)
print(len(ICDMIMEDPL))
ICDMIMEDPL_list = list(ICDMIMEDPL)
ICDMIMEDPL_df = pd.DataFrame(ICDMIMEDPL_list, columns=['BDSPPatientID'])
print(len(ICDMIMEDPL_df))
ICDMIMEDPL_df.to_csv('ICDMIMEDPL.csv', index=False)



print('\n mimi')
ICDMIMEDMI=unique_ids_ICD_minus.intersection(unique_ids_MED_minus)
print(len(ICDMIMEDMI))
ICDMIMEDMI_list = list(ICDMIMEDMI)
ICDMIMEDMI_df = pd.DataFrame(ICDMIMEDMI_list, columns=['BDSPPatientID'])
print(len(ICDMIMEDMI_df))
ICDMIMEDMI_df.to_csv('ICDMIMEDMI.csv', index=False)
