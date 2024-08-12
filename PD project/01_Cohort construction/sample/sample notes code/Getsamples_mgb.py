import pandas as pd
from thunderpack import ThunderReader  # install thunderpack if not yet: pip install thunderpack
import pandas as pd
import tarfile
import pyodbc
import random
import zipfile
import io
import tempfile
import tqdm


df_0 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2016_metadata.csv')
df_1 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2017_metadata.csv')
df_2 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2018_metadata.csv')
df_3 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2019_metadata.csv')
df_4 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2020_metadata.csv')
df_5 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2021_metadata.csv')
df_6 = pd.read_csv('/home/bram/internal_expansion/MGB/notes/meta/mgb_notes_2022_metadata.csv')

dfs = [df_0,df_1, df_2, df_3, df_4, df_5, df_6]  # List of DataFrames
print(dfs)



ICDPLMEDPL_MGB = pd.DataFrame()
ICDPLMEDMI_MGB = pd.DataFrame()
ICDMIMEDPL_MGB = pd.DataFrame()  # Note: This name is a duplicate from the previous one
ICDMIMEDMI_MGB = pd.DataFrame()


complete_df = pd.concat(dfs, ignore_index=True)
print(len(complete_df)) 
print(complete_df.head())

print(complete_df['ContactDate'].dtype)


#Convert to String
complete_df['ContactDate'] = complete_df['ContactDate'].astype(str)

#Convert to Datetime
complete_df['ContactDate'] = pd.to_datetime(complete_df['ContactDate'], format='%Y%m%d')

print(complete_df['ContactDate'].head())
print(complete_df['ContactDate'].dtype)

start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')
complete_df_n = complete_df[(complete_df['ContactDate'] >= start_date) & (complete_df['ContactDate'] < end_date)]
print(complete_df_n)

import zipfile
import random

def select_random_note_zip(df, patient_id, base_zip_path):
    patient_notes = df[df['BDSPPatientID'] == patient_id]
    valid_notes = []
    zip_file = None

    for _, row in patient_notes.iterrows():
        year = row['DeidentifiedName'].split('_')[-1][:4]
        file_path = f'mgb_notes_{year}/{year}/' + row['DeidentifiedName']
        zip_path = f'{base_zip_path}mgb_notes_{year}.zip'
        
        try:
            if zip_file is None or zip_file.filename != zip_path:
                if zip_file is not None:
                    zip_file.close()
                zip_file = zipfile.ZipFile(zip_path, 'r')
            
            with zip_file.open(file_path) as file_obj:
                try:
                    contents = file_obj.read().decode('utf-8')
                except UnicodeDecodeError:
                    try:
                        contents = file_obj.read().decode('latin1')
                    except UnicodeDecodeError:
                        contents = file_obj.read().decode('iso-8859-1')
                
                word_count = len(contents.split())
                print(f"Length of contents for {file_path}: {word_count}")
                if word_count >= 500:
                    valid_notes.append((row['BDSPPatientID'], contents, row['ContactDate'], word_count, row['DeidentifiedName'], 'MGB'))

        except (KeyError, FileNotFoundError, zipfile.BadZipFile) as e:
            print(f"Error processing file {file_path}: {str(e)}")
            continue

    if zip_file is not None:
        zip_file.close()

    if valid_notes:
        selected_note = random.choice(valid_notes)
        return selected_note
    else:
        return None



base_zip_path= '/home/bram/zip/'

PLPL_ANNOTATED_df= pd.read_csv('MGH_SAMPLE/ELIJAH/SAMPLE_ICDPLMEDPL_MGH_ELIJAH.csv')

ICDPLMEDPL_df=pd.read_csv('COHORT_MGB/ICDPLMEDPL_MGB.csv')
print(ICDPLMEDPL_df.columns)
FULLICDPLMEDPL_list = ICDPLMEDPL_df['BDSPPatientID'].tolist()
columns = ['BDSPPatientID', 'note_text', 'CreateDate', 'length', 'deidentifiedname', 'hospital']


counterPLPL = 0
# added_patient_idsPLPL = set(ICDPLMEDPL_MGB['PatientID'].tolist()) if 'PatientID' in ICDPLMEDPL_MGB.columns else set()
added_patient_idsPLPL = set(PLPL_ANNOTATED_df['BDSPPatientID'].tolist()) if 'BDSPPatientID' in PLPL_ANNOTATED_df.columns else set()
print(len(added_patient_idsPLPL))
print(added_patient_idsPLPL)

while len(ICDPLMEDPL_MGB) < 334  and FULLICDPLMEDPL_list:
    randpatientID = random.choice(FULLICDPLMEDPL_list)
    UPDATED_LEN_GROUP=len(FULLICDPLMEDPL_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsPLPL:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDPL_MGB = pd.concat([ICDPLMEDPL_MGB, pd.DataFrame([rnote2])], ignore_index=True) #, columns=columns
            added_patient_idsPLPL.add(randpatientID)
            FULLICDPLMEDPL_list.remove(randpatientID)
            counterPLPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDPLMEDPL_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")
        FULLICDPLMEDPL_list.remove(randpatientID)
    if not FULLICDPLMEDPL_list:
        print("No more patient IDs left to process")




ICDPLMEDPL_MGB.columns = columns
print(ICDPLMEDPL_MGB)
ICDPLMEDPL_MGB.to_csv('SAMPLE_ICDPLMEDPL_MGB.csv', index=False)


# mimi

#import already annotated data's information
MIMI_ANNOTATED_df= pd.read_csv('MGH_SAMPLE/ELIJAH/SAMPLE_ICDMIMEDMI_MGH_ELIJAH.csv')

ICDMIMEDMI_df=pd.read_csv('COHORT_MGB/ICDMIMEDMI_MGB.csv')
print(ICDMIMEDMI_df.columns)
FULLICDMIMEDMI_list = ICDMIMEDMI_df['BDSPPatientID'].tolist()

counterMIMI = 0
added_patient_idsMIMI = set(MIMI_ANNOTATED_df['BDSPPatientID'].tolist()) if 'BDSPPatientID' in MIMI_ANNOTATED_df.columns else set()
print(len(added_patient_idsMIMI))
print(added_patient_idsMIMI)

while len(ICDMIMEDMI_MGB) < 58 and FULLICDMIMEDMI_list:
    randpatientID = random.choice(FULLICDMIMEDMI_list)
    UPDATED_LEN_GROUP=len(FULLICDMIMEDMI_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsMIMI:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDMI_MGB = pd.concat([ICDMIMEDMI_MGB, pd.DataFrame([rnote2])], ignore_index=True)
            added_patient_idsMIMI.add(randpatientID)
            FULLICDMIMEDMI_list.remove(randpatientID)
            counterMIMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDMIMEDMI_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")
        FULLICDMIMEDMI_list.remove(randpatientID)

    if not FULLICDMIMEDMI_list:
        print("No more patient IDs left to process")


ICDMIMEDMI_MGB.columns = columns
print(ICDMIMEDMI_MGB)
ICDMIMEDMI_MGB.to_csv('SAMPLE_ICDMIMEDMI_MGB.csv', index=False)

#PLMI

PLMI_ANNOTATED_df= pd.read_csv('MGH_SAMPLE/ELIJAH/SAMPLE_ICDPLMEDMI_MGH_ELIJAH.csv')

# Load the ICDPLMEDMI data
ICDPLMEDMI_df = pd.read_csv('COHORT_MGB/ICDPLMEDMI_MGB.csv')
print(ICDPLMEDMI_df.columns)

# Create a list of patient IDs
FULLICDPLMEDMI_list = ICDPLMEDMI_df['BDSPPatientID'].tolist()

# Initialize the counter and set of added patient IDs
counterPLMI = 0
added_patient_idsPLMI = set(PLMI_ANNOTATED_df['BDSPPatientID'].tolist()) if 'BDSPPatientID' in PLMI_ANNOTATED_df.columns else set()
print(len(added_patient_idsPLMI))
print(added_patient_idsPLMI)



# Main loop to add notes
while len(ICDPLMEDMI_MGB) < 433 and FULLICDPLMEDMI_list:
    randpatientID = random.choice(FULLICDPLMEDMI_list)
    UPDATED_LEN_GROUP = len(FULLICDPLMEDMI_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    
    if randpatientID not in added_patient_idsPLMI:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDMI_MGB = pd.concat([ICDPLMEDMI_MGB, pd.DataFrame([rnote2])], ignore_index=True)
            added_patient_idsPLMI.add(randpatientID)
            FULLICDPLMEDMI_list.remove(randpatientID)
            counterPLMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDPLMEDMI_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")
        FULLICDPLMEDMI_list.remove(randpatientID)

    if not FULLICDPLMEDMI_list:
        print("No more patient IDs left to process")

# Set columns of the resulting DataFrame
ICDPLMEDMI_MGB.columns = columns
print(ICDPLMEDMI_MGB)

# Save the resulting DataFrame to a CSV file
ICDPLMEDMI_MGB.to_csv('SAMPLE_ICDPLMEDMI_MGB.csv', index=False)

#MIPL

MIPL_ANNOTATED_df= pd.read_csv('MGH_SAMPLE/ELIJAH/SAMPLE_ICDMIMEDPL_MGH_ELIJAH.csv')

ICDMIMEDPL_df = pd.read_csv('COHORT_MGB/ICDMIMEDPL_MGB.csv')
print(ICDMIMEDPL_df.columns)

# Create a list of patient IDs
FULLICDMIMEDPL_list = ICDMIMEDPL_df['BDSPPatientID'].tolist()

# Initialize the counter and set of added patient IDs
counterMIPL = 0
added_patient_idsMIPL = set(MIPL_ANNOTATED_df['BDSPPatientID'].tolist()) if 'BDSPPatientID' in MIPL_ANNOTATED_df.columns else set()
print(len(added_patient_idsMIPL))
print(added_patient_idsMIPL)


# Main loop to add notes
while len(ICDMIMEDPL_MGB) < 334 and FULLICDMIMEDPL_list:
    randpatientID = random.choice(FULLICDMIMEDPL_list)
    UPDATED_LEN_GROUP = len(FULLICDMIMEDPL_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    
    if randpatientID not in added_patient_idsMIPL:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDPL_MGB = pd.concat([ICDMIMEDPL_MGB, pd.DataFrame([rnote2])], ignore_index=True)
            added_patient_idsMIPL.add(randpatientID)
            FULLICDMIMEDPL_list.remove(randpatientID)
            counterMIPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDMIMEDPL_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")
        FULLICDMIMEDPL_list.remove(randpatientID)

    if not FULLICDMIMEDPL_list:
        print("No more patient IDs left to process")

# Set columns of the resulting DataFrame
ICDMIMEDPL_MGB.columns = columns
print(ICDMIMEDPL_MGB)

# Save the resulting DataFrame to a CSV file
ICDMIMEDPL_MGB.to_csv('SAMPLE_ICDMIMEDPL_MGB.csv', index=False)