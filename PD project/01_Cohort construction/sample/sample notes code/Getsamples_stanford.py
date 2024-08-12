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

df_0 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2016.csv')
df_1 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2017.csv')
df_2 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2018.csv')
df_3 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2019.csv')
df_4 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2020.csv')
df_5 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2021.csv')
df_6 = pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/notes/meta/I0004_notes_2022.csv')



dfs = [df_0,df_1, df_2, df_3, df_4, df_5, df_6] 

complete_df = pd.concat(dfs, ignore_index=True)
print(len(complete_df))  # validated
print(complete_df.head())

#Convert to Datetime
complete_df['shifted_date'] = pd.to_datetime(complete_df['shifted_date'])

print(complete_df['shifted_date'].head())
print(complete_df['shifted_date'].dtype)

start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')
complete_df_n = complete_df[(complete_df['shifted_date'] >= start_date) & (complete_df['shifted_date'] < end_date)]
print(complete_df_n)


def select_random_note_zip(df, patient_id, base_zip_path):
    patient_notes = df[df['bdsp_patient_id'] == patient_id]
    valid_notes = []
    zip_file = None

    for _, row in patient_notes.iterrows():
        year = row['filename'].split('_')[-1][:4]
        file_path = f'I0004_notes_{year}/{year}/' + row['filename']
        zip_path = f'{base_zip_path}I0004_notes_{year}.zip'
        
        try:
            if zip_file is None or zip_file.filename != zip_path:
                if zip_file is not None:
                    zip_file.close()
                zip_file = zipfile.ZipFile(zip_path, 'r')
            
            with zip_file.open(file_path) as file_obj:
                contents = file_obj.read().decode('utf-8')
                # print(f"Length of contents for {file_path}: {len(contents)}")
                word_count = len(contents.split())
                # print(f"Length of contents for {file_path}: {len(contents)}")
                if word_count >= 500:
                    valid_notes.append((row['bdsp_patient_id'], contents, row['shifted_date'], word_count, row['filename']))  # Append as dictionary
                    # print(f"Added valid note for {file_path}")

        except (KeyError, FileNotFoundError, zipfile.BadZipFile) as e:
            print(f"Error processing file {file_path}: {str(e)}")
            continue

    if zip_file is not None:
        zip_file.close()

    if valid_notes:
        selected_note = random.choice(valid_notes)
        # print(f"Selected note: {selected_note}") #comment this out
        return selected_note
    else:
        return None
    
    
columns = ['BDSPPatientID', 'note_text', 'ShiftedDate', 'length', 'deidentifiedname']

# Create the empty DataFrames with specified column names
ICDPLMEDPL_STAN = pd.DataFrame()
ICDPLMEDMI_STAN = pd.DataFrame()
ICDMIMEDPL_STAN = pd.DataFrame()  # Note: This name is a duplicate from the previous one
ICDMIMEDMI_STAN = pd.DataFrame()

base_zip_path= '/home/bram/zip/'


ICDPLMEDPL_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/BG_STAN/ICDPLMEDPL_Stan.csv')
print(ICDPLMEDPL_df.columns)
FULLICDPLMEDPL_list = ICDPLMEDPL_df['BDSPPatientID'].tolist()

counterPLPL = 0
added_patient_idsPLPL = set(ICDPLMEDPL_STAN['PatientID'].tolist()) if 'PatientID' in ICDPLMEDPL_STAN.columns else set()
print(added_patient_idsPLPL)

while len(ICDPLMEDPL_STAN) < 250 and FULLICDPLMEDPL_list:
    randpatientID = random.choice(FULLICDPLMEDPL_list)
    UPDATED_LEN_GROUP=len(FULLICDPLMEDPL_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsPLPL:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDPL_STAN = pd.concat([ICDPLMEDPL_STAN, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsPLPL.add(randpatientID)
            FULLICDPLMEDPL_list.remove(randpatientID)
            counterPLPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDPLMEDPL_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")

    if not FULLICDPLMEDPL_list:
        print("No more patient IDs left to process")


ICDPLMEDPL_STAN.columns = columns
print(ICDPLMEDPL_STAN)
ICDPLMEDPL_STAN.to_csv('SAMPLE_ICDPLMEDPL_STAN.csv', index=False)

# mimi

ICDMIMEDMI_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/BG_STAN/ICDMIMEDMI_Stan.csv')
print(ICDMIMEDMI_df.columns)
FULLICDMIMEDMI_list = ICDMIMEDMI_df['BDSPPatientID'].tolist()

counterMIMI = 0
added_patient_idsMIMI = set(ICDMIMEDMI_STAN['PatientID'].tolist()) if 'PatientID' in ICDMIMEDMI_STAN.columns else set()
print(added_patient_idsMIMI)

while len(ICDMIMEDMI_STAN) < 250 and FULLICDMIMEDMI_list:
    randpatientID = random.choice(FULLICDMIMEDMI_list)
    UPDATED_LEN_GROUP=len(FULLICDMIMEDMI_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsMIMI:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDMI_STAN = pd.concat([ICDMIMEDMI_STAN, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsMIMI.add(randpatientID)
            FULLICDMIMEDMI_list.remove(randpatientID)
            counterMIMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDMIMEDMI_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")

    if not FULLICDMIMEDMI_list:
        print("No more patient IDs left to process")


ICDMIMEDMI_STAN.columns = columns
print(ICDMIMEDMI_STAN)
ICDMIMEDMI_STAN.to_csv('SAMPLE_ICDMIMEDMI_STAN.csv', index=False)


#PLMI
ICDPLMEDMI_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/BG_STAN/ICDPLMEDMI_Stan.csv')
print(ICDPLMEDMI_df.columns)
FULLICDPLMEDMI_list = ICDPLMEDMI_df['BDSPPatientID'].tolist()

counterPLMI = 0
added_patient_idsPLMI = set(ICDPLMEDMI_STAN['PatientID'].tolist()) if 'PatientID' in ICDPLMEDMI_STAN.columns else set()
print(added_patient_idsPLMI)

while len(ICDPLMEDMI_STAN) < 250 and FULLICDPLMEDMI_list:
    randpatientID = random.choice(FULLICDPLMEDMI_list)
    UPDATED_LEN_GROUP=len(FULLICDPLMEDMI_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsPLMI:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDMI_STAN = pd.concat([ICDPLMEDMI_STAN, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsPLMI.add(randpatientID)
            FULLICDPLMEDMI_list.remove(randpatientID)
            counterPLMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDPLMEDMI_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")

    if not FULLICDPLMEDMI_list:
        print("No more patient IDs left to process")


ICDPLMEDMI_STAN.columns = columns
print(ICDPLMEDMI_STAN)
ICDPLMEDMI_STAN.to_csv('SAMPLE_ICDPLMEDMI_STAN.csv', index=False)

#MIPL

ICDMIMEDPL_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/Stanford/BG_STAN/ICDMIMEDPL_Stan.csv')
print(ICDMIMEDPL_df.columns)
FULLICDMIMEDPL_list = ICDMIMEDPL_df['BDSPPatientID'].tolist()

counterMIPL = 0
added_patient_idsMIPL = set(ICDMIMEDPL_STAN['PatientID'].tolist()) if 'PatientID' in ICDMIMEDPL_STAN.columns else set()
print(added_patient_idsMIPL)

while len(ICDMIMEDPL_STAN) < 250 and FULLICDMIMEDPL_list:
    randpatientID = random.choice(FULLICDMIMEDPL_list)
    UPDATED_LEN_GROUP=len(FULLICDMIMEDPL_list)
    print(f"Updated length list {UPDATED_LEN_GROUP}.")
    if randpatientID not in added_patient_idsMIPL:
        rnote2 = select_random_note_zip(complete_df_n, randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDPL_STAN = pd.concat([ICDMIMEDPL_STAN, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsMIPL.add(randpatientID)
            FULLICDMIMEDPL_list.remove(randpatientID)
            counterMIPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
            FULLICDMIMEDPL_list.remove(randpatientID)
    else:
        print(f"Patient {randpatientID} already has a note added")

    if not FULLICDMIMEDPL_list:
        print("No more patient IDs left to process")


ICDMIMEDPL_STAN.columns = columns
print(ICDMIMEDPL_STAN)
ICDMIMEDPL_STAN.to_csv('SAMPLE_ICDMIMEDPL_STAN.csv', index=False)