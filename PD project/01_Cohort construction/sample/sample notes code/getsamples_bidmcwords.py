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

df_0 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2016_metadata.csv')
df_1 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2017_metadata.csv')
df_2 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2018_metadata.csv')
df_3 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2019_metadata.csv')
df_4 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2020_metadata.csv')
df_5 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2021_metadata.csv')
df_6 = pd.read_csv('/media/bram/Expansion/Bidmc/notes/metadata/bidmc_notes_2022_metadata.csv')


dfs = [df_0,df_1, df_2, df_3, df_4, df_5, df_6]  # List of DataFrames

# Concatenate all DataFrames in the list
complete_df = pd.concat(dfs, ignore_index=True)
print(len(complete_df))  # validated
print(complete_df.head())

print(complete_df['CreateDate'].dtype)


#Convert to String
complete_df['CreateDate'] = complete_df['CreateDate'].astype(str)

#Convert to Datetime
complete_df['CreateDate'] = pd.to_datetime(complete_df['CreateDate'], format='%Y%m%d')

print(complete_df['CreateDate'].head())
print(complete_df['CreateDate'].dtype)

start_date = pd.Timestamp('2016-05-01')
end_date = pd.Timestamp('2023-01-01')
complete_df_n = complete_df[(complete_df['CreateDate'] >= start_date) & (complete_df['CreateDate'] < end_date)]
print(complete_df_n)



def select_random_note_zip(df, patient_id, base_zip_path):
    patient_notes = df[df['BDSPPatientID'] == patient_id]
    valid_notes = []
    zip_file = None

    for _, row in patient_notes.iterrows():
        year = row['DeidentifiedName'].split('_')[-1][:4]
        file_path = f'bidmc_notes_{year}/{year}/' + row['DeidentifiedName']
        zip_path = f'{base_zip_path}bidmc_notes_{year}.zip'
        
        try:
            if zip_file is None or zip_file.filename != zip_path:
                if zip_file is not None:
                    zip_file.close()
                zip_file = zipfile.ZipFile(zip_path, 'r')
            
            with zip_file.open(file_path) as file_obj:
                contents = file_obj.read().decode('utf-8')
                word_count = len(contents.split())
                # print(f"Length of contents for {file_path}: {len(contents)}")
                if word_count >= 500:
                    valid_notes.append((row['BDSPPatientID'], contents, row['CreateDate'], word_count, row['DeidentifiedName']))  # Append as dictionary
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


# Define the column names
columns = ['BDSPPatientID', 'note_text', 'CreateDate', 'length', 'deidentifiedname']

# Create the empty DataFrames with specified column names
ICDPLMEDPL_BIDMC = pd.DataFrame()
ICDPLMEDMI_BIDMC = pd.DataFrame()
ICDMIMEDPL_BIDMC = pd.DataFrame()  # Note: This name is a duplicate from the previous one
ICDMIMEDMI_BIDMC = pd.DataFrame()

base_zip_path= '/home/bram/zip/'
# combined_list=[150994510, 150429263, 151363155]#not in plpl[150146700, 151219102, 151219104]

# PLPL

# load ICDLIST (not set)
ICDPLMEDPL_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/BIDMC/BG_BIDMC/ICDPLMEDPL.csv')
FULLICDPLMEDPL_list = ICDPLMEDPL_df['BDSPPatientID'].tolist()
# print(column_list)
# ICDPLMEDPL= set(ICDPLMEDPL_list['BDSPPatientID'])        #{150146700,150850795,151219102}
# print(len(ICDPLMEDPL))
counterPLPL=0
added_patient_idsPLPL = set(ICDPLMEDPL_BIDMC['PatientID'].tolist()) if 'PatientID' in ICDPLMEDPL_BIDMC.columns else set()
print(added_patient_idsPLPL)

while len(ICDPLMEDPL_BIDMC) < 250: #250 or len(df2) < 250 or len(df3) < 250 or len(df4) < 250:
    randpatientID = random.choice(FULLICDPLMEDPL_list)
    # Condition 1: Elements divisible by 2
    if (randpatientID in FULLICDPLMEDPL_list) and (randpatientID not in added_patient_idsPLPL)  and len(ICDPLMEDPL_BIDMC) < 250: #randpatientID in ICDPLMEDPL and len(df1) < 250:
        rnote2=select_random_note_zip(complete_df_n,randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDPL_BIDMC = pd.concat([ICDPLMEDPL_BIDMC, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsPLPL.add(randpatientID)
            counterPLPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
    # elif 
    else:
        print('no matches')

# ICDPLMEDPL_BIDMC.columns = columns
print(ICDPLMEDPL_BIDMC)
ICDPLMEDPL_BIDMC.to_csv('SAMPLE_ICDPLMEDPL_BIDMC.csv', index=True)


##mimi
# load ICDLIST/SET
ICDMIMEDMI_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/BIDMC/BG_BIDMC/ICDMIMEDMI.csv')
FULLICDMIMEDMI_list = ICDMIMEDMI_df['BDSPPatientID'].tolist()
# print(column_list)
# ICDPLMEDPL= set(ICDPLMEDPL_list['BDSPPatientID'])        #{150146700,150850795,151219102}
# print(len(ICDPLMEDPL))

added_patient_idsMIMI = set(ICDMIMEDMI_BIDMC['PatientID'].tolist()) if 'PatientID' in ICDMIMEDMI_BIDMC.columns else set()
print(added_patient_idsMIMI)
counterMIMI = 0
while len(ICDMIMEDMI_BIDMC) < 250: #250 or len(df2) < 250 or len(df3) < 250 or len(df4) < 250:
    randpatientID = random.choice(FULLICDMIMEDMI_list)
    # Condition 1: Elements divisible by 2
    if (randpatientID in FULLICDMIMEDMI_list) and (randpatientID not in added_patient_idsMIMI)  and len(ICDMIMEDMI_BIDMC) < 250: #randpatientID in ICDPLMEDPL and len(df1) < 250:
        rnote2=select_random_note_zip(complete_df_n,randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDMI_BIDMC = pd.concat([ICDMIMEDMI_BIDMC, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsMIMI.add(randpatientID)
            counterMIMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
    else:
        print('no matches')

# ICDMIMEDMI_BIDMC.columns = columns
print(ICDMIMEDMI_BIDMC)
ICDMIMEDMI_BIDMC.to_csv('SAMPLE_ICDMIMEDMI_BIDMC.csv', index=False)

# ##MIPL
# # load ICDLIST/SET
ICDMIMEDPL_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/BIDMC/BG_BIDMC/ICDMIMEDPL.csv')
FULLICDMIMEDPL_list = ICDMIMEDPL_df['BDSPPatientID'].tolist()


added_patient_idsMIPL = set(ICDMIMEDPL_BIDMC['PatientID'].tolist()) if 'PatientID' in ICDMIMEDPL_BIDMC.columns else set()
print(added_patient_idsMIPL)
counterMIPL = 0
while len(ICDMIMEDPL_BIDMC) < 250: #250 or len(df2) < 250 or len(df3) < 250 or len(df4) < 250:
    randpatientID = random.choice(FULLICDMIMEDPL_list)
    # Condition 1: Elements divisible by 2
    if (randpatientID in FULLICDMIMEDPL_list) and (randpatientID not in added_patient_idsMIPL)  and len(ICDMIMEDPL_BIDMC) < 250: #randpatientID in ICDPLMEDPL and len(df1) < 250:
        rnote2=select_random_note_zip(complete_df_n,randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDMIMEDPL_BIDMC = pd.concat([ICDMIMEDPL_BIDMC, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsMIPL.add(randpatientID)
            counterMIPL += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterMIPL}")
        else:
            print(f"No valid notes for patient {randpatientID}")
    else:
        print('no matches')

# ICDMIMEDPL_BIDMC.columns = columns
print(ICDMIMEDPL_BIDMC)
ICDMIMEDPL_BIDMC.to_csv('SAMPLE_ICDMIMEDPL_BIDMC.csv', index=False)

# ##PLMI
# # load ICDLIST/SET
ICDPLMEDMI_df=pd.read_csv('/home/bram/internal_expansion/HOSPITALS/BIDMC/BG_BIDMC/ICDPLMEDMI.csv')
FULLICDPLMEDMI_list = ICDPLMEDMI_df['BDSPPatientID'].tolist()


added_patient_idsPLMI = set(ICDPLMEDMI_BIDMC['PatientID'].tolist()) if 'PatientID' in ICDPLMEDMI_BIDMC.columns else set()
print(added_patient_idsPLMI)
counterPLMI = 0
while len(ICDPLMEDMI_BIDMC) < 250: #250 or len(df2) < 250 or len(df3) < 250 or len(df4) < 250:
    randpatientID = random.choice(FULLICDPLMEDMI_list)
    # Condition 1: Elements divisible by 2
    if (randpatientID in FULLICDPLMEDMI_list) and (randpatientID not in added_patient_idsPLMI)  and len(ICDPLMEDMI_BIDMC) < 250: #randpatientID in ICDPLMEDPL and len(df1) < 250:
        rnote2=select_random_note_zip(complete_df_n,randpatientID, base_zip_path)
        if rnote2 is not None:
            ICDPLMEDMI_BIDMC = pd.concat([ICDPLMEDMI_BIDMC, pd.DataFrame([rnote2], columns=columns)], ignore_index=True)
            added_patient_idsPLMI.add(randpatientID)
            counterPLMI += 1
            print(f"Added note for patient {randpatientID}. Total notes added: {counterPLMI}")
        else:
            print(f"No valid notes for patient {randpatientID}")
    else:
        print('no matches')

# ICDPLMEDMI_BIDMC.columns = columns
print(ICDPLMEDMI_BIDMC)
ICDPLMEDMI_BIDMC.to_csv('SAMPLE_ICDPLMEDMI_BIDMC.csv', index=False)














# if len(ICDPLMEDPL_BIDMC) < 2: #250:




# while len(ICDPLMEDPL_BIDMC) < 2: #250 or len(df2) < 250 or len(df3) < 250 or len(df4) < 250:
#     randpatientID = random.choice(ICDPLMEDPL)
#     # Condition 1: Elements divisible by 2
#     if (randpatientID in ICDPLMEDPL) and (randpatientID not in added_patient_ids)  and len(ICDPLMEDPL_BIDMC) < 250: #randpatientID in ICDPLMEDPL and len(df1) < 250:
#         rnote2=select_random_note_zip(df_0,randpatientID, base_zip_path)
#         ICDPLMEDPL_BIDMC = pd.concat([ICDPLMEDPL_BIDMC, pd.DataFrame([rnote2])], ignore_index=True)
#         added_patient_ids.add(randpatientID)
#         print (added_patient_ids)
#     # elif 
#     else:
#         print('no matches')

# ICDPLMEDPL_BIDMC.columns = columns
# print(ICDPLMEDPL_BIDMC)