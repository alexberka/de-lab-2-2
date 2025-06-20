import pandas as pd
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import os
import csv
import re

def load_bdi3(blob_data)->pd.DataFrame:
    bdi3_df = pd.read_csv(BytesIO(blob_data))

    target_columns = [
    'Location - Sub Level 1',
    'Child ID',
    'TEIDS Child ID',
    'Adaptive Developmental Quotient',
    'Social-Emotional Developmental Quotient',
    'Communication Developmental Quotient',
    'Motor Developmental Quotient',
    'Cognitive Developmental Quotient',
    'BDI-3 Total Developmental Quotient',
    'Adaptive-Self Care Date of Testing',
    'Adaptive-Personal Responsibility Date of Testing',
    'Social Emotional-Adult Interaction Date of Testing',
    'Social Emotional-Peer Interaction Date of Testing',
    'Social Emotional-Self Concept / Social Role Date of Testing',
    'Communication-Receptive Communication Date of Testing',
    'Communication-Expressive Communication Date of Testing',
    'Motor-Gross Motor Date of Testing',
    'Motor-Fine Motor Date of Testing',
    'Motor-Perceptual Motor Date of Testing',
    'Cognitive-Attention and Memory Date of Testing',
    'Cognitive-Reasoning / Academic Skills Date of Testing',
    'Cognitive-Perception and Concepts Date of Testing'
    ]

    bdi3_filtered = bdi3_df[target_columns]
    column_lists = bdi3_filtered.columns.to_list()
    new_columns = [rename_columns(name) for name in column_lists]
    bdi3_filtered.columns = new_columns
    return bdi3_filtered
    
def load_eco(blob_data):
    eco_df = pd.read_csv(BytesIO(blob_data), na_values=["#DIV/0!"])
    target_columns = [
        'DISTRICT',
        'CHILD_ID',
        'ECO_Entry_DATE',
        'ECO_Exit_DATE',
        
        'Exit SOCIAL_SCALE', 
        'Exit KNOWLEDGE_SCALE',
        'Exit APPROPRIATE_ACTION_SCALE', 
        # 'Initial IFSP Date',
        # 'Exit Date \nOr end of FY',
        'TEIDS\nECO_Entry_DATE',
        # 'ECO_Continued_DATE',
        'AEPSi\nECO_Entry_DATE',
        'AEPSi \nECO_Exit_DATE',
        'BDI 3\nECO_Entry_DATE',
        'BDI 3\nECO_Exit_DATE', 
        'BDI3 \nExit SOCIAL_SCALE', 
        'BDI3\nExit KNOWLEDGE_SCALE', 
        'BDI3\nExit APPROPRIATE_ACTION_SCALE', 
        'BDI2 Entry Date',
        'BDI-2\nEntry SOCIAL_SCALE',
        'BDI-2\nEntry KNOWLEDGE_SCALE',
        'BDI-2\nEntry APPROPRIATE_ACTION_SCALE'      
    ]
    eco_filtered = eco_df[target_columns]
    column_lists = eco_filtered.columns.to_list()
    new_columns = [rename_columns(name) for name in column_lists]
    eco_filtered.columns = new_columns
    return eco_filtered

def read_filtered(blob_data):
    filtered_data = BytesIO(blob_data)
    data_read = []
    reader = csv.DictReader(filtered_data.read().decode('utf-8').splitlines())
    [data_read.append(row) for row in reader]
    
    return data_read
    
def insert_data(data, db_class, session):
    for d in data:
        for key, value in d.items():
            if value == 'null':
                d[key] = None
    try:
        data_items = [db_class(**datum) for datum in data]
        print(data_items)
        session.add_all(data_items)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Error inserting data: {e}")
        

def rename_columns(column_name):
    # print(column_list)
    name = re.sub(r'\n', ' ',column_name.strip())
    name = re.sub(r'[^\w\s]', ' ', name) # any non alphanumeric = ' '
    name = re.sub(r'\s+', ' ', name)
    name = name.lower().replace(' ', '_')
    return name

def process_bdi3_nulls(df:pd.DataFrame)-> pd.DataFrame:
    quotient_columns=df.filter(like="developmental_quotient").columns.to_list()
    df[quotient_columns] = df[quotient_columns].fillna(0).astype(int)

    date_columns = df.filter(like="date").columns.to_list()
    df[date_columns] = df[date_columns].fillna('1/1/1900')
    return df

def process_eco_nulls(df:pd.DataFrame)-> pd.DataFrame:
    scale_columns=df.filter(like="scale").columns.to_list()
    df[scale_columns] = df[scale_columns].fillna(0).astype(int)

    date_columns = df.filter(like="date").columns.to_list()
    df[date_columns] = df[date_columns].fillna('1/1/1900')
    return df