import os
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient
import azure.functions as func
import logging
import pandas as pd
from sqlalchemy import create_engine, Column, String, Boolean, BigInteger, Date, Integer
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils import load_bdi3, load_eco, read_filtered, insert_data, process_bdi3_nulls, process_eco_nulls

load_dotenv(override=True)

USER = os.getenv('PG_USER')
PASSWORD = os.getenv('PG_PASSWORD')
HOST = os.getenv('PG_HOST')
PORT = os.getenv('PG_PORT')
DB = os.getenv('PG_DB')

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}',echo=True)
Base = declarative_base()


STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")
FILE_NAME_BDI = os.getenv("AZURE_STORAGE_BLOB_NAME_BDI")
FILE_NAME_BDIF = os.getenv("AZURE_STORAGE_BLOB_NAME_BDIF")
FILE_NAME_ECO = os.getenv("AZURE_STORAGE_BLOB_NAME_ECO")
FILE_NAME_ECOF = os.getenv("AZURE_STORAGE_BLOB_NAME_ECOF")


app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def df_to_blob(df:pd.DataFrame, client:BlobServiceClient, filename):
    # cast as integer
    csv_string = df.to_csv(index=False)
    # Convert the CSV string to a bytes object
    csv_bytes = str.encode(csv_string)

    # Upload the bytes object to Azure Blob Storage
    blob_client = client.get_blob_client(container=CONTAINER_NAME, blob=filename)
    blob_client.upload_blob(csv_bytes, overwrite=True)


# define the database model
class Bdi(Base):
    __tablename__ = 'bdi'
    id = Column('id', Integer, primary_key=True, autoincrement=True, nullable=False)
    teids_child_id = Column('teids_child_id', String)
    child_id = Column('child_id', String)
    location_sub_level_1 = Column('location_sub_level_1', String)
    adaptive_developmental_quotient = Column('adaptive_developmental_quotient', BigInteger)
    social_emotional_developmental_quotient = Column('social_emotional_developmental_quotient', BigInteger)
    communication_developmental_quotient = Column('communication_developmental_quotient', BigInteger)
    motor_developmental_quotient = Column('motor_developmental_quotient', BigInteger)
    cognitive_developmental_quotient = Column('cognitive_developmental_quotient', BigInteger)
    bdi_3_total_developmental_quotient = Column('bdi_3_total_developmental_quotient', BigInteger)
    adaptive_self_care_date_of_testing = Column('adaptive_self_care_date_of_testing', Date, nullable=True)
    adaptive_personal_responsibility_date_of_testing = Column('adaptive_personal_responsibility_date_of_testing', Date, nullable=True)
    social_emotional_adult_interaction_date_of_testing = Column('social_emotional_adult_interaction_date_of_testing', Date, nullable=True)
    social_emotional_peer_interaction_date_of_testing = Column('social_emotional_peer_interaction_date_of_testing', Date, nullable=True)
    social_emotional_self_concept_social_role_date_of_testing = Column('social_emotional_self_concept_social_role_date_of_testing', Date, nullable=True)
    communication_receptive_communication_date_of_testing = Column('communication_receptive_communication_date_of_testing', Date, nullable=True)
    communication_expressive_communication_date_of_testing = Column('communication_expressive_communication_date_of_testing', Date,nullable=True)
    motor_gross_motor_date_of_testing = Column('motor_gross_motor_date_of_testing', Date, nullable=True)
    motor_fine_motor_date_of_testing = Column('motor_fine_motor_date_of_testing', Date, nullable=True)
    motor_perceptual_motor_date_of_testing = Column('motor_perceptual_motor_date_of_testing', Date, nullable=True)
    cognitive_attention_and_memory_date_of_testing = Column('cognitive_attention_and_memory_date_of_testing', Date, nullable=True)
    cognitive_reasoning_academic_skills_date_of_testing = Column('cognitive_reasoning_academic_skills_date_of_testing', Date, nullable=True)
    cognitive_perception_and_concepts_date_of_testing = Column('cognitive_perception_and_concepts_date_of_testing', Date, nullable=True)

    def __repr__(self):
        return f"<Bdi(child_id='{self.child_id}', location_sub_level_1='{self.location_sub_level_1}', teids_child_id='{self.teids_child_id}')>"

class Eco(Base):
    __tablename__ = 'eco'
    district = Column('district', String)
    child_id = Column('child_id', String, primary_key=True)
    eco_entry_date = Column('eco_entry_date', Date)
    eco_exit_date = Column('eco_exit_date', Date)
    exit_social_scale = Column('exit_social_scale', BigInteger)
    exit_knowledge_scale = Column('exit_knowledge_scale', BigInteger)
    exit_appropriate_action_scale = Column('exit_appropriate_action_scale', BigInteger)
    teids_eco_entry_date = Column('teids_eco_entry_date', Date)
    aepsi_eco_entry_date = Column('aepsi_eco_entry_date', Date)
    aepsi_eco_exit_date = Column('aepsi_eco_exit_date', Date)
    bdi_3_eco_entry_date = Column('bdi_3_eco_entry_date', Date)
    bdi_3_eco_exit_date = Column('bdi_3_eco_exit_date', Date)
    bdi3_exit_social_scale = Column('bdi3_exit_social_scale', BigInteger)
    bdi3_exit_knowledge_scale = Column('bdi3_exit_knowledge_scale', BigInteger)
    bdi3_exit_appropriate_action_scale = Column('bdi3_exit_appropriate_action_scale', BigInteger)
    bdi2_entry_date = Column('bdi2_entry_date', Date)
    bdi_2_entry_social_scale = Column('bdi_2_entry_social_scale', BigInteger)
    bdi_2_entry_knowledge_scale = Column('bdi_2_entry_knowledge_scale', BigInteger)
    bdi_2_entry_appropriate_action_scale = Column('bdi_2_entry_appropriate_action_scale', BigInteger)

    def __repr__(self):
        return f"<Eco(child_id='{self.child_id}', district='{self.district}')>"

Base.metadata.create_all(engine)

@app.route(route="read_bdi3")
def read_bdi3(req) -> func.HttpResponse:
    try:
        #call get_blob_client
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob = FILE_NAME_BDI
        )
        blob_data = blob_client.download_blob(offset=0, length=1024*1024).readall()  
        bdi3_data = load_bdi3(blob_data)
        
        bdi3_data = process_bdi3_nulls(bdi3_data)
        
        df_to_blob(bdi3_data, blob_service_client, FILE_NAME_BDIF)  
        return func.HttpResponse(
            "BDI3 data collected saved successfully",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
    
@app.route(route="read_eco")
def read_eco(req) -> func.HttpResponse:
    try:
        #call get_blob_client
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob = FILE_NAME_ECO
        )
        blob_data = blob_client.download_blob(offset=0, length=1024*1024).readall()
        eco_data = load_eco(blob_data)

        eco_data = process_eco_nulls(eco_data)
        
        df_to_blob(eco_data, blob_service_client,FILE_NAME_ECOF) 
        return func.HttpResponse(
            "ECO data saved successfully",
            status_code=200
        )
    except Exception as e:
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )
        
        
@app.route(route="save_postgres")
def save_postgres(req) -> func.HttpResponse:
    filename = req.params.get('filename')
    
    if filename == 'bdi':
        blob_name = FILE_NAME_BDIF 
        db_class = Bdi
    elif filename == 'eco':
        blob_name = FILE_NAME_ECOF
        db_class = Eco
    else:
        return FileNotFoundError
    try:
        #call get_blob_client
        blob_service_client = BlobServiceClient.from_connection_string(STORAGE_CONNECTION_STRING)
        try:
            blob_client = blob_service_client.get_blob_client(
            container=CONTAINER_NAME,
            blob = blob_name
        )
        except FileNotFoundError:
            return func.HttpResponse(
                "Blob file not found.",
                status_code=404
            )
            
        blob_data = blob_client.download_blob().readall()
        bdifiltered = read_filtered(blob_data)
        Session = sessionmaker(bind = engine)
        session = Session()
        insert_data(bdifiltered,db_class,session)
        session.close()
        return func.HttpResponse(
            "data saved to postgres successfully",
            status_code=200
        )
    
    except Exception as e:
        return func.HttpResponse(
            f"An error occurred: {e}",
            status_code=500
        )