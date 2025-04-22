import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kobo credentials connect to the API
KOBO_USERNAME = os.getenv("KOBO_USERNAME")
KOBO_PASSWORD = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = "https://kf.kobotoolbox.org/api/v2/assets/aekRSLCFjSiCMZoish7Sp9/export-settings/es8qxEyD2PDNYgLzcSkqauC/data.csv"

# PostgreSQL credentials
PG_HOST = os.getenv("PG_HOST")
PG_PORT = os.getenv("PG_PORT")
PG_DATABASE = os.getenv("PG_DATABASE")
PG_USER = os.getenv("PG_USER")
PG_PASSWORD = os.getenv("PG_PASSWORD")

# Schema and table details
SCHEMA_NAME = "Education"
table_name = "Admission"  # it is good practice to avoid special characters
reponse = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

#1. Fetch data from kobotoolsbox
if reponse.status_code == 200:
    # Read the CSV data into a pandas DataFrame
    print("Data fetched successfully from KoboToolbox")
    csv_data = io.StringIO (reponse.text)
    df = pd.read_csv(csv_data, sep=",", on_bad_lines="skip")
    # Display the first few rows of the DataFrame 
    df.head()

    # Cleaning the DataFrame from any unwanted characters
    print("Processing data ....")
    df.columns = df.columns.str.replace(" ", "_").str.replace("-", "_").str.replace("(", "")

    # Compute the number of applications 
    # df['Total applicatns']= df['Age'].sumcum()

    # Convert the data to proper types 
    df["Date"]= pd.to_datetime(df["Date"], format="%Y-%m-%d", errors="coerce")
    
    # Uploading the data to PostgreSQL database
    print("Uploading data to PostgreSQL database ....")
    # Create a connection to the PostgreSQL database

    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )   

    cur = conn.cursor()
    # Create the schema if it doesn't exist
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

    # Drop and create the table if it exists
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{table_name};")
    cur.execute(f"""
        CREATE TABLE {SCHEMA_NAME}.{table_name} (
        ID SERIAL PRIMARY KEY,
        "start" TIMESTAMP,
        "end" TIMESTAMP,        
        "Date" TIMESTAMP,
        Full_Name text,
        Date_of_Birth text,
        Gender text,
        Phone_Number text,
        Email_Address text,
        District_of_Residence text,
        Educational_Background text,
        Year_of_Graduation INTEGER,
        Program_of_Interest text,
        University_Choices text,
        Scholarships text,
        Admission_Status text
        );
    """) 
    # Insert the data into the table
    insert_query = f"""
       INSERT INTO {SCHEMA_NAME}.{table_name} (
       "start", "end", "Date", Full_Name, Date_of_Birth,
       Gender, Phone_Number, Email_Address, District_of_Residence,
       Educational_Background, Year_of_Graduation, Program_of_Interest,
       University_Choices, Scholarships, Admission_Status)
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
    # Convert the DataFrame to a list of tuples