import pandas as pd
import requests
import io
import psycopg2
from requests.auth import HTTPBasicAuth
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Kobo credentials
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
SCHEMA_NAME = "university"
TABLE_NAME = "admission"

# 1. Fetch data from KoboToolbox
response = requests.get(KOBO_CSV_URL, auth=HTTPBasicAuth(KOBO_USERNAME, KOBO_PASSWORD))

if response.status_code == 200:
    print("☑️ Data fetched successfully from KoboToolbox")

    csv_data = io.StringIO(response.text)
    df = pd.read_csv(csv_data, sep=";",on_bad_lines="skip")  # <-- Removed `sep=','`

    print("First few rows of the DataFrame:")
    # Drop columns if they exist
    df.drop(columns=["Phone_Number", "Year_of_Graduation","Date"], inplace=True, errors='ignore')
    df.dropna(inplace=True, how='all')  # Drop rows where all elements are NaN
    # Clean and standardize column names
    df.columns = (
        df.columns.str.replace(r"[ \-().,\'\":;?!]", "_", regex=True)
                  .str.strip("_")
                  .str.lower()
    )

    print("🧾 Cleaned column names:", df.columns.tolist())

    # Uploading the data to PostgreSQL
    print("Uploading data to PostgreSQL database ....")
    conn = psycopg2.connect(
        host=PG_HOST,
        port=PG_PORT,
        database=PG_DATABASE,
        user=PG_USER,
        password=PG_PASSWORD
    )

    cur = conn.cursor()
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
    cur.execute(f"DROP TABLE IF EXISTS {SCHEMA_NAME}.{TABLE_NAME};")

    cur.execute(f"""
        CREATE TABLE {SCHEMA_NAME}.{TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            "start" TIMESTAMP,
            "end" TIMESTAMP,        
            full_name TEXT,
            date_of_birth TEXT,
            gender TEXT,
            email_address TEXT,
            district_of_residence TEXT,
            educational_background TEXT,
            program_of_interest TEXT,
            university_choices TEXT,
            scholarships TEXT,
            admission_status TEXT
        );
    """)

    insert_query = f"""
        INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
            "start", "end", full_name, date_of_birth, gender,
            email_address, district_of_residence, educational_background, 
            program_of_interest, university_choices, scholarships, admission_status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    # Filter out rows with missing values in the required columns
    df.dropna(subset=["gender", "district_of_residence"], inplace=True)
    for _, row in df.iterrows():
        cur.execute(insert_query, (
            row.get("start"),
            row.get("end"),
            row.get("full_name"),
            row.get("date_of_birth"),
            row.get("gender"),
            row.get("email_address"),
            row.get("district_of_residence"),
            row.get("educational_background"),
            row.get("program_of_interest"),
            row.get("university_choices"),
            row.get("scholarships"),
            row.get("admission_status")
        ))

    conn.commit()
    cur.close()
    conn.close()
    print("☑️ Data uploaded successfully to PostgreSQL database")
    print("☑️ Data processing completed successfully")

else:
    print(f"❌ Failed to fetch data from KoboToolbox. Status code: {response.status_code}")
    print(response.text)