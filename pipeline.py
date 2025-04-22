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
KOBO_USERNAME = os.getenv("KOBO_PASSWORD")
KOBO_CSV_URL = os.getenv("KOBO_CSV_URL")