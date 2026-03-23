import sqlite3
import pymysql
import pyodbc
from tabulate import tabulate
import pandas as pd
import numpy as np
import os
import glob
import warnings
from html2image import Html2Image
from tabulate import tabulate
import smtplib
from IPython.display import display_png
import imgkit
from PIL import Image
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.image import MIMEImage
from email import encoders
import dataframe_image as dfi
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2.service_account import Credentials
from gspread_pandas import Spread, Client
from decimal import Decimal
import warnings
import io
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
warnings.simplefilter(action='ignore', category=FutureWarning)

import requests
import re
from sqlalchemy import create_engine, text
#==============================================
condition = "PartnerCode IN ('IP110231')"
#==============================================

sql_urls = {
    "Motor": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Motor_Query_Lead_Level.sql",
    "Health_Fresh": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Health_Fresh_Query_Lead_Level.sql",
    "Health_Renewal": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Health_Renewal_Query_Lead_Level.sql",
    "Life": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Life_Query_Lead_Level.sql",
    "SME": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_SME_Query_Lead_Level.sql",
    "Health_Persistency": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Health_Persistency.sql"
}

engine = create_engine(
    "mssql+pyodbc://Ranjaysingh:j%405%40Pj1%23%24%21ive"
    "@pbpartnersqldb.cnizbobl3wux.ap-south-1.rds.amazonaws.com/PospDB"
    "?driver=ODBC+Driver+17+for+SQL+Server"
)

dfs = {}

for name, url in sql_urls.items():
    
    sql_script = requests.get(url).content.decode("utf-8-sig").rstrip().rstrip(";")
    
    if "-- CONDITION_PLACEHOLDER" in sql_script:
        sql_script = sql_script.replace(
            "-- CONDITION_PLACEHOLDER",
            f"AND {condition}"
        )
    else:
        sql_script = f"""
        SELECT *
        FROM (
            {sql_script}
        ) t
        WHERE {condition}
        """
    dfs[name] = pd.read_sql_query(sql_script, engine)

# Assign outputs
df_Motor = dfs["Motor"]
df_Health_Fresh = dfs["Health_Fresh"]
df_Health_Renewal = dfs["Health_Renewal"]
df_Life = dfs["Life"]
df_SME = dfs["SME"]
df_Health_Persistency = dfs["Health_Persistency"]