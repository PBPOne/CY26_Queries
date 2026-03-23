import requests
import pandas as pd
from sqlalchemy import create_engine

# ---------------- CONFIG ---------------- #

sql_urls = {
    "Motor": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Motor_Query_Lead_Level.sql",
    "Health_Fresh": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Health_Fresh_Query_Lead_Level.sql",
    "Health_Renewal": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Health_Renewal_Query_Lead_Level.sql",
    "Life": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_Life_Query_Lead_Level.sql",
    "SME": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Lead_Level_Queries/New_Construct_2026_SME_Query_Lead_Level.sql",
    "Health_Persistency": "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/Health_Persistency.sql"
}

# ---------------- DB ---------------- #

def get_engine():
    return create_engine(
        "mssql+pyodbc://Ranjaysingh:j%405%40Pj1%23%24%21ive"
        "@pbpartnersqldb.cnizbobl3wux.ap-south-1.rds.amazonaws.com/PospDB"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )

# ---------------- QUERY ---------------- #

def run_queries(condition):

    engine = get_engine()
    dfs = {}

    for name, url in sql_urls.items():

        print(f"Running: {name}")

        sql_script = requests.get(url).content.decode("utf-8-sig").rstrip().rstrip(";")

        if "-- CONDITION_PLACEHOLDER" in sql_script:
            sql_script = sql_script.replace(
                "-- CONDITION_PLACEHOLDER",
                f"AND {condition}"
            )
        else:
            sql_script = f"""
            SELECT *
            FROM ({sql_script}) t
            WHERE {condition}
            """

        dfs[name] = pd.read_sql_query(sql_script, engine)

    return dfs

# ---------------- PROCESS ---------------- #

def process_data(dfs):

    Data = pd.concat(
        [dfs["Motor"], dfs["Health_Fresh"], dfs["Health_Renewal"], dfs["Life"], dfs["SME"]],
        axis=0
    )

    Data.fillna({
        'policy_booked_flag': 1,
        'policy_issued_flag': 1,
        'policy_verified_flag': 1,
        'motor_booked_flag': 0,
        'motor_cancelled_flag': 0
    }, inplace=True)

    fallback_map = {
        'Accrual_Net_Pr': 'netpr',
        'Accrual_Net_Ins': 'Accrual_Net_Pr',
        'Accrual_Net_Booked': 'Accrual_Net',
        'W_Net': 'Accrual_Net',
        'W_Net_C': 'Accrual_Net_C',
        'W_Net_Booked': 'W_Net',
        'bt': 'product_name'
    }

    for target, source in fallback_map.items():
        if target in Data.columns and source in Data.columns:
            Data[target] = Data[target].fillna(Data[source])

    if 'MON' in Data.columns:
        Data['MON'] = pd.to_datetime(Data['MON'], errors='coerce')

    if 'Prev_end_date' in Data.columns:
        Data['Prev_end_date'] = pd.to_datetime(Data['Prev_end_date'], errors='coerce')

    return Data

# ---------------- MAIN ---------------- #

def get_final_data(condition):

    dfs = run_queries(condition)
    Data = process_data(dfs)

    return Data
