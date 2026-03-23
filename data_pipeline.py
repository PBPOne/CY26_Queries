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

# ---------------- DB CONNECTION ---------------- #

def get_engine():
    return create_engine(
        "mssql+pyodbc://Ranjaysingh:j%405%40Pj1%23%24%21ive"
        "@pbpartnersqldb.cnizbobl3wux.ap-south-1.rds.amazonaws.com/PospDB"
        "?driver=ODBC+Driver+17+for+SQL+Server"
    )

# ---------------- QUERY EXECUTION ---------------- #

def run_queries(condition):

    engine = get_engine()
    dfs = {}

    for name, url in sql_urls.items():

        print(f"Running query: {name}")

        sql_script = requests.get(url).content.decode("utf-8-sig").rstrip().rstrip(";")

        # Apply condition
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

    return dfs

# ---------------- DATA PROCESSING ---------------- #

def process_data(dfs):

    df_Motor = dfs["Motor"]
    df_Health_Fresh = dfs["Health_Fresh"]
    df_Health_Renewal = dfs["Health_Renewal"]
    df_Life = dfs["Life"]
    df_SME = dfs["SME"]

    # Combine
    Data = pd.concat(
        [df_Motor, df_Health_Fresh, df_Health_Renewal, df_Life, df_SME],
        axis=0
    )

    # Fill flags
    Data.fillna({
        'policy_booked_flag': 1,
        'policy_issued_flag': 1,
        'policy_verified_flag': 1,
        'motor_booked_flag': 0,
        'motor_cancelled_flag': 0
    }, inplace=True)

    # Fallback mapping
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

    # Date conversion
    if 'MON' in Data.columns:
        Data['MON'] = pd.to_datetime(Data['MON'], errors='coerce')

    if 'Prev_end_date' in Data.columns:
        Data['Prev_end_date'] = pd.to_datetime(Data['Prev_end_date'], errors='coerce')

    return Data

# ---------------- MAIN FUNCTION ---------------- #

def get_final_data(partner_codes):

    # Build condition dynamically
    partners = ",".join([f"'{p}'" for p in partner_codes])
    condition = f"PartnerCode IN ({partners})"

    dfs = run_queries(condition)

    Data = process_data(dfs)

    return Data
