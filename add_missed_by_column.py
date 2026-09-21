import requests

url = "https://raw.githubusercontent.com/PBPOne/CY26_Queries/main/add_missed_by_column.py"
exec(requests.get(url).text)

# add_missing_next_club_col is now available directly
df = add_missing_next_club_col(df, 'Current_WNet', 'Imperial_Target', output_col='Imperial_Gap')
