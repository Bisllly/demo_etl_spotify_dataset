import os
from dotenv import load_dotenv
import subprocess

load_dotenv()


database = os.getenv("DWH_DATABASE")
user = os.getenv("DWH_USER")
password = os.getenv("DWH_PWD")
db_host = os.getenv("DWH_HOST")


ps_script = f"""
$database = '{database}'
$user = '{user}'
$password = '{password}'
$db_host = '{db_host}'

$tables = psql -h $db_host -U $user -d $database -t -c "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema';"

foreach ($table in $tables) {{
    Write-Host "Table: $table"
    psql -h $db_host -U $user -d $database -c "SELECT * FROM $table LIMIT 5;"
    Write-Host ""
}}
"""

subprocess.run(["powershell", "-Command", ps_script], check=True)
