import pandas as pd
import pyodbc
from datetime import datetime, timedelta
import os
import streamlit as st
import requests
import polars as pl
import json
import toml
import fastexcel

# Enable efficient DataFrame operations
pd.options.mode.copy_on_write = True

# Define the output directory
OUTPUT_DIR = '/Users/kenechukwuchukwuka/Downloads/streamlit/DATADUMP'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Define the API endpoints
enrollee_url = "https://compare-backend.clearlinehmo.com/api/v1/enrollee/get-all?status=active"
company_url = "https://compare-backend.clearlinehmo.com/api/v1/company/get-all"

# Function to fetch data from the API
def fetch_data(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data from {url}. Status code: {response.status_code}")
        return None

def get_sql_driver():
    """Get the appropriate SQL Server driver"""
    drivers = [x for x in pyodbc.drivers()]
    print("Available ODBC Drivers:", drivers)
   
    preferred_drivers = [
        'ODBC Driver 18 for SQL Server',
        'ODBC Driver 17 for SQL Server',
        'SQL Server Native Client 11.0',
        'SQL Server Native Client 10.0',
        'SQL Server'
    ]
   
    for driver in preferred_drivers:
        if driver in drivers:
            print(f"Using driver: {driver}")
            return driver
           
    raise RuntimeError("No compatible SQL Server driver found. Please install SQL Server ODBC Driver.")

def get_date_filter():
    """Returns the date one year ago from today"""
    one_year_ago = datetime.now() - timedelta(days=365)
    return one_year_ago.strftime('%Y-%m-%d')


def create_medicloud_connection():
    """Create connection for Medicloud database using secrets"""
    driver = get_sql_driver()
    
    conn_str = (
        f'DRIVER={{{driver}}};'
        f'SERVER={st.secrets["credentials"]["server"]},{st.secrets["credentials"]["port"]};'
        f'DATABASE={st.secrets["credentials"]["database"]};'
        f'UID={st.secrets["credentials"]["username"]};'
        f'PWD={st.secrets["credentials"]["password"]};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=yes;'
        f'Connection Timeout=30;'
    )
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Debug: Connection string used: {conn_str.replace(st.secrets['credentials']['password'], '****')}")
        raise e

# Function to properly process the JSON data and extract the actual records
def process_json_data(json_data):
    if json_data is None:
        return pd.DataFrame()
    
    if isinstance(json_data, dict):
        for key in ['enrollee', 'data', 'results']:
            if key in json_data and isinstance(json_data[key], list):
                return pd.DataFrame(json_data[key])
        
        try:
            print(f"Available keys in the JSON response: {list(json_data.keys())}")
            df = pd.json_normalize(json_data)
            for column in df.columns:
                if isinstance(df[column].iloc[0], list):
                    nested_df = pd.json_normalize(df[column].iloc[0])
                    return nested_df
            return df
        except Exception as e:
            print(f"Error processing JSON data: {str(e)}")
            return pd.DataFrame({"raw_data": [json.dumps(json_data)]})
    
    elif isinstance(json_data, list):
        return pd.DataFrame(json_data)
    
    return pd.DataFrame()

# Fetch and process data
print("Fetching enrollee data...")
enrollee_data = fetch_data(enrollee_url)
print("Fetching company data...")
company_data = fetch_data(company_url)

print("Processing enrollee data...")
enrollee_df = process_json_data(enrollee_data)
print("Processing company data...")
company_df = process_json_data(company_data)

print(f"Enrollee data shape: {enrollee_df.shape}")
if not enrollee_df.empty:
    print(f"Enrollee columns: {enrollee_df.columns.tolist()}")

print(f"Company data shape: {company_df.shape}")
if not company_df.empty:
    print(f"Company columns: {company_df.columns.tolist()}")

# Save DataFrames to Parquet
print("Saving data to Parquet...")
if not enrollee_df.empty:
    output_path = os.path.join(OUTPUT_DIR, 'enrollees.parquet')
    enrollee_df.to_parquet(output_path, index=False)
    print(f"Saved {len(enrollee_df)} enrollee records to '{output_path}'")
else:
    print("No enrollee data to save")
    
if not company_df.empty:
    output_path = os.path.join(OUTPUT_DIR, 'companies.parquet')
    company_df.to_parquet(output_path, index=False)
    print(f"Saved {len(company_df)} company records to '{output_path}'")
else:
    print("No company data to save")

print("Data has been successfully saved as Parquet files.")    
    
def create_eacount_connection():
    """Create connection for EACOUNT database using secrets"""
    driver = get_sql_driver()
    conn_str = (
        f'DRIVER={{{driver}}};'
        f'SERVER={st.secrets["eaccount_credentials"]["server"]},{st.secrets["eaccount_credentials"]["port"]};'
        f'DATABASE={st.secrets["eaccount_credentials"]["database"]};'
        f'UID={st.secrets["eaccount_credentials"]["username"]};'
        f'PWD={st.secrets["eaccount_credentials"]["password"]};'
        f'Encrypt=yes;'
        f'TrustServerCertificate=yes;'
        f'Connection Timeout=30;'
    )
    try:
        print("Connection string used:EACCOUNT ", conn_str) 
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        print(f"Connection string used: {conn_str.replace(st.secrets['eaccount_credentials']['password'], '****')}")
        raise e
        
    except Exception as e:
        st.error(f"EACOUNT connection error: {str(e)}")
        st.error("""
        Troubleshooting steps:
        1. Verify username/password are correct
        2. Check if SQL Server allows mixed authentication
        3. Ensure TCP/IP is enabled in SQL Server Configuration
        4. Confirm firewall allows port 1433
        5. Try connecting with SQL Server Management Studio first
        """)
        raise    

def export_medicloud_data():
    """Export data from Medicloud database to Parquet"""
    conn = None
    try:
        # Create output directory
        output_dir = os.path.join('/Users/kenechukwuchukwuka/Downloads/streamlit/DATADUMP')
        print(output_dir)
        os.makedirs(output_dir, exist_ok=True)

        # Medicloud database queries
        medicloud_queries = {
            "group_contract": """
                SELECT 
                gc.groupid,
                gc.startdate,
                gc.enddate,
                g.groupname
                FROM dbo.group_contract gc
                JOIN dbo.[group] g ON gc.groupid = g.groupid
                WHERE gc.iscurrent = 1
                AND CAST(gc.enddate AS DATETIME) >= CAST(GETDATE() AS DATETIME);
            """,
            "benefit_procedure": """
                SELECT
                bcf.benefitcodeid,
                bcf.procedurecode,
                bc.benefitcodename,
                bc.benefitcodedesc
                FROM dbo.benefitcode_procedure bcf
                JOIN dbo.benefitcode bc ON bcf.benefitcodeid = bc.benefitcodeid
            """,
            "Total_PA_Procedures": """
                SELECT
                txn.panumber,
                txn.groupname,
                txn.divisionname,
                txn.plancode,
                txn.IID,
                txn.providerid,
                txn.requestdate,
                txn.pastatus,
                tbp.code,
                tbp.requested,
                tbp.granted
                FROM dbo.tbPATxn txn
                JOIN dbo.tbPAProcedures tbp ON txn.panumber = tbp.panumber
                WHERE txn.requestdate >= '2024-01-01' AND txn.requestdate <= GETDATE();
            """,
            "Claims": """
                SELECT nhislegacynumber, nhisproviderid, nhisgroupid, panumber, encounterdatefrom, datesubmitted, chargeamount, approvedamount, procedurecode, deniedamount 
                FROM dbo.claims
                WHERE datesubmitted >= '2024-01-01' AND datesubmitted <= GETDATE();
            """,
            "all_providers": """
                SELECT
                p.*,
                l.lganame,
                s.statename,
                pc.categoryname
            FROM
                dbo.provider p
                JOIN dbo.providercategory pc ON p.provcatid = pc.provcatid
            LEFT JOIN
                dbo.lgas l ON p.lgaid = l.lgaid
            LEFT JOIN
                dbo.states s ON p.stateid = s.stateid
            """,
            "group_coverage": """
                SELECT * FROM dbo.group_coverage
                WHERE iscurrent = 1
                AND CAST(terminationdate AS DATE) >= CAST(GETDATE() AS DATE)
            """,
            "all_active_member": """
                SELECT
                mc.memberid,
                m.groupid,
                m.legacycode,
                m.planid,
                mc.iscurrent,
                m.isterminated,
                mc.terminationdate
                FROM dbo.member_coverage mc
                JOIN dbo.member m ON mc.memberid = m.memberid
                WHERE m.isterminated = 0
                AND mc.iscurrent = 1
                AND CAST(mc.terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
                AND m.legacycode LIKE 'CL%';
            """,
            "all_group": """
                SELECT * FROM dbo.[group]
            """,
            "member_plans":"""
                SELECT * FROM dbo.member_plan
            """,
            "Planbenefitcode_limit":"""
                SELECT * FROM dbo.planbenefitcode_limit
                """,
            "benefitcode":"""
                SELECT * dbo.benefitcode
                """,
            "benefitcode_procedure":"""
                SELECT * FROM dbo.benefitcode_procedure
                """,
            "group_plan": """
                SELECT * FROM dbo.group_plan
                   WHERE iscurrent = 1
                AND CAST(terminationdate AS DATETIME) >= CAST(GETDATE() AS DATETIME)
            """,
            "pa_issue_request": """
                SELECT Providerid, RequestDate, ResolutionTime, EncounterDate, PANumber, DateAdded
                FROM dbo.PAIssueRequest
                WHERE YEAR(EncounterDate) = YEAR(GETDATE())
            """,
            "plans": """
                SELECT * FROM dbo.plans
                """,
            "group_invoice": """
                SELECT 
                    groupid, 
                    invoicenumber, 
                    countofindividual, 
                    countoffamily, 
                    individualprice, 
                    familyprice,
                    isapproved, 
                    invoicestartdate, 
                    invoiceenddate, 
                    invoicetype 
                FROM dbo.group_invoice
                WHERE YEAR(invoicestartdate) = YEAR(GETDATE())
                AND invoicetype = 'STANDARD'
            """,
            "e_account_group": """
                SELECT * FROM dbo.Company
            """    
        }

        print("Connecting to Medicloud database...")
        conn = create_medicloud_connection()
        print("Connected to Medicloud successfully!")

        for query_name, query in medicloud_queries.items():
            try:
                print(f"Executing Medicloud query for {query_name}...")
                cursor = conn.cursor()
                cursor.execute(query)
               
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                df = pd.DataFrame.from_records(results, columns=columns)
               
                filename = os.path.join(output_dir, f'MEDICLOUD_{query_name}.parquet')
                print(f"Exporting data to {filename}...")
                df.to_parquet(filename, index=False, engine="pyarrow")
                print(f"File created successfully: {filename}")
                print(f"Total records exported for {query_name}: {len(df)}")
               
            except Exception as e:
                print(f"Error processing {query_name}: {str(e)}")
            finally:
                if 'cursor' in locals():
                    cursor.close()

    except Exception as e:
        print(f"An error occurred with Medicloud connection: {str(e)}")
    finally:
        if conn:
            conn.close()
            print("Medicloud database connection closed.")


def export_eacount_data():
    """Export data from EACOUNT database"""
    conn = None
    try:
        # Create output directory
        output_dir = os.path.join('/Users/kenechukwuchukwuka/Downloads/streamlit/DATADUMP')
        os.makedirs(output_dir, exist_ok=True)

        print("Connecting to EACOUNT database...")
        conn = create_eacount_connection()
        print("Connected to EACOUNT successfully!")

        # EACOUNT queries
        eacount_queries = {
            "DEBIT_Note": """
                SELECT *
                FROM dbo.DEBIT_Note
                WHERE [From] >= '2023-01-01' AND [From] <= GETDATE();
            """,
            "FIN_GL": """
                SELECT *
                FROM dbo.FIN_GL;
            """,
            "Premium1_schedule": """
                SELECT *
                FROM dbo.Premium1_schedule;
            """,
             "FIN_AccSetup": """
                SELECT *
                FROM dbo.FIN_AccSetup;
            """
        }

        for query_name, query in eacount_queries.items():
            try:
                print(f"Executing EACOUNT query for {query_name}...")
                cursor = conn.cursor()
                cursor.execute(query)
               
                columns = [column[0] for column in cursor.description]
                results = cursor.fetchall()
                df = pd.DataFrame.from_records(results, columns=columns)
               
                filename = os.path.join(output_dir, f'MEDICLOUD_{query_name}.parquet')
                print(f"Exporting {query_name} data to {filename}...")
                df.to_parquet(filename, index=False, engine="pyarrow")
                print(f"File created successfully: {filename}")
                print(f"Total records exported for {query_name}: {len(df)}")
               
            except Exception as e:
                print(f"Error processing {query_name}: {str(e)}")
            finally:
                if 'cursor' in locals():
                    cursor.close()

    except Exception as e:
        print(f"An error occurred with EACOUNT connection: {str(e)}")
    finally:
        if conn:
            conn.close()
            print("EACOUNT database connection closed.")            

def main():
    """Main function to run both export processes"""
    try:
        # Export Medicloud data
        print("Starting Medicloud export process...")
        export_medicloud_data()
       
        # Export EACOUNT data
        print("\nStarting EACOUNT export process...")
        export_eacount_data()
       
        print("\nAll export processes completed successfully!")
       
    except Exception as e:
        print(f"An error occurred in main process: {str(e)}")

if __name__ == "__main__":
    main()