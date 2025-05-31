import pandas as pd
import polars as pl
import os

# Create DATADUMP directory if it doesn't exist
datadump_dir = "DATADUMP"
if not os.path.exists(datadump_dir):
    os.makedirs(datadump_dir)
    print(f"Created directory: {datadump_dir}")

# Define file paths
g_plan_path = "DATADUMP/MEDICLOUD_group_plan.parquet"
group_contract_path = "DATADUMP/MEDICLOUD_group_contract.parquet"
groups_path = "DATADUMP/MEDICLOUD_all_group.parquet"
plans_path = "DATADUMP/MEDICLOUD_plans.parquet"
gl2024_path = "DATADUMP/GL2024.xlsx"
gl2025_path = "DATADUMP/MEDICLOUD_FIN_GL.parquet"
GLSETUP = pd.read_parquet("DATADUMP/MEDICLOUD_FIN_AccSetup.parquet")
E_ACCT_GROUP = pd.read_parquet("DATADUMP/MEDICLOUD_e_account_group.parquet")

# Check if files exist
for file_path in [g_plan_path, group_contract_path, groups_path, plans_path, gl2024_path, gl2025_path]:
    if not os.path.exists(file_path):
        print(f"Warning: File not found: {file_path}")

# Read parquet file using polars
G_PLAN = pl.read_parquet(g_plan_path)
GROUP_CONTRACT = pl.read_parquet(group_contract_path)
GROUPS = pl.read_parquet(groups_path)
PLANS = pl.read_parquet(plans_path)

# Read GL files using pandas
GL2024 = pd.read_excel(gl2024_path, dtype={'RefNo': str})
GL2025 = pd.read_parquet(gl2025_path)

# Concatenate GL2024 and GL2025 vertically using pandas
GL_combined = pd.concat([GL2024, GL2025], axis=0)

# Select only the specified columns from GL_combined
GL_combined = GL_combined[['acctype', 'AccCode', 'GLDesc', 'GLDate', 'GLAmount', 'code']]

# Convert amount column to numeric type
GL_combined['GLAmount'] = pd.to_numeric(GL_combined['GLAmount'])

# Convert AccCode to string in both DataFrames and remove .0 from GLSETUP
GL_combined['AccCode'] = GL_combined['AccCode'].astype(str)
GLSETUP['AccCode'] = GLSETUP['AccCode'].astype(str).str.replace(r'\.0$', '', regex=True)

# Remove .0 from AccCode if present
GL_combined['AccCode'] = GL_combined['AccCode'].str.replace(r'\.0$', '', regex=True)


# Perform a left join to add AccDesc from GLSETUP to GL using AccCode as the key
GLL = pd.merge(
    GL_combined,
    GLSETUP[['AccCode', 'AccDesc']],
    on='AccCode',
    how='left'
).rename(columns={'AccDesc': 'DESCRIPTION'})

# Convert GLDate to datetime format
GLL['GLDate'] = pd.to_datetime(GLL['GLDate'])

# Extract month and year from GLDate
GLL['Month'] = GLL['GLDate'].dt.month
GLL['Year'] = GLL['GLDate'].dt.year

# Convert ID_Company and code columns to string type for joining
E_ACCT_GROUP['ID_Company'] = E_ACCT_GROUP['ID_Company'].astype(str)

GLL['code'] = GLL['code'].astype(str)

# Join with E_ACCT_GROUP
GLT = pd.merge(
    GLL,
    E_ACCT_GROUP[['ID_Company', 'CompanyName']],
    left_on='code',
    right_on='ID_Company',
    how='left'
)

    #NEGATE AMOUNT
# First ensure GLAmount is numeric
GLT['GLAmount'] = pd.to_numeric(GLT['GLAmount'], errors='coerce')

# Convert to absolute values 
GLT['GLAmount'] = GLT['GLAmount'].abs()

Cash_keywords = ["POLARIS BANK 1", "KEYSTONE BANK", "MICROCRED MFB", "ASTRAPOLARIS MFB", "ALERT MICRO BANK", "MONEYFIELD MFB", "KAYVEE  MFB"]

# Calculate cash received (CORPORATE PREMIUM RECEIVABLE)
cash_received = GLT[GLT['DESCRIPTION'].isin(Cash_keywords)].groupby('Month')['GLAmount'].sum().reset_index(name='Cash_Received')
print(cash_received)

