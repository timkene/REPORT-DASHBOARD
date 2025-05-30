import streamlit as st
import pandas as pd
import polars as pl
import datetime
import os
from datetime import datetime
from utils.data_loader import load_excel_data, benefit_limit

# --- CONFIG ---
EXPORTS_DIR = '/Users/kenechukwuchukwuka/Downloads/streamlit/exports'
EXPORTS_MAIN_PATH = os.path.join(EXPORTS_DIR, "main.xlsx")
EXPORTS_NEW_PATH = os.path.join(EXPORTS_DIR, "new.xlsx")
EMAIL_MAP_PATH = '/Users/kenechukwuchukwuka/Downloads/streamlit/id_email_mapping.xlsx'

# Create exports directory if it doesn't exist
os.makedirs(EXPORTS_DIR, exist_ok=True)

# --- LOAD DATA ---
def load_main_file():
    """Load the main tracking file or create if not exists"""
    if os.path.exists(EXPORTS_MAIN_PATH):
        return pd.read_excel(EXPORTS_MAIN_PATH)
    else:
        return pd.DataFrame(columns=["ID", "Benefit", "Total Used", "Limit Amount", 
                                   "Plan ID", "Group Name", "Usage Percentage", 
                                   "First Detected"])

def save_to_excel(df, filepath):
    """Save dataframe to excel with proper formatting"""
    df['First Detected'] = pd.to_datetime(df['First Detected']).dt.strftime('%Y-%m-%d')
    df.to_excel(filepath, index=False)
    
def update_tracking_files(threshold_df):
    """Update main and new files with threshold data"""
    # Load existing main file
    main_df = load_main_file()
    
    # Add current timestamp to new records
    threshold_df['First Detected'] = datetime.now().strftime('%Y-%m-%d')
    
    # Create composite key for comparison
    threshold_df['composite_key'] = threshold_df['ID'] + '_' + threshold_df['Benefit']
    if not main_df.empty:
        main_df['composite_key'] = main_df['ID'] + '_' + main_df['Benefit']
    
    # Identify new records (not in main)
    new_records = threshold_df[~threshold_df['composite_key'].isin(main_df['composite_key'])] if not main_df.empty else threshold_df
    
    # Drop composite key before saving
    if not main_df.empty:
        main_df = main_df.drop('composite_key', axis=1)
    threshold_df = threshold_df.drop('composite_key', axis=1)
    new_records = new_records.drop('composite_key', axis=1)
    
    # Update main file with all current records
    combined_df = pd.concat([main_df, new_records], ignore_index=True)
    save_to_excel(combined_df, EXPORTS_MAIN_PATH)
    
    # Save new records to new.xlsx
    if not new_records.empty:
        save_to_excel(new_records, EXPORTS_NEW_PATH)
        
    return combined_df, new_records

# --- BENEFIT EXCEEDANCE LOGIC ---
def get_all_exceeded_combined(BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT):
    if isinstance(GROUP_CONTRACT, pl.LazyFrame):
        all_groups = GROUP_CONTRACT.select('groupname').unique().collect().get_column('groupname').to_list()
    else:
        all_groups = GROUP_CONTRACT.select('groupname').unique().get_column('groupname').to_list()
    
    all_exceeded = []
    
    if isinstance(BENEFIT, pl.LazyFrame):
        BENEFIT = BENEFIT.collect()
    
    for group_name in all_groups:
        valid_contracts = GROUP_CONTRACT.filter(pl.col("groupname") == group_name)
        if isinstance(valid_contracts, pl.LazyFrame):
            valid_contracts = valid_contracts.collect()
            
        if valid_contracts.height == 0:
            continue
            
        contract_dates = valid_contracts.select([
            pl.min("startdate").alias("min_startdate"),
            pl.max("enddate").alias("max_enddate")
        ])
        if isinstance(contract_dates, pl.LazyFrame):
            contract_dates = contract_dates.collect()
        
        min_date = contract_dates[0, "min_startdate"]
        max_date = contract_dates[0, "max_enddate"]

        filtered_pa = BENEFIT_PA.filter(
            (pl.col("groupname") == group_name) & 
            (pl.col("requestdate") >= min_date) & 
            (pl.col("requestdate") <= max_date)
        )
        if isinstance(filtered_pa, pl.LazyFrame):
            filtered_pa = filtered_pa.collect()
        
        filtered_claims = claims_BENEFIT.filter(
            (pl.col("groupname") == group_name) & 
            (pl.col("requestdate") >= min_date) & 
            (pl.col("requestdate") <= max_date)
        )
        if isinstance(filtered_claims, pl.LazyFrame):
            filtered_claims = filtered_claims.collect()

        pa_with_benefits = filtered_pa.join(
            BENEFIT,
            left_on="code",
            right_on="Code",
            how="left"
        )
        if isinstance(pa_with_benefits, pl.LazyFrame):
            pa_with_benefits = pa_with_benefits.collect()

        claims_with_benefits = filtered_claims.join(
            BENEFIT,
            left_on="code",
            right_on="Code",
            how="left"
        )
        if isinstance(claims_with_benefits, pl.LazyFrame):
            claims_with_benefits = claims_with_benefits.collect()

        common_procedures = pa_with_benefits.join(
            claims_with_benefits.select(["IID", "code", "granted", "planid", "Benefit", "benefitcodeid"]),
            on=["IID", "code"],
            how="inner"
        )
        if isinstance(common_procedures, pl.LazyFrame):
            common_procedures = common_procedures.collect()

        common_procedures = common_procedures.group_by(["IID", "planid_right", "Benefit_right", "benefitcodeid_right"]).agg(
            pl.sum("granted_right").alias("total_granted")
        ).rename({
            "planid_right": "planid",
            "Benefit_right": "Benefit",
            "benefitcodeid_right": "benefitcodeid"
        })

        claims_only = claims_with_benefits.join(
            pa_with_benefits.select(["IID", "code"]),
            on=["IID", "code"],
            how="anti"
        )
        if isinstance(claims_only, pl.LazyFrame):
            claims_only = claims_only.collect()

        claims_only = claims_only.group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
            pl.sum("granted").alias("total_granted")
        )

        pa_only = pa_with_benefits.join(
            pa_with_benefits.select(["IID", "code"]),
            on=["IID", "code"],
            how="anti"
        )
        if isinstance(pa_only, pl.LazyFrame):
            pa_only = pa_only.collect()

        pa_only = pa_only.group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
            pl.sum("granted").alias("total_granted")
        )

        benefit_usage = pl.concat([
            common_procedures,
            claims_only,
            pa_only
        ])
        if isinstance(benefit_usage, pl.LazyFrame):
            benefit_usage = benefit_usage.collect()

        benefit_usage = benefit_usage.group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
            pl.sum("total_granted").alias("total_granted")
        )

        plan_limits = PLAN_BENEFIT_LIMIT.with_columns(
            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
        )
        if isinstance(plan_limits, pl.LazyFrame):
            plan_limits = plan_limits.collect()

        benefit_with_limits = benefit_usage.join(
            plan_limits,
            left_on=["planid", "benefitcodeid"],
            right_on=["planid", "benefitcodeid"],
            how="left"
        )
        if isinstance(benefit_with_limits, pl.LazyFrame):
            benefit_with_limits = benefit_with_limits.collect()

        benefit_with_limits = benefit_with_limits.with_columns(
            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
        ).filter(
            pl.col("maxlimit") > 0
        )

        exceeded_limits = benefit_with_limits.filter(
            pl.col("total_granted") >= pl.col("maxlimit")
        ).with_columns(
            (pl.col("total_granted") - pl.col("maxlimit")).alias("exceeded_by")
        )

        if exceeded_limits.height > 0:
            exceeded_limits = exceeded_limits.with_columns([
                pl.lit(group_name).alias("groupname")
            ])
            all_exceeded.append(exceeded_limits)

    if all_exceeded:
        return pl.concat(all_exceeded)
    else:
        return pl.DataFrame()

# --- STREAMLIT PAGE ---
st.title("Benefit Usage Tracker (Combined PA + CLAIMS)")

@st.cache_data(ttl=3600)
def load_all_data():
    data = load_excel_data()
    BENEFIT_PA, claims_BENEFIT = benefit_limit()
    BENEFIT = data['BENEFITCBA'].rename({
        "procedurecode": "Code",
        "benefitcodedesc": "Benefit"
    })
    PLAN_BENEFIT_LIMIT = data['PLAN_BENEFIT_LIMIT']
    GROUP_CONTRACT = data['GROUP_CONTRACT']
    return BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT

BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT = load_all_data()

@st.cache_data
def get_benefit_usage():
    pl_df = get_all_exceeded_combined(BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT)
    if pl_df.height == 0:
        return pd.DataFrame()
    df = pl_df.select([
        pl.col("IID").alias("ID"),
        pl.col("Benefit"),
        pl.col("total_granted").alias("Total Used"),
        pl.col("maxlimit").alias("Limit Amount"),
        pl.col("planid").alias("Plan ID"),
        pl.col("groupname").alias("Group Name"),
        pl.col("benefitcodeid").alias("Benefit Code ID"),
    ]).to_pandas()
    
    df['Usage Percentage'] = (df['Total Used'] / df['Limit Amount'] * 100).round(2)
    return df

def calculate_benefit_distribution(BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT):
    if isinstance(GROUP_CONTRACT, pl.LazyFrame):
        all_groups = GROUP_CONTRACT.select('groupname').unique().collect().get_column('groupname').to_list()
    else:
        all_groups = GROUP_CONTRACT.select('groupname').unique().get_column('groupname').to_list()
    
    all_usage = []
    
    if isinstance(BENEFIT, pl.LazyFrame):
        BENEFIT = BENEFIT.collect()
    
    for group_name in all_groups:
        valid_contracts = GROUP_CONTRACT.filter(pl.col("groupname") == group_name)
        if isinstance(valid_contracts, pl.LazyFrame):
            valid_contracts = valid_contracts.collect()
            
        if valid_contracts.height == 0:
            continue
            
        contract_dates = valid_contracts.select([
            pl.min("startdate").alias("min_startdate"),
            pl.max("enddate").alias("max_enddate")
        ])
        if isinstance(contract_dates, pl.LazyFrame):
            contract_dates = contract_dates.collect()
        
        min_date = contract_dates[0, "min_startdate"]
        max_date = contract_dates[0, "max_enddate"]

        filtered_pa = BENEFIT_PA.filter(
            (pl.col("groupname") == group_name) & 
            (pl.col("requestdate") >= min_date) & 
            (pl.col("requestdate") <= max_date)
        )
        if isinstance(filtered_pa, pl.LazyFrame):
            filtered_pa = filtered_pa.collect()
        
        filtered_claims = claims_BENEFIT.filter(
            (pl.col("groupname") == group_name) & 
            (pl.col("requestdate") >= min_date) & 
            (pl.col("requestdate") <= max_date)
        )
        if isinstance(filtered_claims, pl.LazyFrame):
            filtered_claims = filtered_claims.collect()

        pa_with_benefits = filtered_pa.join(
            BENEFIT,
            left_on="code",
            right_on="Code",
            how="left"
        )
        if isinstance(pa_with_benefits, pl.LazyFrame):
            pa_with_benefits = pa_with_benefits.collect()

        claims_with_benefits = filtered_claims.join(
            BENEFIT,
            left_on="code",
            right_on="Code",
            how="left"
        )
        if isinstance(claims_with_benefits, pl.LazyFrame):
            claims_with_benefits = claims_with_benefits.collect()

        usage = pl.concat([
            pa_with_benefits.select(["IID", "Benefit", "granted", "planid", "benefitcodeid"]),
            claims_with_benefits.select(["IID", "Benefit", "granted", "planid", "benefitcodeid"])
        ])
        if isinstance(usage, pl.LazyFrame):
            usage = usage.collect()

        usage = usage.group_by(["IID", "Benefit", "planid", "benefitcodeid"]).agg(
            pl.sum("granted").alias("total_granted")
        )

        plan_limits = PLAN_BENEFIT_LIMIT.with_columns(
            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
        )
        if isinstance(plan_limits, pl.LazyFrame):
            plan_limits = plan_limits.collect()

        usage_with_limits = usage.join(
            plan_limits,
            left_on=["planid", "benefitcodeid"],
            right_on=["planid", "benefitcodeid"],
            how="left"
        )
        if isinstance(usage_with_limits, pl.LazyFrame):
            usage_with_limits = usage_with_limits.collect()

        usage_with_limits = usage_with_limits.with_columns([
            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0),
            pl.lit(group_name).alias("groupname")
        ]).filter(
            pl.col("maxlimit") > 0
        )

        if usage_with_limits.height > 0:
            all_usage.append(usage_with_limits)

    if not all_usage:
        return pd.DataFrame()

    combined_usage = pl.concat(all_usage)
    if isinstance(combined_usage, pl.LazyFrame):
        combined_usage = combined_usage.collect()
    
    combined_usage = combined_usage.with_columns(
        (pl.col("total_granted") / pl.col("maxlimit") * 100).alias("usage_percentage")
    )

    distribution = combined_usage.group_by("Benefit").agg([
        pl.col("usage_percentage").filter(pl.col("usage_percentage") < 50).count().alias("below_50"),
        pl.col("usage_percentage").filter((pl.col("usage_percentage") >= 50) & (pl.col("usage_percentage") < 70)).count().alias("between_50_70"),
        pl.col("usage_percentage").filter(pl.col("usage_percentage") >= 70).count().alias("above_70")
    ]).sort("Benefit")

    if isinstance(distribution, pl.LazyFrame):
        distribution = distribution.collect()

    return distribution.to_pandas()

# Load all necessary data
usage_df = get_benefit_usage()
email_map = pd.read_excel(EMAIL_MAP_PATH) if os.path.exists(EMAIL_MAP_PATH) else pd.DataFrame()

# Calculate and display benefit distribution
st.subheader("Benefit Usage Distribution")
distribution_df = calculate_benefit_distribution(BENEFIT_PA, claims_BENEFIT, BENEFIT, PLAN_BENEFIT_LIMIT, GROUP_CONTRACT)
if not distribution_df.empty:
    st.dataframe(
        distribution_df,
        column_config={
            "Benefit": "Benefit Type",
            "below_50": st.column_config.NumberColumn("Below 50%"),
            "between_50_70": st.column_config.NumberColumn("50% - 70%"),
            "above_70": st.column_config.NumberColumn("Above 70%")
        },
        use_container_width=True
    )
else:
    st.info("No benefit usage data available.")

# Filter for 70% threshold
threshold_df = usage_df[usage_df['Usage Percentage'] >= 70].copy()

if not threshold_df.empty:
    # Update tracking files
    main_df, new_records = update_tracking_files(threshold_df)
    
    # Display results
    st.subheader("Current Processing Results")
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Total Records ≥ 70%", len(threshold_df))
        st.metric("New Records", len(new_records))
    
    with col2:
        st.metric("Total Records in Main", len(main_df))
        if os.path.exists(EXPORTS_MAIN_PATH):
            st.download_button(
                "Download Main File",
                open(EXPORTS_MAIN_PATH, 'rb').read(),
                "main.xlsx",
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    
    # Display new records if any
    if not new_records.empty:
        st.subheader("New Records Detected")
        st.dataframe(new_records)
        st.download_button(
            "Download New Records",
            open(EXPORTS_NEW_PATH, 'rb').read(),
            "new.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("No new records detected in this run.")
    
    # Display main tracking file
    with st.expander("View All Tracked Records"):
        st.dataframe(main_df)
    
    # Breakdown view
    st.subheader("Record Breakdown")
    enrollee_ids = threshold_df["ID"].unique()
    selected_id = st.selectbox("Select ID for breakdown", enrollee_ids)
    if selected_id:
        st.write(f"Breakdown for {selected_id}:")
        id_benefits = threshold_df[threshold_df["ID"] == selected_id]
        st.dataframe(id_benefits)
else:
    st.info("No records found exceeding 70% threshold in this run.")

st.info("""
This system tracks benefit usage and maintains two files:
- main.xlsx: Contains all historical records of benefits usage ≥ 70%
- new.xlsx: Contains only new records found in the current run
Records are compared using both ID and Benefit to identify new entries.
""")


