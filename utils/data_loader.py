import polars as pl
import os
from datetime import datetime, timedelta
import streamlit as st
import traceback
import re
import numpy as np
import pandas as pd
import gc
import psutil

def load_data_in_chunks(file_path, chunk_size=100000):
    """Load data in chunks to reduce memory usage"""
    if file_path.endswith('.parquet'):
        return pl.scan_parquet(file_path)
    elif file_path.endswith('.csv'):
        return pl.scan_csv(file_path)
    elif file_path.endswith('.xlsx'):
        return pl.read_excel(file_path)
    return None

@st.cache_data(ttl=3600, max_entries=10)
def load_excel_data():
    """Load all Excel files from DATADUMP folder with memory optimization"""
    try:
        # Define the base path to your DATADUMP folder
        possible_paths = [
            'DATADUMP',
            os.path.join('DATADUMP'),
            'DATADUMP'
        ]
        
        # Find the first valid path
        base_path = None
        for path in possible_paths:
            if os.path.exists(path):
                base_path = path
                break
        
        if base_path is None:
            st.error(f"Could not find DATADUMP directory. Searched in: {', '.join(possible_paths)}")
            return {}
        
        # Update required_files dictionary to include all files
        required_files = {
            'PA': "MEDICLOUD_Total_PA_Procedures.parquet",
            'GROUP_CONTRACT': "MEDICLOUD_group_contract.parquet", 
            'M_PLAN': "MEDICLOUD_member_plans.parquet",
            'PROVIDER': "MEDICLOUD_all_providers.parquet",
            'ACTIVE_ENROLLEE': "MEDICLOUD_all_active_member.parquet",
            'GROUPS': "MEDICLOUD_all_group.parquet",
            'PLAN_NAMES': "PLAN NAME.csv",
            'CLAIMS': "MEDICLOUD_Claims.parquet",
            'GROUP_COVERAGE': "MEDICLOUD_group_coverage.parquet",
            'PREMIUM': "MLR CIL (1).xlsx",
            'pattern': "pattern.xlsx",
            'BENEFIT': "NEW_BENEFIT_Sheet1.parquet",
            'DEBIT': "EACOUNT_DEBIT_Note.parquet",
            'EPREMIUM': "EACOUNT_Premium1_schedule.parquet",
            'GL': "EACOUNT_FIN_GL.parquet",
            'GLSETUP': "EACOUNT_FIN_AccSetup.parquet",
            'G_PLAN': "MEDICLOUD_group_plan.parquet",
            'E_ACCT_GROUP': "MEDICLOUD_e_account_group.parquet",
            'ONLINE_PA': "MEDICLOUD_pa_issue_request.parquet",
            'HR_LIST': "enrollees.parquet",
            'PAA' : "MEDICLOUD_Total_PA_Procedures_2022.parquet",
            'PAAA' : "MEDICLOUD_Total_PA_Procedures_2023.parquet",
            'CCLAIMS' : "MEDICLOUD_Claims_2022.parquet",
            'CCCLAIMS' : "MEDICLOUD_Claims_2023.parquet",
            'BENEFITCBA' : "MEDICLOUD_benefit_procedure.parquet",
            'PLAN_BENEFIT_LIMIT' : "MEDICLOUD_planbenefitcode_limit.parquet",
            'PLANS': "MEDICLOUD_plans.parquet"   
        }
        
        # Verify all required files exist
        missing_files = []
        for key, filename in required_files.items():
            file_path = os.path.join(base_path, filename)
            if not os.path.exists(file_path):
                missing_files.append(filename)
        
        if missing_files:
            st.error(f"Missing required files: {', '.join(missing_files)}")
            return {}
        
        # Load all files into a dictionary using lazy loading where possible
        data = {}
        for key, filename in required_files.items():
            file_path = os.path.join(base_path, filename)
            try:
                if filename.endswith('.csv'):
                    # Use scan_csv for lazy loading of CSV files
                    data[key] = pl.scan_csv(file_path)
                elif filename.endswith('.xlsx'):
                    # Load Excel files directly into Polars DataFrame
                    if key == 'PREMIUM':
                        data['mlrr_data'] = pl.read_excel(file_path, sheet_name='DATA')
                        data['revenue_data'] = pl.read_excel(file_path, sheet_name='REVENUE')
                    else:
                        data[key] = pl.read_excel(file_path)
                else:
                    # Use scan_parquet for lazy loading of parquet files
                    data[key] = pl.scan_parquet(file_path)
                
                # Force garbage collection after each file load
                gc.collect()
                
            except Exception as e:
                st.error(f"Error loading {filename}: {str(e)}")
                return {}
        
        return data
        
    except Exception as e:
        st.error(f"Error loading Excel files: {str(e)}")
        return {}

def validate_dataframe(df, required_columns, name):
    """Validate DataFrame has required columns and non-zero rows"""
    if df.height == 0:
        raise ValueError(f"{name} DataFrame is empty")
    
    missing_cols = [col for col in required_columns if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in {name}: {missing_cols}")
    
    return True

def enrollee_comparison():
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
        
        # Check if required dataframes exist
        if 'ACTIVE_ENROLLEE' not in st.session_state.data:
            st.error("ACTIVE_ENROLLEE not found")
            return pl.DataFrame()
        if 'HR_LIST' not in st.session_state.data:
            st.error("HR_LIST not found")
            return pl.DataFrame()
        if 'GROUPS' not in st.session_state.data:
            st.error("GROUPS not found")
            return pl.DataFrame()
        
        # Get dataframes and clean legacycode column using lazy evaluation
        cba_list = st.session_state.data['ACTIVE_ENROLLEE'].lazy().with_columns(
            pl.col("legacycode").str.replace_all(r"[/ \-~]", "").alias("legacycode_clean")
        )
        hr_list = st.session_state.data['HR_LIST'].lazy()
        groups = st.session_state.data['GROUPS'].lazy()

        # Table 1: Total number of enrollee IDs under each groupid for both dataframes
        cba_counts = cba_list.group_by("groupid").agg(pl.count("legacycode_clean").alias("cba_count"))
        hr_counts = hr_list.group_by("companyId").agg(pl.count("normalizedHmoId").alias("hr_count"))
        
        tablee1 = cba_counts.join(hr_counts, left_on="groupid", right_on="companyId", how="outer").fill_null(0)
        table10 = tablee1.join(
                groups,
                left_on="groupid",
                right_on="groupid",
                how="left"
            )
        table1 = table10.select(['groupname', 'cba_count', 'hr_count']).collect()
        
        # Table 2: GroupIDs where unique count of enrollee IDs differs between dataframes
        cba_unique_counts = cba_list.group_by("groupid").agg(pl.col("legacycode_clean").n_unique().alias("cba_unique_count"))
        hr_unique_counts = hr_list.group_by("companyId").agg(pl.col("normalizedHmoId").n_unique().alias("hr_unique_count"))
        
        counts_combined = cba_unique_counts.join(hr_unique_counts, left_on="groupid", right_on="companyId", how="outer").fill_null(0)
        table2 = counts_combined.filter(pl.col("cba_unique_count") != pl.col("hr_unique_count")).collect()
        
        # Table 3: Detailed comparison per groupid
        # Get all unique groupids from both dataframes (handle different column names)
        cba_groupids = cba_list.select(pl.col("groupid").alias("groupid"))
        hr_groupids = hr_list.select(pl.col("companyId").alias("groupid"))
        
        all_groupids = pl.concat([cba_groupids, hr_groupids]).unique().collect()
        
        results = []
        
        for groupid in all_groupids.to_series():
            # Filter data for current groupid
            cba_group = cba_list.filter(pl.col("groupid") == groupid).collect()
            hr_group = hr_list.filter(pl.col("companyId") == groupid).collect()
            
            # Get enrollee IDs for current group
            cba_ids = cba_group.select("legacycode_clean").to_series()
            hr_ids = hr_group.select("normalizedHmoId").to_series()
            
            # Calculate metrics
            matching_count = len(set(cba_ids) & set(hr_ids))
            cba_not_in_hr = len(set(cba_ids) - set(hr_ids))
            hr_not_in_cba = len(set(hr_ids) - set(cba_ids))
            
            results.append({
                "groupid": groupid,
                "matching_count": matching_count,
                "cba_not_in_hr": cba_not_in_hr,
                "hr_not_in_cba": hr_not_in_cba
            })
        
        table3 = pl.DataFrame(results)
        
        return table1, table2, table3
    except Exception as e:
        st.error(f"Error processing enrollee_comp: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    

def process_debit_notes():
    """Process and analyze DEBIT notes data"""
    try:
        # Force reload of data from session state
        if 'data' not in st.session_state:
            st.error("Data not loaded")
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
            
        # Get fresh copies of the data and collect them
        DEBIT = st.session_state.data['DEBIT'].collect()
        CONTRACT = st.session_state.data['GROUP_CONTRACT'].collect()
        G_PLAN = st.session_state.data['G_PLAN'].collect()
        
        #GROUP_PLAN ANALYSIS
        # Calculate premium based on family and individual counts and prices
        G_PLAN = G_PLAN.with_columns(
            (pl.col('countoffamily') * pl.col('familyprice') + 
             pl.col('countofindividual') * pl.col('individualprice')).alias('PREMIUM')
        )
        # Group G_PLAN by groupid and sum PREMIUM
        G_PLAN_grouped = G_PLAN.group_by('groupid').agg(
            pl.col('PREMIUM').sum().alias('Total_Premium')
        ).sort('Total_Premium', descending=True)

        # Join G_PLAN_grouped with CONTRACT to get groupname
        G_PLAN_grouped = G_PLAN_grouped.join(
            CONTRACT.select(['groupid', 'groupname']),
            on='groupid',
            how='left'
        )

        # Filter DEBIT for dates within the last 12 months
        today = datetime.now().date()
        one_year_ago = today - timedelta(days=365)
        DEBIT = DEBIT.filter(
            pl.col('From').cast(pl.Date).is_not_null() & 
            (pl.col('From').cast(pl.Date) >= pl.lit(one_year_ago)) &
            (pl.col('From').cast(pl.Date) <= pl.lit(today))
        )

        # Get unique clean group names
        clean_groups = CONTRACT.select(
        pl.col('groupname').unique()
        ).get_column('groupname').to_list()
        
        # Split DEBIT into valid and invalid groups
        valid_debits = DEBIT.filter(
            pl.col('CompanyName').is_in(clean_groups)
        )

        invalid_debits = DEBIT.filter(
            ~pl.col('CompanyName').is_in(clean_groups)
        )

         # Select only required columns for valid debits
        invalid_debit = invalid_debits.select(
            pl.col('CompanyName'),
            pl.col('Amount')
        )
        
        # INVALID CONTRACT - Find groups in clean_groups that don't exist in DEBIT's CompanyName
        invalid_contract = CONTRACT.filter(
        ~pl.col('groupname').is_in(DEBIT.get_column('CompanyName'))
        )

        # Group by CompanyName and sum Amount
        DEBIT_grouped = DEBIT.group_by('CompanyName').agg(
        pl.col('Amount').sum().alias('Total_Amount')
        ).sort('Total_Amount', descending=True)

        # Join G_PLAN_grouped with DEBIT_grouped
        # First rename CompanyName to groupname in DEBIT_grouped for the join
        DEBIT_grouped = DEBIT_grouped.with_columns(
            pl.col('CompanyName').alias('groupname')
        ).drop('CompanyName')

        # Perform left join to keep all groups from G_PLAN_grouped
        combined_data = G_PLAN_grouped.join(
            DEBIT_grouped,
            on='groupname',
            how='left'
        )

        # Fill null values in Total_Amount with 0
        combined_data = combined_data.with_columns(
            pl.col('Total_Amount').fill_null(0)
        )

        return valid_debits, combined_data, invalid_debit, invalid_contract
        
    except Exception as e:
        st.error(f"Error processing DEBIT notes: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
def benefit(selected_group=None, excluded_group='1799', start_date=None, end_date=None):
    """calculate benefit spread using PA and Benefit Table with group selection and time filter"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame()
            
        # Check if required dataframes exist
        if 'CLAIMS' not in st.session_state.data:
            st.error("CLAIMS data not found")
            return pl.DataFrame()
        if 'GROUPS' not in st.session_state.data:
            st.error("GROUPS not found")
            return pl.DataFrame()
        if 'PA' not in st.session_state.data:
            st.error("PAnot found")
            return pl.DataFrame()
        if 'BENEFITCBA' not in st.session_state.data:
            st.error("BENEFITCBA not found")
            return pl.DataFrame() 
        if 'PROVIDER' not in st.session_state.data:
            st.error("PROVIDER not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        
        # Get required dataframes
        CLAIMS = st.session_state.data['CLAIMS']
        GROUPS = st.session_state.data.get('GROUPS', pl.DataFrame())
        PROVIDER = st.session_state.data['PROVIDER']
        BENEFITCBA = st.session_state.data['BENEFITCBA']

        BENEFIT = BENEFITCBA.rename({
        "procedurecode": "Code",
        "benefitcodedesc": "Benefit"
        })

        CLAIMS = CLAIMS.select([
            # Include all columns except the ones we're transforming
            pl.col("*").exclude(["encounterdatefrom", "approvedamount", "nhisgroupid"]),
            
            # Add our transformed columns
            pl.col('encounterdatefrom').cast(pl.Datetime).alias('date'),
            pl.col('approvedamount').alias('cost'),
            
            # Clean nhisgroupid with explicit alias
            pl.col('nhisgroupid')
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Int64, strict=False)
            .alias('nhisgroupid')  # Explicitly keep original name
        ])

        # Ensure groupid is also Int64
        GROUPS = GROUPS.with_columns(pl.col("groupid").cast(pl.Int64))

        # Merge CLAIMS with GROUPS to get group names
        CLAIMS_with_group_names = CLAIMS.join(
            GROUPS,
            left_on='nhisgroupid',
            right_on='groupid',
            how='left'
        )

        # Filter CLAIMS by selected group and exclude a specific group
        if selected_group:
            CLAIMS_with_group_names = CLAIMS_with_group_names.filter(pl.col('groupname') == selected_group)
        if excluded_group:
            CLAIMS_with_group_names = CLAIMS_with_group_names.filter(pl.col('groupname') != excluded_group)

        # Apply time filter if dates are provided
        if start_date:
            CLAIMS_with_group_names = CLAIMS_with_group_names.filter(pl.col('date') >= start_date)
        if end_date:
            CLAIMS_with_group_names = CLAIMS_with_group_names.filter(pl.col('date') <= end_date)

        # Merge with BENEFIT DataFrame using CLAIMS[procedurecode] as the key
        claims_with_benefitt = CLAIMS_with_group_names.join(
            BENEFIT,
            left_on='procedurecode',  # Assuming 'code' is the column in CLAIMS
            right_on='Code',  # Assuming 'Code' is the column in BENEFIT
            how='left'
        )
        # Merge to get providername
        claims_with_benefit = claims_with_benefitt.join(
            PROVIDER.select(['providertin', 'providername']),
            left_on='nhisproviderid',
            right_on='providertin',
            how='left'
        )
        return claims_with_benefit
    
    except Exception as e:
       st.error(f"Error in benefit_and_pharmacy: {str(e)}")
       st.error(traceback.format_exc())    

def pharmacy_carecord():
    '''calculate any pharmacy related metrics'''
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame()  # Return an empty DataFrame instead of None

        # Check if required dataframes exist
        if 'BENEFITCBA' not in st.session_state.data:
            st.error("BENEFITCBA not found")
            return pl.DataFrame()   # Return an empty DataFrame instead of None
        if 'PROVIDER' not in st.session_state.data:
            st.error("PROVIDER not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'PA' not in st.session_state.data:
            st.error("PA not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'PAA' not in st.session_state.data:
            st.error("2022PA not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'PAAA' not in st.session_state.data:  # Fixed: Changed from 'PAA' to 'PAAA'
            st.error("2023PA not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'CCLAIMS' not in st.session_state.data:
            st.error("2022CLAIMS not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'CCCLAIMS' not in st.session_state.data:
            st.error("2023CLAIMS not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'CLAIMS' not in st.session_state.data:
            st.error("CLAIMS not found")
            return pl.DataFrame()  # Return an empty DataFrame instead of None
        if 'GROUPS' not in st.session_state.data:
            st.error("GROUPS not found")
            return pl.DataFrame()

        # Get required dataframes
        PROVIDER = st.session_state.data['PROVIDER']
        PA = st.session_state.data['PA']
        PAA = st.session_state.data.get('PAA', pl.DataFrame())
        PAAA = st.session_state.data.get('PAAA', pl.DataFrame())
        CLAIMS = st.session_state.data.get('CLAIMS', pl.DataFrame())
        CCLAIMS = st.session_state.data.get('CCLAIMS', pl.DataFrame())
        CCCLAIMS = st.session_state.data.get('CCCLAIMS', pl.DataFrame())
        GROUPS = st.session_state.data.get('GROUPS', pl.DataFrame())
        BENEFITCBA = st.session_state.data['BENEFITCBA']

        BENEFIT = BENEFITCBA.rename({
        "procedurecode": "Code",
        "benefitcodedesc": "Benefit"
        })
        B_CLAIMS = CLAIMS.rename({"nhislegacynumber": "IID", "encounterdatefrom" : "requestdate", "approvedamount" : "granted", "procedurecode" : "code"})

        # Ensure groupid is also Int64
        GROUPS = GROUPS.with_columns(pl.col("groupid").cast(pl.Int64))

        # Convert date columns to the same type before concatenation for PA dataframes
        def convert_pa_dates(df):
            if "date" in df.columns:
                df = df.with_columns(pl.col("date").dt.date())
            if "requestdate" in df.columns:
                df = df.with_columns(pl.col("requestdate").dt.date())
            return df
        
        # Force same schema for 'granted' column across PA, PAA, PAAA
        PA = PA.with_columns(pl.col("granted").cast(pl.Float64))
        PAA = PAA.with_columns(pl.col("granted").cast(pl.Float64))
        PAAA = PAAA.with_columns(pl.col("granted").cast(pl.Float64))
        
        # Apply date conversion
        PA = convert_pa_dates(PA)
        PAA = convert_pa_dates(PAA)
        PAAA = convert_pa_dates(PAAA)

        # Find common columns to ensure schema compatibility
        common_columns = list(set(PA.columns) & set(PAA.columns) & set(PAAA.columns))
        PA = PA.select(common_columns)
        PAA = PAA.select(common_columns)
        PAAA = PAAA.select(common_columns)

        # Now safe to concatenate with vertical_relaxed to be more forgiving of schema differences
        combined_PA = pl.concat([PA, PAA, PAAA], how="vertical_relaxed")

        # Similarly handle CLAIMS dataframes
        def convert_claims_dates(df):
            if "encounterdatefrom" in df.columns:
                df = df.with_columns(pl.col("encounterdatefrom").dt.date())
            return df
        
        CLAIMS = convert_claims_dates(CLAIMS)
        CCLAIMS = convert_claims_dates(CCLAIMS)
        CCCLAIMS = convert_claims_dates(CCCLAIMS)
        
        # Find common columns for CLAIMS
        claims_common_columns = list(set(CLAIMS.columns) & set(CCLAIMS.columns) & set(CCCLAIMS.columns))
        CLAIMS = CLAIMS.select(claims_common_columns)
        CCLAIMS = CCLAIMS.select(claims_common_columns)
        CCCLAIMS = CCCLAIMS.select(claims_common_columns)
        
        combined_CLAIMS = pl.concat([CLAIMS, CCLAIMS, CCCLAIMS], how="vertical_relaxed")

        # Convert granted to Float64
        PA = PA.with_columns(pl.col('granted').cast(pl.Float64).alias('cost'))
        combined_PA = combined_PA.with_columns(pl.col('granted').cast(pl.Float64).alias('cost'))

        # Ensure dates are in datetime format - cast to date first to ensure consistency
        if "requestdate" in PA.columns:
            PA = PA.with_columns(pl.col('requestdate').cast(pl.Date).cast(pl.Datetime).alias('date'))
        
        if "requestdate" in combined_PA.columns:
            combined_PA = combined_PA.with_columns(pl.col('requestdate').cast(pl.Date).cast(pl.Datetime).alias('date'))

        # Process CLAIMS - ensure consistent type casting
        combined_CLAIMS = combined_CLAIMS.select([
            # Include all columns except the ones we're transforming
            pl.col("*").exclude(["encounterdatefrom", "approvedamount", "nhisgroupid"]),
            
            # Add our transformed columns - ensuring proper type conversion
            pl.col('encounterdatefrom').cast(pl.Date).cast(pl.Datetime).alias('date'),
            pl.col('approvedamount').alias('cost'),
            
            # Clean nhisgroupid with explicit alias
            pl.col('nhisgroupid')
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Int64, strict=False)
            .alias('nhisgroupid')  # Explicitly keep original name
        ])

        # Merge CLAIMS with GROUPS to get group names
        combinedCLAIMS_group_names = combined_CLAIMS.join(
            GROUPS,
            left_on='nhisgroupid',
            right_on='groupid',
            how='left'
        )

        combinedCLAIMS_benefitt = combinedCLAIMS_group_names.join(
            BENEFIT,
            left_on='procedurecode',  # Assuming 'code' is the column in CLAIMS
            right_on='Code',  # Assuming 'Code' is the column in BENEFIT
            how='left'
        )
        # Merge to get providername
        all_claims = combinedCLAIMS_benefitt.join(
            PROVIDER.select(['providertin', 'providername']),
            left_on='nhisproviderid',
            right_on='providertin',
            how='left'
        )

        # Merge to get providername
        pa_provider = PA.join(
            PROVIDER.select(['providertin', 'providername']),
            left_on='providerid',
            right_on='providertin',
            how='left'
        )

        combinedPA_provider = combined_PA.join(
            PROVIDER.select(['providertin', 'providername']),
            left_on='providerid',
            right_on='providertin',
            how='left'
        )
        pa_benefit = pa_provider.join(
            BENEFIT.select(['Code', 'Benefit']),
            left_on='code',
            right_on='Code',
            how='left'
        )
        all_paa = combinedPA_provider.join(
            BENEFIT.select(['Code', 'Benefit']),
            left_on='code',
            right_on='Code',
            how='left'
        )

        all_pa = all_paa.join(
            BENEFIT.select(['Code', 'Benefit']),
            left_on='code',
            right_on='Code',
            how='left'
        )
        
        return pa_benefit, all_pa, all_claims

    except Exception as e:
        st.error(f"Error in pharmacy_carecord: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
    
def auto_invoice():
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame()

        # Check if required dataframes exist and collect them
        required_dfs = ['GROUP_CONTRACT', 'G_PLAN', 'GROUPS', 'PLANS']
        dfs = {}
        
        for df_name in required_dfs:
            if df_name not in st.session_state.data:
                st.error(f"{df_name} not found in session state")
                return pl.DataFrame()
            
            df = st.session_state.data[df_name]
            if isinstance(df, pl.LazyFrame):
                df = df.collect()
            dfs[df_name] = df
            
            if df.height == 0:
                st.error(f"{df_name} DataFrame is empty")
                return pl.DataFrame()

        # Get required dataframes
        GROUP_CONTRACT = dfs['GROUP_CONTRACT']
        G_PLAN = dfs['G_PLAN']
        GROUPS = dfs['GROUPS']
        PLANS = dfs['PLANS']

        # Add startdate and enddate to G_PLAN from GROUP_CONTRACT
        # First get unique groupid with their dates from GROUP_CONTRACT
        group_dates = GROUP_CONTRACT.select(['groupid', 'startdate', 'enddate']).unique(subset=['groupid'])

        # Calculate premium
        G_PLAN = G_PLAN.with_columns(
            (pl.col('countoffamily') * pl.col('familyprice') + 
             pl.col('countofindividual') * pl.col('individualprice')).alias('PREMIUM')
        )

        # Join these dates to G_PLAN
        G_PLAN_with_dates = G_PLAN.join(
            group_dates,
            on='groupid',
            how='left'
        )

        # Convert date columns to datetime type for consistency
        G_PLAN_with_dates = G_PLAN_with_dates.with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])

        # Join groupname from GROUPS to G_PLAN_with_dates
        G_PLAN_with_dates = G_PLAN_with_dates.join(
            GROUPS.select(['groupid', 'groupname']),
            on='groupid',
            how='left'
        )

        # Join planname from PLANS to G_PLAN_with_dates
        G_PLAN_with_dates = G_PLAN_with_dates.join(
            PLANS.select(['planid', 'planname']),
            on='planid', 
            how='left'
        )

        # Verify the final DataFrame is not empty
        if G_PLAN_with_dates.height == 0:
            st.error("No data available after processing")
            return pl.DataFrame()

        return G_PLAN_with_dates

    except Exception as e:
        st.error(f"Error in auto_invoice: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame()

def calculate_pa_mlr():
    """Calculate MLR using PA data"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame()
            
        # Check if required dataframes exist
        if 'PA' not in st.session_state.data:
            st.error("PA data not found")
            return pl.DataFrame()
        if 'GROUP_CONTRACT' not in st.session_state.data:
            st.error("GROUP_CONTRACT data not found")
            return pl.DataFrame()
        

        # Get required dataframes
        PA = st.session_state.data['PA']
        GROUP_CONTRACT = st.session_state.data['GROUP_CONTRACT']

        
        # Ensure dates are in datetime format
        PA = PA.with_columns(pl.col('requestdate').cast(pl.Datetime))
        GROUP_CONTRACT = GROUP_CONTRACT.with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])
        
        # Convert granted amount to numeric
        PA = PA.with_columns(pl.col('granted').cast(pl.Float64, strict=False))

        
        # Merge PA with GROUP_CONTRACT
        pa_with_dates = PA.join(
            GROUP_CONTRACT.select(['groupname', 'startdate', 'enddate']),
            on='groupname',
            how='inner'
        )
        
        # Filter PA data
        pa_filtered = pa_with_dates.filter(
            (pl.col('requestdate') >= pl.col('startdate')) & 
            (pl.col('requestdate') <= pl.col('enddate'))
        )

        
        # Calculate total granted amount
        PA_mlr= pa_filtered.group_by('groupname').agg(
            pl.col('granted').sum().alias('Total cost')
        )
        PA_mlr = PA_mlr.with_columns(
            (pl.col('Total cost') * 1.4).alias('PA(40%)')
        )

        PA_mlr = PA_mlr.sort('PA(40%)', descending=True)

        
        return PA_mlr
        
    except Exception as e:
        st.error(f"Error calculating PA MLR: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame()

def calculate_claims_mlr():
    """Calculate MLR using Claims data"""
    try:
        # Get required dataframes
        CLAIMS = st.session_state.data['CLAIMS']
        GROUPS = st.session_state.data['GROUPS']
        GROUP_CONTRACT = st.session_state.data['GROUP_CONTRACT']
        
        # Convert approvedamount to numeric
        CLAIMS = CLAIMS.with_columns(pl.col('approvedamount').cast(pl.Float64))
        
        # Ensure dates are in datetime format
        CLAIMS = CLAIMS.with_columns(pl.col('encounterdatefrom').cast(pl.Datetime))
        GROUP_CONTRACT = GROUP_CONTRACT.with_columns([
            pl.col('startdate').cast(pl.Datetime),
            pl.col('enddate').cast(pl.Datetime)
        ])
        
        # Convert groupid to string type in both dataframes before joining
        GROUPS = GROUPS.with_columns(pl.col('groupid').cast(pl.Utf8))
        CLAIMS = CLAIMS.with_columns(pl.col('nhisgroupid').cast(pl.Utf8))
        
        claims_with_group = CLAIMS.join(
            GROUPS.select(['groupid', 'groupname']),
            left_on='nhisgroupid',
            right_on='groupid',
            how='inner'
        )
        
        # Then merge with GROUP_CONTRACT to get contract dates
        claims_with_dates = claims_with_group.join(
            GROUP_CONTRACT.select(['groupname', 'startdate', 'enddate']),
            on='groupname',
            how='inner'
        )
        
        # Filter claims to only include those within contract dates
        claims_filtered = claims_with_dates.filter(
            (pl.col('encounterdatefrom') >= pl.col('startdate')) & 
            (pl.col('encounterdatefrom') <= pl.col('enddate'))
        )
        
        # Calculate total approved amount for each group
        claims_mlr = claims_filtered.group_by('groupname').agg(
            pl.col('approvedamount').sum().alias('Total cost')
        )
        
        # Sort by MLR descending
        claims_mlr = claims_mlr.sort('Total cost', descending=True)
        
        return claims_mlr
        
    except Exception as e:
        st.error(f"Error calculating Claims MLR: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame()

def clean_text(text):
    """Clean text: lowercasing, removing extra spaces and special characters"""
    text = str(text).lower()
    text = re.sub(r'\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    return text.strip()

def initialize_data():
    """Initialize all necessary data with better error handling and memory management"""
    try:
        # Initialize session state if it doesn't exist
        if 'data' not in st.session_state:
            st.session_state.data = {}
            
        # Only load data if it's not already loaded
        if not st.session_state.data:
            with st.spinner('Loading data...'):
                # Load data with progress tracking
                st.session_state.data_loading_progress = 0
                
                try:
                    data = load_excel_data()
                    
                    if not data:
                        st.error("Failed to load data. Please check the data files.")
                        st.session_state.data_loading_progress = -1
                        return None
                    
                    # Initialize session state with empty data first
                    st.session_state.data = {}
                    st.session_state.data_loading_progress = 25
                    
                    # Load data into session state
                    for key, value in data.items():
                        st.session_state.data[key] = value
                    st.session_state.data_loading_progress = 50
                    
                    # Track empty data frames
                    empty_frames = []
                    for key, df in st.session_state.data.items():
                        if isinstance(df, pl.LazyFrame):
                            sample = df.limit(1).collect()
                            if sample.height == 0:
                                empty_frames.append(key)
                    
                    # Report empty frames if any
                    if empty_frames:
                        st.warning(f"The following data frames are empty: {', '.join(empty_frames)}")
                    
                    # Check for critical data frames that should not be empty
                    critical_frames = ['PA', 'CLAIMS', 'PROVIDER', 'ACTIVE_ENROLLEE', 'GROUP_CONTRACT', 'ONLINE_PA']
                    missing_critical = [frame for frame in critical_frames if frame in empty_frames]
                    
                    if missing_critical:
                        st.error(f"Critical data frames are empty: {', '.join(missing_critical)}")
                        st.session_state.data = {}  # Reset session state on error
                        st.session_state.data_loading_progress = -1
                        return None
                    
                    # Calculate base metrics
                    try:
                        metrics = calculate_base_metrics(
                            st.session_state.data['PA'],
                            st.session_state.data['CLAIMS'],
                            st.session_state.data['PROVIDER'],
                            st.session_state.data['ACTIVE_ENROLLEE'],
                            st.session_state.data['GROUP_CONTRACT'],
                            st.session_state.data['GROUP_COVERAGE']
                        )
                        st.session_state.base_metrics = metrics
                        st.session_state.data_loading_progress = 100
                    except Exception as e:
                        st.error(f"Error calculating base metrics: {str(e)}")
                        st.session_state.data_loading_progress = -1
                        return None
                        
                except Exception as e:
                    st.error(f"Error loading data: {str(e)}")
                    st.session_state.data = {}  # Reset session state on error
                    st.session_state.data_loading_progress = -1
                    return None
                    
        return st.session_state.data
                    
    except Exception as e:
        st.error(f"Error in initialize_data: {str(e)}")
        st.error(traceback.format_exc())
        st.session_state.data_loading_progress = -1
        return None

def calculate_base_metrics(PA, CLAIMS, PROVIDER, ACTIVE_ENROLLEE, GROUP_CONTRACT, GROUP_COVERAGE):
    """Calculate base metrics from the data"""
    try:
        # Collect all lazy frames
        PA = PA.collect() if isinstance(PA, pl.LazyFrame) else PA
        CLAIMS = CLAIMS.collect() if isinstance(CLAIMS, pl.LazyFrame) else CLAIMS
        PROVIDER = PROVIDER.collect() if isinstance(PROVIDER, pl.LazyFrame) else PROVIDER
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.collect() if isinstance(ACTIVE_ENROLLEE, pl.LazyFrame) else ACTIVE_ENROLLEE
        GROUP_CONTRACT = GROUP_CONTRACT.collect() if isinstance(GROUP_CONTRACT, pl.LazyFrame) else GROUP_CONTRACT
        GROUP_COVERAGE = GROUP_COVERAGE.collect() if isinstance(GROUP_COVERAGE, pl.LazyFrame) else GROUP_COVERAGE
        
        # Ensure consistent data types for comparison
        filtered_provider = PROVIDER.with_columns(
            pl.col('provcatid').cast(pl.Utf8)
        ).filter(pl.col('provcatid') != '12')
        
        # Calculate provider distribution by state and category
        pivot_provider = (
            filtered_provider
            .group_by(['statename', 'categoryname'])
            .agg(pl.count('providerid').alias('count'))
            .pivot(
                values='count',
                index='statename',
                columns='categoryname'
            )
            .fill_null(0)
        )
        
        # Rest of your existing code...
        current_year = datetime.now().year
        current_year_pa = PA.filter(pl.col('requestdate').dt.year() == current_year)
        current_year_claims = CLAIMS.filter(pl.col('encounterdatefrom').dt.year() == current_year)
        
        # Calculate metrics
        metrics = {
            'TOTAL_ACTIVE_ENROLLEES': ACTIVE_ENROLLEE.select(pl.col('legacycode').unique().count())[0,0],
            'TOTAL_PROVIDER_COUNT': filtered_provider.select(pl.col('providerid').unique().count())[0,0],
            'TOTAL_ACTIVE_CONTRACTS': GROUP_CONTRACT.select(pl.col('groupid').unique().count())[0,0],
            'TOTAL_ACTIVE_POLICY': GROUP_COVERAGE.select(pl.col('groupid').unique().count())[0,0],
            'pivot_provider': pivot_provider,
            'TOTAL_PA_COST': round(current_year_pa.select(pl.col('granted').sum())[0,0], 2),
            'TOTAL_CLAIMS_COST': round(current_year_claims.select(pl.col('approvedamount').sum())[0,0], 2),
            'TOTAL_DENIED_COST': round(current_year_claims.select(pl.col('deniedamount').sum())[0,0], 2),
            'DENIAL_RATE': round(
                (current_year_claims.select(pl.col('deniedamount').sum())[0,0] / 
                current_year_claims.select(pl.col('approvedamount').sum())[0,0] * 100) 
                if current_year_claims.select(pl.col('approvedamount').sum())[0,0] > 0 else 0, 2
            )
        }
        
        return metrics
    except Exception as e:
        st.error(f"Error calculating base metrics: {str(e)}")
        st.error(traceback.format_exc())
        return {}

def prepare_active_plans():
    """Prepare active plans distribution data"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()
            
        # Check if required dataframes exist
        required_data = ['ACTIVE_ENROLLEE', 'M_PLAN', 'G_PLAN', 'PA', 'GROUPS', 'CLAIMS']
        for key in required_data:
            if key not in st.session_state.data:
                st.error(f"{key} data not found in session state")
                return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

        # Get dataframes and ensure they are collected if they are LazyFrames
        ACTIVE_ENROLLEE = st.session_state.data['ACTIVE_ENROLLEE']
        M_PLAN = st.session_state.data['M_PLAN']
        G_PLAN = st.session_state.data['G_PLAN']
        PA = st.session_state.data['PA']
        GROUP = st.session_state.data['GROUPS']
        CLAIMS = st.session_state.data['CLAIMS']

        # Collect LazyFrames if needed
        if isinstance(ACTIVE_ENROLLEE, pl.LazyFrame):
            ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.collect()
        if isinstance(M_PLAN, pl.LazyFrame):
            M_PLAN = M_PLAN.collect()
        if isinstance(G_PLAN, pl.LazyFrame):
            G_PLAN = G_PLAN.collect()
        if isinstance(PA, pl.LazyFrame):
            PA = PA.collect()
        if isinstance(GROUP, pl.LazyFrame):
            GROUP = GROUP.collect()
        if isinstance(CLAIMS, pl.LazyFrame):
            CLAIMS = CLAIMS.collect()

        # Ensure consistent data types
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.with_columns([
            pl.col("legacycode").cast(pl.Utf8),
            pl.col("memberid").cast(pl.Int64)
        ])

        M_PLAN = M_PLAN.with_columns([
            pl.col("memberid").cast(pl.Int64),
            pl.col("planid").cast(pl.Int64),
            pl.col("iscurrent").cast(pl.Utf8)
        ])

        G_PLAN = G_PLAN.with_columns([
            pl.col("planid").cast(pl.Int64),
            pl.col("groupid").cast(pl.Int64),
            pl.col("individualprice").cast(pl.Float64),
            pl.col("familyprice").cast(pl.Float64),
            pl.col("maxnumdependant").cast(pl.Int64)
        ])

        PA = PA.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("IID").cast(pl.Utf8),
            pl.col("granted").cast(pl.Float64)
        ])

        GROUP = GROUP.with_columns([
            pl.col("groupid").cast(pl.Int64),
            pl.col("groupname").cast(pl.Utf8)
        ])

        # Rename CLAIMS columns
        B_CLAIMS = CLAIMS.rename({
            "nhislegacynumber": "IID",
            "encounterdatefrom": "requestdate",
            "approvedamount": "granted",
            "procedurecode": "code",
            "nhisgroupid": "groupid"
        })

        # Filter current plans
        M_PLANN = M_PLAN.filter(pl.col("iscurrent") == "true")

        # Process dates
        BB_CLAIMS = B_CLAIMS.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("granted").cast(pl.Float64)
        ])

        # Handle empty groupid values before conversion
        BB_CLAIMS = BB_CLAIMS.with_columns([
            pl.when(pl.col('groupid') == "").then(None).otherwise(pl.col('groupid')).alias('groupid')
        ]).with_columns(
            pl.col('groupid').cast(pl.Int64, strict=False)
        )

        PAA = PA.with_columns(pl.col("requestdate").dt.year().alias("year"))

        # Calculate individual price
        G_PLAN_N = G_PLAN.with_columns([
            pl.when(pl.col("individualprice") == 0)
            .then(pl.col("familyprice") / pl.col("maxnumdependant"))
            .otherwise(pl.col("individualprice"))
            .alias("N_individual_price")
        ])

        # Categorize prices
        G_PLAN_C = G_PLAN_N.with_columns([
            pl.when(pl.col("N_individual_price") <= 30000)
            .then(pl.lit("0-30,000"))
            .when(pl.col("N_individual_price") <= 60000)
            .then(pl.lit("31,000 to 60,000"))
            .when(pl.col("N_individual_price") <= 100000)
            .then(pl.lit("61,000 to 100,000"))
            .when(pl.col("N_individual_price") <= 150000)
            .then(pl.lit("101,000 to 150,000"))
            .when(pl.col("N_individual_price") <= 200000)
            .then(pl.lit("151,000 to 200,000"))
            .when(pl.col("N_individual_price") <= 500000)
            .then(pl.lit("200,000 to 500,000"))
            .otherwise(pl.lit("above 500,000"))
            .alias("price_category")
        ])

        # Join with group names
        G_PLANN = G_PLAN_C.join(
            GROUP.select(['groupid', 'groupname']),
            on='groupid',
            how='left'
        )

        # Process BB_CLAIMS
        BBB_CLAIMS = BB_CLAIMS.join(
            GROUP.select(['groupid', 'groupname']),
            on='groupid',
            how='left'
        )

        # Process ACTIVE_ENROLLEE
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.drop('planid').join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        # Join with ACTIVE_ENROLLEE
        PA_M = PAA.join(
            ACTIVE_ENROLLEE.select(['legacycode', 'memberid']),
            left_on='IID',
            right_on='legacycode',
            how='left'
        )

        CLAIMS_M = BBB_CLAIMS.join(
            ACTIVE_ENROLLEE.select(['legacycode', 'memberid']),
            left_on='IID',
            right_on='legacycode',
            how='left'
        )

        # Join with M_PLANN
        PA_MP = PA_M.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        CLAIMS_MP = CLAIMS_M.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        # Ensure consistent types for joining
        PA_MP = PA_MP.with_columns(pl.col("planid").cast(pl.Int64))
        G_PLANN = G_PLANN.with_columns(pl.col("planid").cast(pl.Int64))

        # Final join
        merged_PA = PA_MP.join(
            G_PLANN,
            on=["planid", "groupname"],
            how="left"
        )

        # Select final columns
        merged_PA = merged_PA.select([
            "groupname",
            "planid",
            "memberid",
            "price_category",
            "granted",
            "IID",
            "code",
            "requestdate",
            "year"
        ])

        claims_mp = CLAIMS_MP.select([
            "groupname",
            pl.col("planid").cast(pl.Int64).alias("planid"),
            "granted",
            "IID",
            "code",
            "requestdate"
        ])

        # Calculate active plans distribution
        merged_data = ACTIVE_ENROLLEE.select(['planid', 'legacycode']).join(
            G_PLAN_C.select(['planid', 'price_category']),
            on='planid',
            how='left'
        )

        active_plans = merged_data.group_by('price_category').agg(
            pl.col('legacycode').n_unique().alias('count')
        )

        # Calculate percentages
        total_enrollees = active_plans.select(pl.col('count').sum())[0,0]
        active_plans = active_plans.with_columns(
            (pl.col('count') / total_enrollees * 100).round(2).alias('percentage')
        ).sort('count', descending=True)

        return active_plans, merged_PA, claims_mp

    except Exception as e:
        st.error(f"Error preparing active plans: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

def get_dataframe(key):
    """Safely get a DataFrame from session state with proper error handling"""
    try:
        if 'data' not in st.session_state:
            st.error("Session state not initialized. Please reload the page.")
            return pl.DataFrame()
            
        df = st.session_state.data.get(key)
        if df is None:
            st.warning(f"DataFrame '{key}' not found in session state")
            return pl.DataFrame()
            
        return df.collect() if isinstance(df, pl.LazyFrame) else df
        
    except Exception as e:
        st.error(f"Error getting DataFrame '{key}': {str(e)}")
        return pl.DataFrame()

revenue_data = get_dataframe('revenue_data')
PA = get_dataframe('PA')
# ... etc for other dataframes

def online_pa_usage():
    """Calculate the percentage of providers using online PA"""
    try:
        # Get required dataframes with explicit error handling
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return None, None, None
            
        if 'PROVIDER' not in st.session_state.data:
            st.error("PROVIDER data not found")
            return None, None, None
            
        if 'PA' not in st.session_state.data:
            st.error("PA data not found")
            return None, None, None
            
        if 'ONLINE_PA' not in st.session_state.data:
            st.error("ONLINE_PA data not found")
            return None, None, None

        # Get and collect the dataframes
        PROVIDER = st.session_state.data['PROVIDER']
        PA = st.session_state.data['PA']
        ONLINE_PA = st.session_state.data['ONLINE_PA']

        # Collect LazyFrames if needed
        PROVIDER = PROVIDER.collect() if isinstance(PROVIDER, pl.LazyFrame) else PROVIDER
        PA = PA.collect() if isinstance(PA, pl.LazyFrame) else PA
        ONLINE_PA = ONLINE_PA.collect() if isinstance(ONLINE_PA, pl.LazyFrame) else ONLINE_PA

        # Standardize column names
        PA = PA.rename({"panumber": "PANumber"})
        ONLINE_PA = ONLINE_PA.rename({"PANumber": "PANumber"})  

        # Filter out NHIA and University of Lagos
        PA = PA.filter(pl.col('groupname') != 'NHIA')

        # Ensure date columns are in proper format
        PA = PA.with_columns(pl.col('requestdate').cast(pl.Date))
        ONLINE_PA = ONLINE_PA.with_columns(pl.col('RequestDate').cast(pl.Date))

        # Rename provider ID and request date columns in PA
        PA = PA.with_columns([
            pl.col("providerid").alias("Providerid"),
            pl.col("requestdate").alias("RequestDate")
        ])

        # Filter for this year
        current_year = datetime.now().year
        PA = PA.filter(pl.col('RequestDate').is_between(
            datetime(current_year, 1, 1).date(), datetime(current_year, 12, 31).date()
        ))

        ONLINE_PA = ONLINE_PA.filter(pl.col('RequestDate').is_between(
            datetime(current_year, 1, 1).date(), datetime(current_year, 12, 31).date()
        ))

        # Filter for the last 14 days
        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=13)
        PA_last_14 = PA.filter(pl.col('RequestDate').is_between(start_date, end_date))
        ONLINE_PA_last_14 = ONLINE_PA.filter(pl.col('RequestDate').is_between(start_date, end_date))

        # ---- DAILY STATS ----
        # Simplify the approach by focusing on unique PA numbers per provider
        
        # Get unique provider IDs from both datasets - ensure consistent data types
        # First, ensure Providerid is the same type in both dataframes
        PA_last_14 = PA_last_14.with_columns(pl.col("Providerid").cast(pl.Utf8))
        ONLINE_PA_last_14 = ONLINE_PA_last_14.with_columns(pl.col("Providerid").cast(pl.Utf8))
        
        # Now concatenate the unique provider IDs
        providers_daily = (
            pl.concat([
                PA_last_14.select("Providerid").unique(), 
                ONLINE_PA_last_14.select("Providerid").unique()
            ])
            .unique()
        )
        
        daily_stats = []
        
        # For each provider, calculate the percentages correctly
        for provider_id in providers_daily["Providerid"]:
            # Get unique PA numbers for this provider
            pa_numbers = PA_last_14.filter(pl.col("Providerid") == provider_id)["PANumber"].unique()
            online_pa_numbers = ONLINE_PA_last_14.filter(pl.col("Providerid") == provider_id)["PANumber"].unique()
            
            # Find PA numbers that exist in both datasets
            common_pa_numbers = set(pa_numbers) & set(online_pa_numbers)
            
            total_pa = len(pa_numbers)
            online_pa = len(common_pa_numbers)
            
            # Calculate percentage (avoid division by zero)
            online_percentage = (online_pa / total_pa * 100) if total_pa > 0 else 0
            
            daily_stats.append({
                "Providerid": provider_id,
                "total_pa": total_pa,
                "online_pa": online_pa,
                "online_percentage": online_percentage
            })
        
        # Convert to DataFrame
        pa_daily_stat = pl.DataFrame(daily_stats)
        
        # Join with provider names
        Provider = PROVIDER.rename({"providertin": "Providerid"})
        pa_daily_stats = (
            pa_daily_stat.join(Provider.select(["Providerid", "providername"]), on="Providerid", how="left")
            .select(["providername", "total_pa", "online_pa", "online_percentage"])
        )

        # ---- YEARLY STATS ----
        # Apply the same approach for yearly stats
        
        # Get unique provider IDs from both datasets - ensure consistent data types
        # First, ensure Providerid is the same type in both dataframes
        PA = PA.with_columns(pl.col("Providerid").cast(pl.Utf8))
        ONLINE_PA = ONLINE_PA.with_columns(pl.col("Providerid").cast(pl.Utf8))
        
        # Now concatenate the unique provider IDs
        providers_yearly = (
            pl.concat([
                PA.select("Providerid").unique(), 
                ONLINE_PA.select("Providerid").unique()
            ])
            .unique()
        )
        
        yearly_stats = []
        
        # For each provider, calculate the percentages correctly
        for provider_id in providers_yearly["Providerid"]:
            # Get unique PA numbers for this provider
            pa_numbers = PA.filter(pl.col("Providerid") == provider_id)["PANumber"].unique()
            online_pa_numbers = ONLINE_PA.filter(pl.col("Providerid") == provider_id)["PANumber"].unique()
            
            # Find PA numbers that exist in both datasets
            common_pa_numbers = set(pa_numbers) & set(online_pa_numbers)
            
            total_pa = len(pa_numbers)
            online_pa = len(common_pa_numbers)
            
            # Calculate percentage (avoid division by zero)
            online_percentage = (online_pa / total_pa * 100) if total_pa > 0 else 0
            
            yearly_stats.append({
                "Providerid": provider_id,
                "total_pa": total_pa,
                "online_pa": online_pa,
                "online_percentage": online_percentage
            })
        
        # Convert to DataFrame
        pa_yearly_stat = pl.DataFrame(yearly_stats)
        
        # Join with provider names
        pa_yearly_stats = (
            pa_yearly_stat.join(Provider.select(["Providerid", "providername"]), on="Providerid", how="left")
            .select(["providername", "total_pa", "online_pa", "online_percentage"])
        )

        return pa_daily_stats, pa_yearly_stats, ONLINE_PA

    except Exception as e:
        st.error(f"Error in online_pa_usage: {str(e)}")
        st.error(traceback.format_exc())
        return None, None, None

def client_analysis():
    """Analyze client data and return relevant metrics"""
    try:
        # Get required dataframes and collect them
        PA = st.session_state.data.get('PA', pl.DataFrame())
        GL = st.session_state.data.get('GL', pl.DataFrame())
        E_ACCT_GROUP = st.session_state.data.get('E_ACCT_GROUP', pl.DataFrame())
        GROUPS = st.session_state.data.get('GROUPS', pl.DataFrame())
        DEBIT = st.session_state.data.get('DEBIT', pl.DataFrame())
        G_PLAN = st.session_state.data.get('G_PLAN', pl.DataFrame())
        PROVIDER = st.session_state.data.get('PROVIDER', pl.DataFrame()) 
        BENEFITCBA = st.session_state.data.get('BENEFITCBA', pl.DataFrame())  

        # Collect LazyFrames if they are LazyFrames
        PA = PA.collect() if isinstance(PA, pl.LazyFrame) else PA
        GL = GL.collect() if isinstance(GL, pl.LazyFrame) else GL
        E_ACCT_GROUP = E_ACCT_GROUP.collect() if isinstance(E_ACCT_GROUP, pl.LazyFrame) else E_ACCT_GROUP
        GROUPS = GROUPS.collect() if isinstance(GROUPS, pl.LazyFrame) else GROUPS
        DEBIT = DEBIT.collect() if isinstance(DEBIT, pl.LazyFrame) else DEBIT
        G_PLAN = G_PLAN.collect() if isinstance(G_PLAN, pl.LazyFrame) else G_PLAN
        PROVIDER = PROVIDER.collect() if isinstance(PROVIDER, pl.LazyFrame) else PROVIDER
        BENEFITCBA = BENEFITCBA.collect() if isinstance(BENEFITCBA, pl.LazyFrame) else BENEFITCBA

        BENEFIT = BENEFITCBA.rename({
            "procedurecode": "Code",
            "benefitcodedesc": "Benefit"
        })

        # Cast columns to appropriate types
        debit = DEBIT.with_columns([
            pl.col("Amount").cast(pl.Float64),  # Add this line to ensure amount is numeric
            pl.col("From").cast(pl.Date)  # Ensure dates are correctly typed
        ])

        # Filter DEBIT for dates within the last 12 months
        today = datetime.now().date()
        one_year_ago = today - timedelta(days=365)
        debit = debit.filter(
                pl.col('From').is_not_null() & 
                (pl.col('From') >= pl.lit(one_year_ago)) &
                (pl.col('From') <= pl.lit(today))
            )
        debit = debit.rename({"CompanyName": "groupname"})
        
        # Convert ID_Company and code columns to string type for joining
        # Cast columns to appropriate types
        ledger = GL.with_columns([
            pl.col("GLDate").cast(pl.Datetime),
            pl.col("GLAmount").cast(pl.Float64)  # Add this line to ensure granted is numeric
        ])
        E_ACCT_GROUP = E_ACCT_GROUP.with_columns(pl.col("ID_Company").cast(pl.Utf8))
        ledger = ledger.with_columns(pl.col("code").cast(pl.Utf8))
        GL = ledger.join(
            E_ACCT_GROUP.select(['ID_Company', 'CompanyName']),
            left_on='code',
            right_on='ID_Company',
            how='left'
        ).rename({"CompanyName": "groupname"})
        
        final_ledger = GL.filter(
            pl.col("acctype").is_in(["CURRENT ASSETS", "CASH"])
        ).group_by(
            ["code", "groupname", "GLDate", "GLDesc"]
        ).agg([
            pl.col("GLAmount").sum().alias("total_amount"),
            pl.col("GLAmount").alias("amounts"),
            pl.col("acctype").alias("acctypes")
        ])
        
        # Filter for groups where:
        # 1. Total amount is 0 (debits and credits match)
        # 2. There are exactly 2 entries (CURRENT ASSET and CASH)
        # 3. Contains both CURRENT ASSET and CASH entries
        matching_entries = final_ledger.filter(
            (pl.col("total_amount") == 0) & 
            (pl.col("amounts").list.len() == 2) &
            (pl.col("acctypes").list.contains("CURRENT ASSETS")) &
            (pl.col("acctypes").list.contains("CASH"))
        )
        
        # Extract the positive amount and create the result list
        ledger_result = matching_entries.select([
            "GLDesc",
            "groupname",
            "GLDate",
            pl.col("amounts").list.max().alias("GLAmount")
        ])

        # DATA MODIFICATION 
        groupname_plan = G_PLAN.join(
            GROUPS.select(["groupid", "groupname"]),  # Select only the columns we need from group
            on="groupid",                            # Join on the common column
            how="left")

        groupname_plan = groupname_plan.with_columns([
            pl.col("planid").cast(pl.Utf8),  # Ensure planid is string for grouping
            (pl.col('countoffamily') * pl.col('familyprice') + 
             pl.col('countofindividual') * pl.col('individualprice')).alias('PREMIUM')
        ])
        
        # Join provider data to main dataframe
        main_dfff = PA.join(
            PROVIDER.select(["providertin", "providername"]),
            left_on="providerid",
            right_on="providertin",
            how="left"
        )
        main_dff = main_dfff.join(
            BENEFIT.select(["Benefit", "Code"]),
            left_on="code",
            right_on="Code",
            how="left"
        )
        # Cast columns to appropriate types
        main_dff = main_dff.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("granted").cast(pl.Float64)  # Add this line to ensure granted is numeric
        ])

        main_dff = main_dff.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            # Remove commas and convert 'granted' to float explicitly
            pl.col("granted").cast(pl.Float64).alias("granted_numeric"),
            (pl.col("granted").cast(pl.Float64) * 1.4).alias("granted_with_markup")
        ])

        # Ensure all returned DataFrames are collected
        main_dff = main_dff.collect() if isinstance(main_dff, pl.LazyFrame) else main_dff
        ledger_result = ledger_result.collect() if isinstance(ledger_result, pl.LazyFrame) else ledger_result
        debit = debit.collect() if isinstance(debit, pl.LazyFrame) else debit
        groupname_plan = groupname_plan.collect() if isinstance(groupname_plan, pl.LazyFrame) else groupname_plan

        return main_dff, ledger_result, debit, groupname_plan
        
    except Exception as e:
        st.error(f"Error in client_analysis: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame(), pl.DataFrame(), pl.DataFrame()

def benefit_limit():
    """Prepare active plans distribution data"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame(), pl.DataFrame()

        # Get data from session state with proper error handling
        required_data = ['ACTIVE_ENROLLEE', 'M_PLAN', 'PA', 'GROUPS', 'CLAIMS']
        for key in required_data:
            if key not in st.session_state.data:
                st.error(f"{key} data not found in session state")
                return pl.DataFrame(), pl.DataFrame()

        # Get dataframes and ensure they are collected if they are LazyFrames
        ACTIVE_ENROLLEE = st.session_state.data['ACTIVE_ENROLLEE']
        M_PLAN = st.session_state.data['M_PLAN']
        PA = st.session_state.data['PA']
        GROUP = st.session_state.data['GROUPS']
        CLAIMS = st.session_state.data['CLAIMS']

        # Collect LazyFrames if needed
        if isinstance(ACTIVE_ENROLLEE, pl.LazyFrame):
            ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.collect()
        if isinstance(M_PLAN, pl.LazyFrame):
            M_PLAN = M_PLAN.collect()
        if isinstance(PA, pl.LazyFrame):
            PA = PA.collect()
        if isinstance(GROUP, pl.LazyFrame):
            GROUP = GROUP.collect()
        if isinstance(CLAIMS, pl.LazyFrame):
            CLAIMS = CLAIMS.collect()

        # First, ensure GROUP has the correct columns and types
        GROUP = GROUP.with_columns([
            pl.col("groupid").cast(pl.Int64, strict=False),
            pl.col("groupname").cast(pl.Utf8)
        ])

        # Ensure PA has the correct columns and types
        PA = PA.with_columns([
            pl.col("requestdate").cast(pl.Datetime),
            pl.col("IID").cast(pl.Utf8),
            pl.col("code").cast(pl.Utf8),
            pl.col("granted").cast(pl.Float64),
            pl.col("panumber").cast(pl.Utf8),
        ])

        # Create B_CLAIMS with proper column mapping and type casting
        B_CLAIMS = CLAIMS.select([
            pl.col("nhislegacynumber").cast(pl.Utf8).alias("IID"),
            pl.col("encounterdatefrom").cast(pl.Datetime).alias("requestdate"),
            pl.col("approvedamount").cast(pl.Float64).alias("granted"),
            pl.col("procedurecode").cast(pl.Utf8).alias("code"),
            pl.col("panumber").cast(pl.Utf8),
            pl.col("nhisgroupid")
        ])

        # Handle groupid conversion in a single step
        B_CLAIMS = B_CLAIMS.with_columns([
            pl.col("nhisgroupid")
            .str.strip_chars()
            .replace("", None)
            .cast(pl.Int64, strict=False)
            .alias("groupid")
        ]).drop("nhisgroupid")

        # Filter current plans
        M_PLANN = M_PLAN.filter(pl.col("iscurrent") == "true")

        # Filter DEBIT for dates within the last 12 months
        PA = PA.with_columns(pl.col("requestdate").cast(pl.Datetime))
        BB_CLAIMS = B_CLAIMS.with_columns(pl.col("requestdate").cast(pl.Datetime))

        # Create a simplified GROUP DataFrame with only the columns we need
        GROUP_SIMPLE = GROUP.select(['groupid', 'groupname'])

        # Join with GROUP to get groupname - use left join to preserve all records
        try:
            # First, ensure both groupid columns are the same type
            BB_CLAIMS = BB_CLAIMS.with_columns(pl.col("groupid").cast(pl.Int64, strict=False))
            GROUP_SIMPLE = GROUP_SIMPLE.with_columns(pl.col("groupid").cast(pl.Int64, strict=False))

            # Perform the join
            BB_CLAIMS = BB_CLAIMS.join(
                GROUP_SIMPLE,
                on='groupid',
                how='left'
            )

        except Exception as e:
            st.error(f"Error during join operation: {str(e)}")
            st.error("BB_CLAIMS groupid type:", BB_CLAIMS["groupid"].dtype)
            st.error("GROUP_SIMPLE groupid type:", GROUP_SIMPLE["groupid"].dtype)
            raise

        # Process ACTIVE_ENROLLEE
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.drop('planid')
        ACTIVE_ENROLLEE = ACTIVE_ENROLLEE.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        # Join PA with ACTIVE_ENROLLEE
        PA_M = PA.join(
            ACTIVE_ENROLLEE.select(['legacycode', 'memberid']),
            left_on='IID',
            right_on='legacycode',
            how='left'
        )

        # Join CLAIMS with ACTIVE_ENROLLEE
        CLAIMS_M = BB_CLAIMS.join(
            ACTIVE_ENROLLEE.select(['legacycode', 'memberid']),
            left_on='IID',
            right_on='legacycode',
            how='left'
        )

        # Join with M_PLANN for plan information
        PA_MP = PA_M.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        CLAIMS_MP = CLAIMS_M.join(
            M_PLANN.select(['memberid', 'planid']),
            on='memberid',
            how='left'
        )

        # Debug: Print intermediate DataFrame info
        st.write("PA_MP columns:", PA_MP.columns)
        st.write("CLAIMS_MP columns:", CLAIMS_MP.columns)

        # Select final columns with proper type casting and error handling
        try:
            # First, ensure all required columns exist
            required_columns = ["panumber", "groupname", "planid", "granted", "IID", "code", "requestdate"]
            
            # Check if all required columns exist in PA_MP
            missing_columns_pa = [col for col in required_columns if col not in PA_MP.columns]
            if missing_columns_pa:
                st.error(f"Missing columns in PA_MP: {missing_columns_pa}")
                return pl.DataFrame(), pl.DataFrame()
            
            # Check if all required columns exist in CLAIMS_MP
            missing_columns_claims = [col for col in required_columns if col not in CLAIMS_MP.columns]
            if missing_columns_claims:
                st.error(f"Missing columns in CLAIMS_MP: {missing_columns_claims}")
                return pl.DataFrame(), pl.DataFrame()

            # Create BENEFIT_PA with explicit column selection and consistent ordering
            BENEFIT_PA = PA_MP.select([
                pl.col("panumber").cast(pl.Utf8),
                pl.col("groupname").cast(pl.Utf8),
                pl.col("planid").cast(pl.Int64, strict=False),
                pl.col("granted").cast(pl.Float64),
                pl.col("IID").cast(pl.Utf8),
                pl.col("code").cast(pl.Utf8),
                pl.col("requestdate").cast(pl.Datetime)
            ]).select(required_columns)  # Ensure consistent column ordering

            # Create claims_BENEFIT with explicit column selection and consistent ordering
            claims_BENEFIT = CLAIMS_MP.select([
                pl.col("panumber").cast(pl.Utf8),
                pl.col("groupname").cast(pl.Utf8),
                pl.col("planid").cast(pl.Int64, strict=False),
                pl.col("granted").cast(pl.Float64),
                pl.col("IID").cast(pl.Utf8),
                pl.col("code").cast(pl.Utf8),
                pl.col("requestdate").cast(pl.Datetime)
            ]).select(required_columns)  # Ensure consistent column ordering

            # Verify the final DataFrames are not empty
            if BENEFIT_PA.height == 0:
                st.error("BENEFIT_PA DataFrame is empty after processing")
                return pl.DataFrame(), pl.DataFrame()
            if claims_BENEFIT.height == 0:
                st.error("claims_BENEFIT DataFrame is empty after processing")
                return pl.DataFrame(), pl.DataFrame()

            return BENEFIT_PA, claims_BENEFIT

        except Exception as e:
            st.error(f"Error during final DataFrame creation: {str(e)}")
            st.error("PA_MP columns:", PA_MP.columns)
            st.error("CLAIMS_MP columns:", CLAIMS_MP.columns)
            raise

    except Exception as e:
        st.error(f"Error preparing active plans: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame(), pl.DataFrame()

def prepare_claims_comparison(data):
    """Prepare claims comparison data for analysis"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return pl.DataFrame()
            
        # Check if required dataframes exist
        if 'PA' not in st.session_state.data:
            st.error("PA data not found")
            return pl.DataFrame()
        if 'CLAIMS' not in st.session_state.data:
            st.error("CLAIMS data not found")
            return pl.DataFrame()
            
        # Get required dataframes and collect them
        PA = st.session_state.data['PA'].collect()
        CLAIMS = st.session_state.data['CLAIMS'].collect()
        
        # Ensure dates are in datetime format
        PA = PA.with_columns(pl.col('requestdate').cast(pl.Datetime))
        CLAIMS = CLAIMS.with_columns(pl.col('encounterdatefrom').cast(pl.Datetime))

        # Ensure amounts are numeric
        PA = PA.with_columns(pl.col('granted').cast(pl.Float64))
        CLAIMS = CLAIMS.with_columns(pl.col('approvedamount').cast(pl.Float64))
        
        # Calculate monthly PA costs
        monthly_pa = PA.group_by(
            pl.col('requestdate').dt.strftime('%B %Y').alias('Month')
        ).agg(
            pl.col('granted').sum().alias('PA Cost')
        ).sort('Month')
        
        # Calculate monthly claims costs
        monthly_claims = CLAIMS.group_by(
            pl.col('encounterdatefrom').dt.strftime('%B %Y').alias('Month')
        ).agg(
            pl.col('approvedamount').sum().alias('Claims Cost')
        ).sort('Month')
        
        # Join PA and claims data
        comparison = monthly_pa.join(
            monthly_claims,
            on='Month',
            how='outer'
        ).fill_null(0)
        
        # Calculate unclaimed PA cost (PA cost - claims cost)
        comparison = comparison.with_columns(
            (pl.col('PA Cost') - pl.col('Claims Cost')).alias('Unclaimed PA Cost')
        )
        
        # Store in session state for later use
        st.session_state.claims_comparison = comparison
        
        return comparison

    except Exception as e:
        st.error(f"Error in prepare_claims_comparison: {str(e)}")
        st.error(traceback.format_exc())
        return pl.DataFrame()

def revenue_pa():
    """Calculate revenue metrics and PA cost comparison"""
    try:
        # Ensure data is initialized
        if 'data' not in st.session_state:
            initialize_data()

        # Access valid_debits from session state
        if 'valid_debits' not in st.session_state:
            st.error("valid_debits not found in session state. Please ensure process_debit_notes() is called.")
            return {}
        
        valid_debits = st.session_state.valid_debits    
            
        # Get required dataframes and collect them
        revenue_data = st.session_state.data.get('revenue_data', pl.DataFrame())
        PA = st.session_state.data.get('PA', pl.DataFrame())
        GL = st.session_state.data.get('GL', pl.DataFrame())
        EPREMIUM = st.session_state.data.get('EPREMIUM', pl.DataFrame())
        GLSETUP = st.session_state.data.get('GLSETUP', pl.DataFrame())
        E_ACCT_GROUP = st.session_state.data.get('E_ACCT_GROUP', pl.DataFrame())
        GCONTRACT = st.session_state.data.get('GROUP_CONTRACT', pl.DataFrame())

        # Collect LazyFrames if they are LazyFrames
        revenue_data = revenue_data.collect() if isinstance(revenue_data, pl.LazyFrame) else revenue_data
        PA = PA.collect() if isinstance(PA, pl.LazyFrame) else PA
        GL = GL.collect() if isinstance(GL, pl.LazyFrame) else GL
        EPREMIUM = EPREMIUM.collect() if isinstance(EPREMIUM, pl.LazyFrame) else EPREMIUM
        GLSETUP = GLSETUP.collect() if isinstance(GLSETUP, pl.LazyFrame) else GLSETUP
        E_ACCT_GROUP = E_ACCT_GROUP.collect() if isinstance(E_ACCT_GROUP, pl.LazyFrame) else E_ACCT_GROUP
        GCONTRACT = GCONTRACT.collect() if isinstance(GCONTRACT, pl.LazyFrame) else GCONTRACT
        
        # Validate input data
        if revenue_data.height == 0 or PA.height == 0 or GL.height == 0 or EPREMIUM.height == 0 or GLSETUP.height == 0 or E_ACCT_GROUP.height == 0 or GCONTRACT.height == 0:
            st.error("Required data not found")
            return {}

        # Convert amount column to numeric type
        GL = GL.with_columns(
            pl.col("GLAmount").cast(pl.Float64).alias("GLAmount")
        )

        # Get selected contract 
        GCONTRACT = GCONTRACT.select(['groupname', 'startdate', 'enddate'])

        # Convert AccCode to string in both DataFrames
        GL = GL.with_columns(pl.col("AccCode").cast(pl.Utf8))
        GLSETUP = GLSETUP.with_columns(pl.col("AccCode").cast(pl.Utf8))

        GL = GL.with_columns(
            pl.col("AccCode").str.replace(r"\.0$", "").alias("AccCode")
        )

        
        # Perform a left join to add AccDesc from GLSETUP to GL using AccCode as the key
        GLL = GL.join(
        GLSETUP.select(["AccCode", "AccDesc"]),
        left_on="AccCode",
        right_on="AccCode",
        how="left"
        ).rename({"AccDesc": "DESCRIPTION"})

        # Convert GLDate to datetime format
        GLL = GLL.with_columns(pl.col("GLDate").cast(pl.Datetime))
        
        # Extract month and year from GLDate
        GLL = GLL.with_columns([
            pl.col("GLDate").dt.month().alias("Month"),
            pl.col("GLDate").dt.year().alias("Year")
        ])
        # Convert ID_Company and code columns to string type for joining
        E_ACCT_GROUP = E_ACCT_GROUP.with_columns(pl.col("ID_Company").cast(pl.Utf8))
        GLL = GLL.with_columns(pl.col("code").cast(pl.Utf8))

        GLT = GLL.join(
            E_ACCT_GROUP.select(['ID_Company', 'CompanyName']),
            left_on='code',
            right_on='ID_Company',
            how='left'
        )


        ### 2. ALLOCATED PREMIUM FOR ALLOCATION###
        E_PREMIUM = EPREMIUM.filter(
            (~pl.col("description").str.to_lowercase().str.contains("tpa")) &
            (pl.col("GL_year") == 2025)
        )

        #CONVERT COLUMN NAME TO MONTH 
        # Create mapping of month numbers to month names
        month_map = {
            1: 'JANUARY',
            2: 'FEBRUARY',
            3: 'MARCH',
            4: 'APRIL',
            5: 'MAY',
            6: 'JUNE',
            7: 'JULY',
            8: 'AUGUST',
            9: 'SEPTEMBER',
            10: 'OCTOBER',
            11: 'NOVEMBER',
            12: 'DECEMBER'
        }

        # Rename columns from mth1-mth12 to month names
        E_PREMIUM = E_PREMIUM.rename({
            f'mth{i}': month_map[i] for i in range(1, 13)
        })

        # Convert month columns to numeric type
        E_PREMIUM = E_PREMIUM.with_columns([
            pl.col(month).cast(pl.Float64, strict=False) 
            for month in month_map.values()
        ])

        
        # Get current month and next month
        current_date = datetime.now()
        current_month = current_date.strftime('%B').upper()
        next_month =  (current_date.replace(day=1) + timedelta(days=31)).strftime('%B').upper()
        
        # Convert monthly columns to numeric
        monthly_columns = revenue_data.columns[2:]  # Skip 'COMPANY NAME' and 'PATTERN'
        revenue_data = revenue_data.with_columns([pl.col(col).cast(pl.Float64, strict=False) for col in monthly_columns])
        
        # Calculate total receivables for each month
        monthly_totals = revenue_data.select([pl.col(col).sum() for col in monthly_columns]).row(0)
        monthly_totals = dict(zip(monthly_columns, monthly_totals))
        
        # Calculate current month's receivables
        current_month_receivables = revenue_data.select(pl.col(current_month)).sum().item() if current_month in revenue_data.columns else 0
        
        # Calculate PA cost for current month
        PA = PA.with_columns([pl.col('requestdate').cast(pl.Datetime), pl.col('granted').cast(pl.Float64)])
        
        current_month_pa = PA.filter(
            (pl.col('requestdate').dt.month() == current_date.month) &
            (pl.col('requestdate').dt.year() == current_date.year)
        )
        
        current_month_pa_cost = current_month_pa.select(pl.col('granted')).sum().item() * 1.4  # Adding 40% markup
        
        # Calculate next month's forecast
        if next_month in revenue_data.columns:
            next_month_payers = revenue_data.filter(pl.col(next_month) > 0).select(['COMPANY NAME', next_month])
            next_month_total = next_month_payers.select(pl.col(next_month)).sum().item()
            next_month_payers_list = next_month_payers.to_dicts()
        else:
            next_month_total = 0
            next_month_payers_list = []

        # After processing the data, call the analyze_company_payments function
        payment_analysis = analyze_company_payments(valid_debits, GCONTRACT, GLT)    
        
        # Prepare metrics dictionary
        metrics = {
            'monthly_totals': monthly_totals,
            'current_month': {
                'month': current_month,
                'receivables': float(current_month_receivables),
                'pa_cost': float(current_month_pa_cost),
                'ratio': float((current_month_receivables / current_month_pa_cost * 100) if current_month_pa_cost > 0 else 0),
                'allo_premium': E_PREMIUM.to_dicts(),
                'General Ledger': GLT,
                'contract': GCONTRACT.to_dicts(),
                'payment_analysis': payment_analysis  # Return the DataFrame directly instead of converting to dicts
            },
            'next_month': {
                'month': next_month,
                'total_expected': float(next_month_total),
                'payers': next_month_payers_list
            }
        }
        
        return metrics
        
    except Exception as e:
        st.error(f"Error calculating revenue metrics: {str(e)}")
        st.error(traceback.format_exc())
        return {}
    
def analyze_company_payments(debit_df, contract_df, ledger_df):
    """
    Analyzes company payments by matching debit amounts and ledger entries within contract periods.
    
    Parameters:
    -----------
    debit_df : pl.DataFrame
        DataFrame containing debit information with columns: CompanyName, Amount, From, To
    contract_df : pl.DataFrame
        DataFrame containing contract information with columns: CompanyName, start_date, end_date
    ledger_df : pl.DataFrame
        DataFrame containing ledger information with columns: CompanyName, GlAmount, GLDate
    
    Returns:
    --------
    pl.DataFrame
        DataFrame with columns: CompanyName, DebitAmount, LedgerAmount, Balance
    """
    # Convert date columns to datetime
    debit_df = debit_df.with_columns([
        pl.col('From').cast(pl.Datetime),
        pl.col('To').cast(pl.Datetime)
    ])
    
    contract_df = contract_df.with_columns([
        pl.col('startdate').cast(pl.Datetime),
        pl.col('enddate').cast(pl.Datetime)
    ])

    contract_df = contract_df.rename({"groupname": "CompanyName"})
    
    ledger_df = ledger_df.with_columns([
        pl.col('GLDate').cast(pl.Datetime)
    ])

    # Modified approach: Group by code, company, and date only (not using description)
    # This will match transactions with the same code, company, and date regardless of description
    grouped = ledger_df.filter(
        pl.col("acctype").is_in(["CURRENT ASSETS", "CASH"])
    ).group_by(
        ["code", "CompanyName", "GLDate"]
    ).agg([
        pl.col("GLAmount").sum().alias("total_amount"),
        pl.col("GLAmount").alias("amounts"),
        pl.col("acctype").alias("acctypes")
    ])
    
    # First identify potential matching groups without requiring exact zero balance
    # Look for cases where amounts approximately offset each other
    matching_entries = grouped.filter(
        (pl.col("total_amount").abs() < 0.01) &  # Allow for small floating point differences
        (pl.col("amounts").list.len() >= 2) &    # At least 2 entries
        (pl.col("acctypes").list.contains("CURRENT ASSETS") | 
         pl.col("acctypes").list.contains("CASH"))
    )
    
    # Extract the positive amount and create the result list
    # For each matching group, take the absolute maximum amount
    ledger_result = matching_entries.select([
        "CompanyName",
        "GLDate",
        pl.col("amounts").list.eval(pl.element().abs()).list.max().alias("GLAmount")
    ])
    
    # Initialize results list
    results = []
    
    # Process each company in the contract DataFrame
    for contract_row in contract_df.iter_rows(named=True):
        company = contract_row['CompanyName']
        start_date = contract_row['startdate']
        end_date = contract_row['enddate']
        
        # Get debit amounts for this company within the contract period
        company_debits = debit_df.filter(
            (pl.col('CompanyName') == company) &
            (
                # Either dates match exactly
                ((pl.col('From') == start_date) & (pl.col('To') == end_date)) |
                # Or dates fall within the contract period
                ((pl.col('From') >= start_date) & (pl.col('To') <= end_date))
            )
        )
        debit_amount = company_debits.select(pl.col('Amount').sum()).item()
        
        # Get ledger amounts for this company within the contract period
        company_ledger = ledger_result.filter(
            (pl.col('CompanyName') == company) &
            (pl.col('GLDate') >= start_date) &
            (pl.col('GLDate') <= end_date)
        )
        ledger_amount = company_ledger.select(pl.col('GLAmount').sum()).item()
        
        # Add to results
        results.append({
            'CompanyName': company,
            'DebitAmount': debit_amount,
            'LedgerAmount': ledger_amount,
            'Balance': ledger_amount - debit_amount
        })
    
    # Create final DataFrame
    result_df = pl.DataFrame(results)
    
    return result_df    

def get_data():
    """Get data and calculate base metrics"""
    try:
        # Initialize data if needed
        data = initialize_data()
        if not data:
            st.error("Failed to initialize data")
            return None, None
            
        # Get required dataframes
        PA = data.get('PA')
        CLAIMS = data.get('CLAIMS')
        PROVIDER = data.get('PROVIDER')
        ACTIVE_ENROLLEE = data.get('ACTIVE_ENROLLEE')
        GROUP_CONTRACT = data.get('GROUP_CONTRACT')
        GROUP_COVERAGE = data.get('GROUP_COVERAGE')
        
        # Check if all required dataframes exist
        required_frames = [PA, CLAIMS, PROVIDER, ACTIVE_ENROLLEE, GROUP_CONTRACT, GROUP_COVERAGE]
        if any(frame is None for frame in required_frames):
            st.error("One or more required dataframes are missing")
            return None, None
            
        # Calculate base metrics
        metrics = calculate_base_metrics(
            PA, CLAIMS, PROVIDER, ACTIVE_ENROLLEE, 
            GROUP_CONTRACT, GROUP_COVERAGE
        )
        
        # Store metrics in session state
        st.session_state.base_metrics = metrics
        
        return data, metrics
        
    except Exception as e:
        st.error(f"Error in get_data: {str(e)}")
        st.error(traceback.format_exc())
        return None, None

def convert_to_pandas_if_needed(df):
    """Convert Polars DataFrame to Pandas if needed"""
    try:
        if isinstance(df, pl.DataFrame):
            if df.height == 0:  # Check if DataFrame is empty
                return pd.DataFrame()
            return df.to_pandas()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame()
    except Exception as e:
        st.error(f"Error converting DataFrame: {str(e)}")
        return pd.DataFrame()

def calculate_average_costs_per_enrollee():
    """Calculate average costs per enrollee for the current year"""
    try:
        # Check if session state exists
        if 'data' not in st.session_state:
            st.error("No data found in session state")
            return {}
            
        # Check if required dataframes exist
        if 'PA' not in st.session_state.data:
            st.error("PA data not found")
            return {}
        if 'CLAIMS' not in st.session_state.data:
            st.error("CLAIMS data not found")
            return {}
        if 'ACTIVE_ENROLLEE' not in st.session_state.data:
            st.error("ACTIVE_ENROLLEE data not found")
            return {}
            
        # Get required dataframes and collect them
        PA = st.session_state.data['PA'].collect()
        CLAIMS = st.session_state.data['CLAIMS'].collect()
        ACTIVE_ENROLLEE = st.session_state.data['ACTIVE_ENROLLEE'].collect()
        
        # Get current year
        current_year = datetime.now().year
        
        # Calculate total number of active enrollees
        total_enrollees = ACTIVE_ENROLLEE.select(pl.col('legacycode').n_unique()).item()
        
        # Calculate total PA cost for current year
        current_year_pa = PA.filter(
            pl.col('requestdate').dt.year() == current_year
        ).select(pl.col('granted').sum()).item()
        
        # Calculate total claims cost for current year
        current_year_claims = CLAIMS.filter(
            pl.col('encounterdatefrom').dt.year() == current_year
        ).select(pl.col('approvedamount').sum()).item()
        
        # Calculate average costs per enrollee
        metrics = {
            'pa_cost_per_enrollee': current_year_pa / total_enrollees if total_enrollees > 0 else 0,
            'claims_cost_per_enrollee': current_year_claims / total_enrollees if total_enrollees > 0 else 0,
            'total_enrollees': total_enrollees,
            'total_pa_cost': current_year_pa,
            'total_claims_cost': current_year_claims
        }
        
        return metrics
        
    except Exception as e:
        st.error(f"Error calculating average costs per enrollee: {str(e)}")
        st.error(traceback.format_exc())
        return {}
