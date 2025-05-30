import streamlit as st
import polars as pl
from utils.data_loader import load_excel_data, benefit_limit

# Set page configuration
st.set_page_config(
    page_title="Benefit Analysis Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add title and description
st.title("Benefit Analysis Dashboard")
st.markdown("Analyze benefit utilization and identify customers exceeding their limits")

# Function to load data
@st.cache_data
def load_data():
    try:
        data = load_excel_data()
        
        # Collect LazyFrames before checking height
        for key, df in data.items():
            if isinstance(df, pl.LazyFrame):
                data[key] = df.collect()
                
        # Verify data is not empty
        for key, df in data.items():
            if df.height == 0:
                st.warning(f"Warning: Empty DataFrame for {key}")
        
        st.session_state.data = data
        GROUP_CONTRACT = st.session_state.data['GROUP_CONTRACT']
        PLAN_BENEFIT_LIMIT = st.session_state.data['PLAN_BENEFIT_LIMIT']
        BENEFITCBA = st.session_state.data['BENEFITCBA']

        # Rename columns and create BENEFIT DataFrame
        BENEFIT = BENEFITCBA.rename({
            "procedurecode": "Code",
            "benefitcodedesc": "Benefit"
        })
        
        # Normalize the Code column in BENEFIT
        BENEFIT = BENEFIT.with_columns(
            pl.col("Code").cast(pl.Utf8).str.to_uppercase().str.replace_all(" ", "").alias("Code")
        )

        BENEFIT_PA, claims_BENEFIT = benefit_limit()
        
        # Normalize code columns in other DataFrames
        BENEFIT_PA = BENEFIT_PA.with_columns(
            pl.col("code").cast(pl.Utf8).str.to_uppercase().str.replace_all(" ", "").alias("code")
        )
        
        claims_BENEFIT = claims_BENEFIT.with_columns(
            pl.col("code").cast(pl.Utf8).str.to_uppercase().str.replace_all(" ", "").alias("code")
        )
        
        st.session_state.BENEFIT_PA = BENEFIT_PA
        st.session_state.claims_BENEFIT = claims_BENEFIT

        return BENEFIT_PA, GROUP_CONTRACT, BENEFIT, PLAN_BENEFIT_LIMIT, claims_BENEFIT
    
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None, None, None, None, None

# Load data
BENEFIT_PA, GROUP_CONTRACT, BENEFIT, PLAN_BENEFIT_LIMIT, claims_BENEFIT = load_data()

# Check if data is loaded successfully
if all(df is not None for df in [BENEFIT_PA, GROUP_CONTRACT, BENEFIT, PLAN_BENEFIT_LIMIT, claims_BENEFIT]):
    # Get list of all group names
    all_groups = GROUP_CONTRACT["groupname"].unique().sort().to_list()
    
    # Function to calculate exceed amounts for all groups
    def calculate_all_groups_exceed():
        all_exceeded_amounts = {"PA": {}, "CLAIMS": {}}
        
        for source in ["PA", "CLAIMS"]:
            active_data = BENEFIT_PA if source == "PA" else claims_BENEFIT
            exceed_by_group = {}
            total_exceed = 0
            
            for group_name in all_groups:
                valid_contracts = GROUP_CONTRACT.filter(
                    pl.col("groupname") == group_name
                )
                
                if valid_contracts.height > 0:
                    contract_dates = valid_contracts.select(
                        pl.min("startdate").alias("min_startdate"),
                        pl.max("enddate").alias("max_enddate")
                    )
                    
                    min_date = contract_dates[0, "min_startdate"]
                    max_date = contract_dates[0, "max_enddate"]
                    
                    # Ensure we're using the correct columns for filtering
                    filtered_data = active_data.filter(
                        (pl.col("groupname") == group_name) & 
                        (pl.col("requestdate") >= min_date) & 
                        (pl.col("requestdate") <= max_date)
                    )
                    
                    if filtered_data.height > 0:
                        # Join with BENEFIT to get benefit information
                        joined_df = filtered_data.join(
                            BENEFIT,
                            left_on="code",
                            right_on="Code",
                            how="left"
                        )
                        
                        # Group by the correct columns
                        benefit_usage = joined_df.group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
                            pl.sum("granted").alias("total_granted")
                        )
                        
                        # Ensure plan_limits has the correct types
                        plan_limits = PLAN_BENEFIT_LIMIT.with_columns(
                            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
                        )
                        
                        # Join with plan limits
                        benefit_with_limits = benefit_usage.join(
                            plan_limits,
                            left_on=["planid", "benefitcodeid"],
                            right_on=["planid", "benefitcodeid"],
                            how="left"
                        ).with_columns(
                            pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
                        )
                        
                        # Calculate exceeded amounts
                        exceeded_amounts = benefit_with_limits.filter(
                            (pl.col("maxlimit") > 0) & 
                            (pl.col("total_granted") > pl.col("maxlimit"))
                        ).with_columns(
                            (pl.col("total_granted") - pl.col("maxlimit")).alias("exceed_amount")
                        )
                        
                        if exceeded_amounts.height > 0:
                            group_total_exceed = exceeded_amounts["exceed_amount"].sum()
                            exceed_by_group[group_name] = group_total_exceed
                            total_exceed += group_total_exceed
                        else:
                            exceed_by_group[group_name] = 0.0
            
            all_exceeded_amounts[source] = {
                "by_group": exceed_by_group,
                "total": total_exceed
            }
            
        return all_exceeded_amounts
    
    # Calculate exceed amounts for all groups
    all_groups_exceed = calculate_all_groups_exceed()
    
    # Create tabs
    tab1, tab2 = st.tabs(["All Groups Summary", "Exceeded Limits"])
    
    with tab1:
        st.header("Benefit Limits Exceeded - All Groups Summary")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("PA Data - Total Exceed Amount")
            st.metric("Total Amount Exceeded", f"₦{all_groups_exceed['PA']['total']:.2f}")
            
            st.subheader("Amount Exceeded by Group")
            exceed_data = [{"Group": group, "Amount": amount} 
                         for group, amount in all_groups_exceed['PA']['by_group'].items()]
            
            exceed_data = sorted(exceed_data, key=lambda x: x["Amount"], reverse=True)
            exceed_df = pl.DataFrame(exceed_data)
            
            if exceed_df.height > 0:
                exceed_df = exceed_df.with_columns(
                    pl.col("Amount").map_elements(lambda x: f"₦{x:.2f}").alias("Exceeded Amount")
                ).drop("Amount")
                st.dataframe(exceed_df, use_container_width=True)
            else:
                st.info("No exceeded amounts found in PA data")
        
        with col2:
            st.subheader("CLAIMS Data - Total Exceed Amount")
            st.metric("Total Amount Exceeded", f"₦{all_groups_exceed['CLAIMS']['total']:.2f}")
            
            st.subheader("Amount Exceeded by Group")
            exceed_data = [{"Group": group, "Amount": amount} 
                         for group, amount in all_groups_exceed['CLAIMS']['by_group'].items()]
            
            exceed_data = sorted(exceed_data, key=lambda x: x["Amount"], reverse=True)
            exceed_df = pl.DataFrame(exceed_data)
            
            if exceed_df.height > 0:
                exceed_df = exceed_df.with_columns(
                    pl.col("Amount").map_elements(lambda x: f"₦{x:.2f}").alias("Exceeded Amount")
                ).drop("Amount")
                st.dataframe(exceed_df, use_container_width=True)
            else:
                st.info("No exceeded amounts found in CLAIMS data")
                
        st.header("Combined Statistics")
        
        total_customers = set()
        total_plans = set()
        
        for group_name in all_groups:
            for data_source, data_df in [("PA", BENEFIT_PA), ("CLAIMS", claims_BENEFIT)]:
                group_data = data_df.filter(pl.col("groupname") == group_name)
                
                if group_data.height > 0:
                    customers = group_data["IID"].unique().to_list()
                    plans = group_data["planid"].unique().to_list()
                    total_customers.update(customers)
                    total_plans.update(plans)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Groups", len(all_groups))
        col2.metric("Total Plans", len(total_plans))
        col3.metric("Total Customers", len(total_customers))
    
    with tab2:
        st.header("Customers Exceeding Benefit Limits")
        st.markdown("This tab shows only customers who have exceeded or hit their benefit limits.")
        
        data_source_tab2 = st.radio("Select Data Source", ["PA", "CLAIMS", "Combined"], key="data_source_tab2")
        selected_group_tab2 = st.selectbox("Select Group Name", all_groups, key="group_tab2")
        
        valid_contracts = GROUP_CONTRACT.filter(
            pl.col("groupname") == selected_group_tab2
        )
        
        if valid_contracts.height == 0:
            st.warning(f"No contracts found for group '{selected_group_tab2}'")
        else:
            contract_dates = valid_contracts.select(
                pl.min("startdate").alias("min_startdate"),
                pl.max("enddate").alias("max_enddate")
            )
            
            min_date = contract_dates[0, "min_startdate"]
            max_date = contract_dates[0, "max_enddate"]
            
            st.info(f"Analyzing data from {min_date} to {max_date}")
            
            if data_source_tab2 == "Combined":
                # Filter both PA and Claims data for the selected group and date range
                filtered_pa = BENEFIT_PA.filter(
                    (pl.col("groupname") == selected_group_tab2) & 
                    (pl.col("requestdate") >= min_date) & 
                    (pl.col("requestdate") <= max_date)
                )
                
                filtered_claims = claims_BENEFIT.filter(
                    (pl.col("groupname") == selected_group_tab2) & 
                    (pl.col("requestdate") >= min_date) & 
                    (pl.col("requestdate") <= max_date)
                )
                
                # Join PA data with benefits
                pa_with_benefits = filtered_pa.join(
                    BENEFIT,
                    left_on="code",
                    right_on="Code",
                    how="left"
                )
                
                # Join Claims data with benefits
                claims_with_benefits = filtered_claims.join(
                    BENEFIT,
                    left_on="code",
                    right_on="Code",
                    how="left"
                )
                
                # Calculate usage for procedures in both PA and Claims (using Claims cost)
                common_procedures = pa_with_benefits.join(
                    claims_with_benefits.select(["IID", "code", "granted", "planid", "Benefit", "benefitcodeid"]),
                    on=["IID", "code"],
                    how="inner"
                ).group_by(["IID", "planid_right", "Benefit_right", "benefitcodeid_right"]).agg(
                    pl.sum("granted_right").alias("total_granted")
                ).rename({
                    "planid_right": "planid",
                    "Benefit_right": "Benefit",
                    "benefitcodeid_right": "benefitcodeid"
                })
                
                # Calculate usage for procedures only in Claims
                claims_only = claims_with_benefits.join(
                    pa_with_benefits.select(["IID", "code"]),
                    on=["IID", "code"],
                    how="anti"
                ).group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
                    pl.sum("granted").alias("total_granted")
                )
                
                # Calculate usage for procedures only in PA
                pa_only = pa_with_benefits.join(
                    claims_with_benefits.select(["IID", "code"]),
                    on=["IID", "code"],
                    how="anti"
                ).group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
                    pl.sum("granted").alias("total_granted")
                )
                
                # Combine all results
                benefit_usage = pl.concat([
                    common_procedures,
                    claims_only,
                    pa_only
                ]).group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
                    pl.sum("total_granted").alias("total_granted")
                )
                
                if benefit_usage.height == 0:
                    st.warning(f"No combined data available for the selected group")
                    st.stop()
            else:
                active_data = BENEFIT_PA if data_source_tab2 == "PA" else claims_BENEFIT
                
                filtered_data = active_data.filter(
                    (pl.col("groupname") == selected_group_tab2) & 
                    (pl.col("requestdate") >= min_date) & 
                    (pl.col("requestdate") <= max_date)
                )
                
                if filtered_data.height == 0:
                    st.warning(f"No {data_source_tab2} data available for the selected group")
                    st.stop()
                
                joined_df = filtered_data.join(
                    BENEFIT,
                    left_on="code",
                    right_on="Code",
                    how="left"
                )
                
                benefit_usage = joined_df.group_by(["IID", "planid", "Benefit", "benefitcodeid"]).agg(
                    pl.sum("granted").alias("total_granted")
                )
            
            # Ensure plan_limits has the correct types
            plan_limits = PLAN_BENEFIT_LIMIT.with_columns(
                pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
            )
            
            # Join with plan limits
            benefit_with_limits = benefit_usage.join(
                plan_limits,
                left_on=["planid", "benefitcodeid"],
                right_on=["planid", "benefitcodeid"],
                how="left"
            ).with_columns(
                pl.col("maxlimit").cast(pl.Float64, strict=False).fill_null(0)
            )
            
            # Calculate exceeded limits
            exceeded_limits = benefit_with_limits.filter(
                (pl.col("maxlimit") > 0) & 
                (pl.col("total_granted") >= pl.col("maxlimit"))
            ).with_columns(
                (pl.col("total_granted") - pl.col("maxlimit")).alias("exceeded_by")
            )
            
            if exceeded_limits.height == 0:
                st.info(f"No customers in {selected_group_tab2} have exceeded their benefit limits.")
            else:
                display_exceeded = exceeded_limits.select([
                    "IID", 
                    "planid", 
                    "Benefit", 
                    "total_granted", 
                    "maxlimit", 
                    "exceeded_by"
                ]).sort(by=["exceeded_by", "IID"], descending=[True, False])
                
                display_exceeded = display_exceeded.rename({
                    "IID": "Customer ID",
                    "planid": "Plan ID",
                    "Benefit": "Benefit Type",
                    "total_granted": "Total Used",
                    "maxlimit": "Limit Amount",
                    "exceeded_by": "Exceeded By"
                })
                
                display_exceeded = display_exceeded.with_columns([
                    pl.col("Total Used").map_elements(lambda x: f"₦{x:.2f}").alias("Total Used"),
                    pl.col("Limit Amount").map_elements(lambda x: f"₦{x:.2f}").alias("Limit Amount"),
                    pl.col("Exceeded By").map_elements(lambda x: f"₦{x:.2f}").alias("Exceeded By")
                ])
                
                total_customers = exceeded_limits["IID"].unique().len()
                total_plans = exceeded_limits["planid"].unique().len()
                total_benefits = exceeded_limits["Benefit"].unique().len()
                total_exceeded = exceeded_limits["exceeded_by"].sum()
                
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Customers Exceeding Limits", total_customers)
                col2.metric("Plans Affected", total_plans)
                col3.metric("Benefits Exceeded", total_benefits)
                col4.metric("Total Amount Exceeded", f"₦{total_exceeded:.2f}")
                
                st.subheader("Customers Who Have Exceeded Benefit Limits")
                st.dataframe(display_exceeded, use_container_width=True)
                
                @st.cache_data
                def convert_df_to_csv(_df):
                    return _df.to_pandas().to_csv(index=False).encode('utf-8')

                csv = convert_df_to_csv(display_exceeded)
                st.download_button(
                    label="Download Exceeded Limits Data as CSV",
                    data=csv,
                    file_name=f"exceeded_limits_{selected_group_tab2}_{data_source_tab2}.csv",
                    mime="text/csv",
                )
                
                with st.expander("Detailed Analysis"):
                    plan_stats = exceeded_limits.group_by("planid").agg([
                        pl.col("IID").unique().len().alias("customers_count"),
                        pl.col("Benefit").unique().len().alias("benefits_count"),
                        pl.col("exceeded_by").sum().alias("total_exceeded")
                    ]).sort(by="total_exceeded", descending=True)
                    
                    plan_stats = plan_stats.with_columns(
                        pl.col("total_exceeded").map_elements(lambda x: f"₦{x:.2f}").alias("total_exceeded")
                    )
                    
                    st.subheader("Exceeded Amounts by Plan")
                    st.dataframe(plan_stats, use_container_width=True)
                    
                    benefit_stats = exceeded_limits.group_by("Benefit").agg([
                        pl.col("IID").unique().len().alias("customers_count"),
                        pl.col("planid").unique().len().alias("plans_count"),
                        pl.col("exceeded_by").sum().alias("total_exceeded")
                    ]).sort(by="total_exceeded", descending=True)
                    
                    benefit_stats = benefit_stats.with_columns(
                        pl.col("total_exceeded").map_elements(lambda x: f"₦{x:.2f}").alias("total_exceeded")
                    )
                    
                    st.subheader("Most Frequently Exceeded Benefits")
                    st.dataframe(benefit_stats, use_container_width=True)
else:
    st.error("Failed to load data. Please check your data sources and try again.")

# Add a footer
st.sidebar.markdown("---")
st.sidebar.markdown("Dashboard created with Streamlit and Polars")