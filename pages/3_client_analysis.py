import streamlit as st
import polars as pl
import pandas as pd
from st_aggrid import AgGrid
import os
import io
import zipfile
from datetime import datetime, date
from utils.data_loader import client_analysis
from utils.requires_data_loading import requires_data_loading

@requires_data_loading

def filter_by_date_range(df, start_date, end_date, date_col="requestdate"):
    """Filter dataframe by date range"""
    # Create date expressions using the string format
    start = pl.lit(start_date)
    end = pl.lit(end_date)
    
    # Cast strings to dates for comparison
    return df.filter(
        (pl.col(date_col) >= start.cast(pl.Date)) & 
        (pl.col(date_col) <= end.cast(pl.Date))
    )

def generate_download_link(df, filename, file_format="csv"):
    """Generate a download link for a dataframe"""
    if file_format == "csv":
        output = io.BytesIO()
        df.write_csv(output)
        output.seek(0)
        return output.getvalue()
    elif file_format == "excel":
        output = io.BytesIO()
        if isinstance(df, pl.DataFrame):
            df.write_excel(output)
        else:  # For pandas DataFrames
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)
        output.seek(0)
        return output.getvalue()

def create_zip_from_dfs(dfs_dict):
    """Create a zip file containing all report files"""
    zip_buffer = io.BytesIO()
    
    with zipfile.ZipFile(zip_buffer, 'a', zipfile.ZIP_DEFLATED, False) as zip_file:
        for filename, df in dfs_dict.items():
            data = io.BytesIO()
            if isinstance(df, pl.DataFrame):
                df.write_excel(data)
            else:  # For pandas DataFrames
                with pd.ExcelWriter(data, engine='openpyxl') as writer:
                    df.to_excel(writer, index=False)
            data.seek(0)
            zip_file.writestr(f"{filename}.xlsx", data.getvalue())
    
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

def main():
    st.set_page_config(
        page_title="Health Insurance Analytics Report Generator",
        page_icon="📊",
        layout="wide"
    )
    
    st.title("Health Insurance Analytics Report Generator")
    st.markdown("Generate comprehensive health insurance analytics reports based on your data.")
    
    # Load data
    try:
        with st.spinner("Loading data..."):
            main_dff, ledger_result, debit, groupname_plan = client_analysis()
    
        st.success("Data loaded successfully!")
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None    
    
    # Sidebar for filters
    st.sidebar.header("Filter Options")
    
    # Get client names for selection
    client_names = main_dff.select("groupname").unique().to_series().sort()
    
    selected_client = st.sidebar.selectbox(
        "Select Client",
        options=client_names,
        index=0
    )
    
    col1, col2 = st.sidebar.columns(2)
    
    with col1:
        start_date = st.date_input("Main Start Date", value=datetime(2023, 1, 1))
    with col2:
        end_date = st.date_input("Main End Date", value=datetime(2023, 12, 31))
        
    st.sidebar.markdown("---")
    st.sidebar.subheader("Debit Analysis Date Range")
    
    with col1:
        debit_start_date = st.date_input("Debit Start Date", value=datetime(2023, 1, 1))
    with col2:
        debit_end_date = st.date_input("Debit End Date", value=datetime(2023, 12, 31))
    
    # Format dates as strings
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    debit_start_date_str = debit_start_date.strftime("%Y-%m-%d")
    debit_end_date_str = debit_end_date.strftime("%Y-%m-%d")
    
    # Generate Reports button
    if st.sidebar.button("Generate Reports", type="primary"):
        with st.spinner("Generating reports, please wait..."):
            # Filter data for main analysis
            filtered_df = main_dff.filter(pl.col("groupname") == selected_client)
            filtered_df = filter_by_date_range(filtered_df, start_date_str, end_date_str)
            
            # Filter ledger data for the same client and date range
            filtered_ledger = ledger_result.filter(pl.col("groupname") == selected_client)
            filtered_ledger = filter_by_date_range(filtered_ledger, start_date_str, end_date_str, "GLDate")
            
            # Filter premium data for the selected client (no date filter needed)
            filtered_premium = groupname_plan.filter(pl.col("groupname") == selected_client)
            
            # Filter debit data with separate date range
            filtered_debit = debit.filter(pl.col("groupname") == selected_client)
            filtered_debit = filter_by_date_range(filtered_debit, debit_start_date_str, debit_end_date_str, "From")
            
            # Add a progress bar
            progress_bar = st.progress(0)
            
            # Initialize reports dictionary
            reports = {}
            
            # 1. Top customers by cost
            top_customers = (filtered_df
                .group_by("IID")
                .agg(pl.col("granted_with_markup").sum().alias("total_cost"))
                .sort("total_cost", descending=True)
                .head(20))
            reports["top_customers"] = top_customers
            progress_bar.progress(10)

            # 2. Top plans by cost
            top_plans = (filtered_df
                .group_by("plancode")
                .agg(pl.col("granted_with_markup").sum().alias("total_cost"))
                .sort("total_cost", descending=True)
                .head(20))
            reports["top_plans"] = top_plans
            progress_bar.progress(20)
            
            # 3. Hospital analysis
            hospital_metrics = (filtered_df
                .group_by("providername")
                .agg([
                    pl.col("granted_with_markup").sum().alias("total_cost"),
                    pl.n_unique("requestdate").alias("visit_count"),
                    pl.n_unique("IID").alias("unique_patients"),
                    (pl.col("granted_with_markup").sum() / pl.n_unique("requestdate")).alias("avg_cost_per_visit")
                ])
                .sort("total_cost", descending=True))
            reports["hospital_analysis"] = hospital_metrics
            progress_bar.progress(30)
            
            # 4. Time analysis
            time_analysis = (filtered_df
                .with_columns(pl.col("requestdate").dt.strftime("%Y-%m").alias("month"))
                .group_by("month")
                .agg([
                    pl.col("granted_with_markup").sum().alias("total_cost"),
                    pl.col("granted_with_markup").mean().alias("avg_cost"),
                    pl.n_unique("IID").alias("unique_patients"),
                    pl.n_unique("requestdate").alias("visit_count")
                ])
                .sort("month"))
            reports["time_analysis"] = time_analysis
            progress_bar.progress(40)
            
            # 5. Benefit type analysis
            benefit_analysis = (filtered_df
                .group_by("Benefit")
                .agg([
                    pl.col("granted_with_markup").sum().alias("total_cost"),
                    pl.n_unique("IID").alias("unique_patients"),
                    pl.n_unique("requestdate").alias("visit_count")
                ])
                .sort("total_cost", descending=True))
            reports["benefit_analysis"] = benefit_analysis
            progress_bar.progress(50)
            
            # 6. Ledger Analysis
            ledger_total = filtered_ledger.select(pl.col("GLAmount").sum()).item()
            ledger_analysis = (filtered_ledger
                .with_columns(pl.col("GLDate").dt.strftime("%Y-%m").alias("month"))
                .group_by("month")
                .agg([
                    pl.col("GLAmount").sum().alias("total_amount"),
                    pl.n_unique("GLDesc").alias("transaction_count")
                ])
                .sort("month"))
            reports["ledger_analysis"] = ledger_analysis
            progress_bar.progress(60)
            
            # 7. Premium Analysis - Total premium
            total_premium = filtered_premium.select(pl.col("PREMIUM").sum()).item()
            
            # 8. Premium Analysis by Plan
            premium_by_plan = (filtered_premium
                .group_by(["planid"])
                .agg([
                    pl.col("PREMIUM").sum().alias("total_premium"),
                    pl.n_unique("groupid").alias("group_count"),
                    pl.sum("countofindividual").alias("total_individuals"),
                    pl.sum("countoffamily").alias("total_families")
                ])
                .sort("total_premium", descending=True))
            reports["premium_by_plan"] = premium_by_plan
            progress_bar.progress(70)
            
            # 9. Debit Analysis
            debit_total = filtered_debit.select(pl.col("Amount").sum()).item()
            debit_analysis = (filtered_debit
                .with_columns(pl.col("From").dt.strftime("%Y-%m").alias("month"))
                .group_by("month")
                .agg([
                    pl.col("Amount").sum().alias("total_amount")
                ])
                .sort("month"))
            reports["debit_analysis"] = debit_analysis
            progress_bar.progress(80)
            
            # 10. Enhanced Summary metrics
            summary_metrics = {
                "Metric": [
                    "Total Cost",
                    "Unique Customers",
                    "Unique Hospitals",
                    "Total Visits",
                    "Average Cost per Visit",
                    "Total number of plan",
                    "Total Ledger Amount",
                    "Total Premium",
                    "Total Debit Amount",
                    "Debit Period"
                ],
                "Value": [
                    filtered_df.select(pl.col("granted_with_markup").sum()).item(),
                    filtered_df.select(pl.n_unique("IID")).item(),
                    filtered_df.select(pl.n_unique("providername")).item(),
                    filtered_df.select(pl.n_unique("requestdate")).item(),
                    filtered_df.select(pl.col("granted_with_markup").mean()).item(),
                    filtered_df.select(pl.n_unique("plancode")).item(),
                    ledger_total,
                    total_premium,
                    debit_total,
                    f"{debit_start_date_str} to {debit_end_date_str}"
                ]
            }
            summary_metrics_df = pd.DataFrame(summary_metrics)
            reports["summary_metrics"] = summary_metrics_df
            progress_bar.progress(90)
            
            # 11. Financial Overview - Combined view of all financial metrics
            financial_overview = {
                "Metric": [
                    "Total Claims Cost", 
                    "Total Ledger Amount", 
                    "Total Premium", 
                    "Total Debit Amount",
                    "Premium - Claims Difference",
                    "Date Range (Main)",
                    "Date Range (Debit)"
                ],
                "Amount": [
                    filtered_df.select(pl.col("granted_with_markup").sum()).item(),
                    ledger_total,
                    total_premium,
                    debit_total,
                    total_premium - filtered_df.select(pl.col("granted_with_markup").sum()).item(),
                    f"{start_date_str} to {end_date_str}",
                    f"{debit_start_date_str} to {debit_end_date_str}"
                ]
            }
            financial_overview_df = pd.DataFrame(financial_overview)
            reports["financial_overview"] = financial_overview_df
            
            # Raw datasets
            reports["filtered_raw_data"] = filtered_df
            reports["filtered_ledger_data"] = filtered_ledger
            reports["filtered_premium_data"] = filtered_premium
            reports["filtered_debit_data"] = filtered_debit
            
            progress_bar.progress(100)
            
            # Create tabs to display results
            tabs = st.tabs([
                "Summary", 
                "Top Customers", 
                "Top Plans", 
                "Hospital Analysis",
                "Time Analysis",
                "Benefit Analysis",
                "Ledger Analysis",
                "Premium Analysis",
                "Debit Analysis",
                "Financial Overview",
                "Raw Data"
            ])
            
            # Summary Tab
            with tabs[0]:
                st.subheader("Summary Metrics")
                st.dataframe(summary_metrics_df, use_container_width=True)
                
                st.download_button(
                    label="Download Summary Metrics",
                    data=generate_download_link(summary_metrics_df, "summary_metrics", "excel"),
                    file_name="summary_metrics.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Top Customers Tab
            with tabs[1]:
                st.subheader("Top Customers by Cost")
                st.dataframe(top_customers, use_container_width=True)
                
                st.download_button(
                    label="Download Top Customers",
                    data=generate_download_link(top_customers, "top_customers", "excel"),
                    file_name="top_customers.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Top Plans Tab
            with tabs[2]:
                st.subheader("Top Plans by Cost")
                st.dataframe(top_plans, use_container_width=True)
                
                st.download_button(
                    label="Download Top Plans",
                    data=generate_download_link(top_plans, "top_plans", "excel"),
                    file_name="top_plans.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Hospital Analysis Tab
            with tabs[3]:
                st.subheader("Hospital Analysis")
                st.dataframe(hospital_metrics, use_container_width=True)
                
                st.download_button(
                    label="Download Hospital Analysis",
                    data=generate_download_link(hospital_metrics, "hospital_analysis", "excel"),
                    file_name="hospital_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Time Analysis Tab
            with tabs[4]:
                st.subheader("Time Analysis")
                st.dataframe(time_analysis, use_container_width=True)
                
                st.download_button(
                    label="Download Time Analysis",
                    data=generate_download_link(time_analysis, "time_analysis", "excel"),
                    file_name="time_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Benefit Analysis Tab
            with tabs[5]:
                st.subheader("Benefit Analysis")
                st.dataframe(benefit_analysis, use_container_width=True)
                
                st.download_button(
                    label="Download Benefit Analysis",
                    data=generate_download_link(benefit_analysis, "benefit_analysis", "excel"),
                    file_name="benefit_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Ledger Analysis Tab
            with tabs[6]:
                st.subheader("Ledger Analysis")
                st.dataframe(ledger_analysis, use_container_width=True)
                
                st.download_button(
                    label="Download Ledger Analysis",
                    data=generate_download_link(ledger_analysis, "ledger_analysis", "excel"),
                    file_name="ledger_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Premium Analysis Tab
            with tabs[7]:
                st.subheader("Premium Analysis by Plan")
                st.dataframe(premium_by_plan, use_container_width=True)
                
                st.download_button(
                    label="Download Premium Analysis",
                    data=generate_download_link(premium_by_plan, "premium_by_plan", "excel"),
                    file_name="premium_by_plan.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Debit Analysis Tab
            with tabs[8]:
                st.subheader("Debit Analysis")
                st.dataframe(debit_analysis, use_container_width=True)
                
                st.download_button(
                    label="Download Debit Analysis",
                    data=generate_download_link(debit_analysis, "debit_analysis", "excel"),
                    file_name="debit_analysis.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Financial Overview Tab
            with tabs[9]:
                st.subheader("Financial Overview")
                st.dataframe(financial_overview_df, use_container_width=True)
                
                st.download_button(
                    label="Download Financial Overview",
                    data=generate_download_link(financial_overview_df, "financial_overview", "excel"),
                    file_name="financial_overview.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
            
            # Raw Data Tab
            with tabs[10]:
                raw_data_tabs = st.tabs(["Claims Data", "Ledger Data", "Premium Data", "Debit Data"])
                
                with raw_data_tabs[0]:
                    st.subheader("Filtered Claims Data")
                    st.dataframe(filtered_df.head(1000), use_container_width=True)
                    st.caption(f"Showing first 1000 rows of {filtered_df.height} total rows")
                    
                    st.download_button(
                        label="Download Full Claims Data",
                        data=generate_download_link(filtered_df, "filtered_raw_data", "excel"),
                        file_name="filtered_raw_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with raw_data_tabs[1]:
                    st.subheader("Filtered Ledger Data")
                    st.dataframe(filtered_ledger.head(1000), use_container_width=True)
                    st.caption(f"Showing first 1000 rows of {filtered_ledger.height} total rows")
                    
                    st.download_button(
                        label="Download Full Ledger Data",
                        data=generate_download_link(filtered_ledger, "filtered_ledger_data", "excel"),
                        file_name="filtered_ledger_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with raw_data_tabs[2]:
                    st.subheader("Filtered Premium Data")
                    st.dataframe(filtered_premium.head(1000), use_container_width=True)
                    st.caption(f"Showing first 1000 rows of {filtered_premium.height} total rows")
                    
                    st.download_button(
                        label="Download Full Premium Data",
                        data=generate_download_link(filtered_premium, "filtered_premium_data", "excel"),
                        file_name="filtered_premium_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
                
                with raw_data_tabs[3]:
                    st.subheader("Filtered Debit Data")
                    st.dataframe(filtered_debit.head(1000), use_container_width=True)
                    st.caption(f"Showing first 1000 rows of {filtered_debit.height} total rows")
                    
                    st.download_button(
                        label="Download Full Debit Data",
                        data=generate_download_link(filtered_debit, "filtered_debit_data", "excel"),
                        file_name="filtered_debit_data.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
            
            # Download all reports as zip
            st.sidebar.markdown("---")
            st.sidebar.header("Download All Reports")
            
            # Create timestamp for the zip file
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            st.sidebar.download_button(
                label="Download All Reports (ZIP)",
                data=create_zip_from_dfs(reports),
                file_name=f"reports_{selected_client}_{timestamp}.zip",
                mime="application/zip"
            )

    else:
        # Display initial instructions
        st.info("👈 Please select a client and date range, then click 'Generate Reports' to begin.")
        
        # Display data overview
        st.subheader("Data Overview")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("Total Clients", main_dff.select(pl.n_unique("groupname")).item())
        
        with col2:
            st.metric("Total Claims", main_dff.height)
        
        with col3:
            st.metric("Total Cost", f"₦{main_dff.select(pl.sum('granted_with_markup')).item():,.2f}")

if __name__ == "__main__":
    main()