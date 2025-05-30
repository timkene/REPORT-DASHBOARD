import streamlit as st
import polars as pl
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from utils.data_loader import pharmacy_carecord, revenue_pa
from datetime import datetime
from statsmodels.tsa.arima.model import ARIMA
import warnings
from prophet import Prophet
from prophet.plot import plot_plotly
warnings.filterwarnings('ignore')
from utils.requires_data_loading import requires_data_loading


# Set page configuration
st.set_page_config(
    page_title="PA and Claims Cost Analysis",
    layout="wide"
)


# Load data from session
try:
    pa_benefit, all_pa, all_claims = pharmacy_carecord()
    st.session_state.pa_benefit = pa_benefit
    st.session_state.all_pa = all_pa
    st.session_state.all_claims = all_claims
    
    pa_data = all_pa  # Adjust according to your actual function signature
    claims_data =all_claims # Adjust according to your actual function signature

    # Get revenue metrics
    metrics = revenue_pa()
    if not metrics:
        st.error("Unable to load revenue metrics")
        st.stop()
        
    GLT = metrics['current_month']['General Ledger']  # It is already a DataFrame
    GLT = (
        GLT
        .with_columns(
            pl.col("GLAmount").cast(pl.Float64, strict=False).abs().alias("cost")
        )
        .filter(
            (pl.col("CompanyName").is_not_null()) & (pl.col("CompanyName") != "")
        )
        .rename({"GLDate": "date"})
    )
    
    # Extract cash received data
    Cash_keywords = ["POLARIS BANK 1", "KEYSTONE BANK", "MICROCRED MFB", "ASTRAPOLARIS MFB", "ALERT MICRO BANK", "MONEYFIELD MFB", "KAYVEE  MFB"]
    cash_received = GLT.filter(pl.col('DESCRIPTION').is_in(Cash_keywords))
except Exception as e:
    st.error(f"Error loading data: {e}")
    st.stop()


# Function to extract year and month from date
def prepare_monthly_data(df):
    df = df.with_columns([
        pl.col("date").dt.year().alias("year"),
        pl.col("date").dt.month().alias("month")
    ])
    return df

# Prepare data
pa_data = prepare_monthly_data(pa_data)
claims_data = prepare_monthly_data(claims_data)
cash_received = prepare_monthly_data(cash_received)

# Filter for relevant years
pa_data_filtered = pa_data.filter((pl.col("year") == 2022) | (pl.col("year") == 2023) | (pl.col("year") == 2024) | (pl.col("year") == 2025))
claims_data_filtered = claims_data.filter((pl.col("year") == 2022) | (pl.col("year") == 2023) | (pl.col("year") == 2024) | (pl.col("year") == 2025))
cash_received_filtered = cash_received.filter((pl.col("year") == 2024) | (pl.col("year") == 2025))

def convert_lazyframe_to_df(df):
    """Convert LazyFrame to DataFrame if needed"""
    if isinstance(df, pl.LazyFrame):
        return df.collect()
    elif isinstance(df, pl.DataFrame):
        return df
    elif isinstance(df, pd.DataFrame):
        return df
    elif hasattr(df, 'data'):  # Handle pandas Styler objects
        return df.data
    else:
        raise TypeError(f"Expected LazyFrame, DataFrame, pandas DataFrame, or Styler, got {type(df)}")

def display_dataframe(df, **kwargs):
    """Safely display a DataFrame in Streamlit"""
    try:
        if hasattr(df, 'style'):  # Check if object has style attribute (pandas Styler)
            st.dataframe(df, **kwargs)
        elif isinstance(df, pd.DataFrame):
            st.dataframe(df, **kwargs)
        else:
            df = convert_lazyframe_to_df(df)
            st.dataframe(df.to_pandas(), **kwargs)
    except Exception as e:
        st.error(f"Error displaying dataframe: {str(e)}")
        # Fallback to basic display
        if isinstance(df, pd.DataFrame):
            st.dataframe(df, **kwargs)
        elif hasattr(df, 'data'):  # Handle pandas Styler objects
            st.dataframe(df.data, **kwargs)
        else:
            df = convert_lazyframe_to_df(df)
            st.dataframe(df.to_pandas(), **kwargs)

# Ensure all input data is collected
pa_data_filtered = convert_lazyframe_to_df(pa_data_filtered)
claims_data_filtered = convert_lazyframe_to_df(claims_data_filtered)
cash_received_filtered = convert_lazyframe_to_df(cash_received_filtered)

# Aggregate monthly costs
pa_monthly = pa_data_filtered.group_by(["year", "month"]).agg(
    pl.sum("cost").alias("total_cost"),
    pl.n_unique("panumber").alias("unique_panumber")
)

claims_monthly = claims_data_filtered.group_by(["year", "month"]).agg(
    pl.sum("cost").alias("total_cost")
)

cash_monthly = cash_received_filtered.group_by(["year", "month"]).agg(
    pl.sum("cost").alias("total_cost")
)

# Convert to pandas for easier plotting with plotly
pa_monthly_pd = pa_monthly.to_pandas()
claims_monthly_pd = claims_monthly.to_pandas()
cash_monthly_pd = cash_monthly.to_pandas()

# Aggregate by benefit
pa_benefit_monthly = pa_data_filtered.group_by(["year", "month", "Benefit"]).agg(
    pl.sum("cost").alias("total_cost"),
    pl.n_unique("panumber").alias("unique_panumber")
)

claims_benefit_monthly = claims_data_filtered.group_by(["year", "month", "Benefit"]).agg(
    pl.sum("cost").alias("total_cost")
)

# Convert to pandas for easier plotting with plotly
pa_benefit_monthly_pd = pa_benefit_monthly.to_pandas()
claims_benefit_monthly_pd = claims_benefit_monthly.to_pandas()

# Create month names for x-axis
month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

# Add month names to dataframes
pa_monthly_pd["month_name"] = pa_monthly_pd["month"].apply(lambda x: month_names[x-1])
claims_monthly_pd["month_name"] = claims_monthly_pd["month"].apply(lambda x: month_names[x-1])
cash_monthly_pd["month_name"] = cash_monthly_pd["month"].apply(lambda x: month_names[x-1])

# Streamlit app
st.title("PA and Claims Cost Analysis")

# Create tabs for different visualizations
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Cost Comparison", "Percentage Difference", "Unique PA Numbers", "Cash Received", "Prophet forecast"])

with tab1:
    st.header("PA and Claims Cost Analysis (2022 vs 2023 vs 2024 vs 2025)")
    
    # Create plotly figure for overall cost comparison
    fig = go.Figure()
    
    # Add PA 2022 data
    pa_2022 = pa_monthly_pd[pa_monthly_pd["year"] == 2022].sort_values("month")
    if not pa_2022.empty:
        fig.add_trace(go.Scatter(
            x=pa_2022["month_name"], 
            y=pa_2022["total_cost"],
            mode='lines+markers',
            name='PA 2022',
            line=dict(color='brown', width=2)
        ))


    # Add PA 2023 data
    pa_2023 = pa_monthly_pd[pa_monthly_pd["year"] == 2023].sort_values("month")
    if not pa_2023.empty:
        fig.add_trace(go.Scatter(
            x=pa_2023["month_name"], 
            y=pa_2023["total_cost"],
            mode='lines+markers',
            name='PA 2023',
            line=dict(color='green', width=2)
        ))

    # Add PA 2024 data
    pa_2024 = pa_monthly_pd[pa_monthly_pd["year"] == 2024].sort_values("month")
    if not pa_2024.empty:
        fig.add_trace(go.Scatter(
            x=pa_2024["month_name"], 
            y=pa_2024["total_cost"],
            mode='lines+markers',
            name='PA 2024',
            line=dict(color='blue', width=2)
        ))
    
    # Add PA 2025 data
    pa_2025 = pa_monthly_pd[pa_monthly_pd["year"] == 2025].sort_values("month")
    if not pa_2025.empty:
        fig.add_trace(go.Scatter(
            x=pa_2025["month_name"], 
            y=pa_2025["total_cost"],
            mode='lines+markers',
            name='PA 2025',
            line=dict(color='royalblue', width=2, dash='dash')
        ))

    # Add Claims 2022 data
    claims_2022 = claims_monthly_pd[claims_monthly_pd["year"] == 2022].sort_values("month")
    if not claims_2022.empty:
        fig.add_trace(go.Scatter(
            x=claims_2022["month_name"], 
            y=claims_2022["total_cost"],
            mode='lines+markers',
            name='Claims 2022',
            line=dict(color='black', width=2)
        ))

    # Add Claims 2023 data
    claims_2023 = claims_monthly_pd[claims_monthly_pd["year"] == 2023].sort_values("month")
    if not claims_2023.empty:
        fig.add_trace(go.Scatter(
            x=claims_2023["month_name"], 
            y=claims_2023["total_cost"],
            mode='lines+markers',
            name='Claims 2023',
            line=dict(color='purple', width=2)
        ))        
    
    # Add Claims 2024 data
    claims_2024 = claims_monthly_pd[claims_monthly_pd["year"] == 2024].sort_values("month")
    if not claims_2024.empty:
        fig.add_trace(go.Scatter(
            x=claims_2024["month_name"], 
            y=claims_2024["total_cost"],
            mode='lines+markers',
            name='Claims 2024',
            line=dict(color='red', width=2)
        ))
    
    # Add Claims 2025 data
    claims_2025 = claims_monthly_pd[claims_monthly_pd["year"] == 2025].sort_values("month")
    if not claims_2025.empty:
        fig.add_trace(go.Scatter(
            x=claims_2025["month_name"], 
            y=claims_2025["total_cost"],
            mode='lines+markers',
            name='Claims 2025',
            line=dict(color='salmon', width=2, dash='dash')
        ))
    
    # Update layout
    fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Total Cost",
        legend_title="Data Series",
        template="plotly_white",
        height=600,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(fig, use_container_width=True)

# Add a separator between charts
    st.markdown("---")
    
    # Prepare data for benefit comparison by month
    pa_benefit_monthly = pa_data_filtered.group_by(["year", "month", "Benefit"]).agg(
        pl.sum("cost").alias("total_cost")
    ).to_pandas()
    
    claims_benefit_monthly = claims_data_filtered.group_by(["year", "month", "Benefit"]).agg(
        pl.sum("cost").alias("total_cost")
    ).to_pandas()
    
    # Add month names to dataframes
    pa_benefit_monthly["month_name"] = pa_benefit_monthly["month"].apply(lambda x: month_names[x-1])
    claims_benefit_monthly["month_name"] = claims_benefit_monthly["month"].apply(lambda x: month_names[x-1])
    
    # Get available years for selectbox
    available_years = sorted(pa_benefit_monthly["year"].unique(), reverse=True)
    default_year = 2025 if 2025 in available_years else available_years[0]
    
    # Add year selector
    st.subheader("Benefit Cost Comparison - Monthly Trends")
    selected_year = st.selectbox(
        "Select Year for Benefit Comparison Charts", 
        options=available_years,
        index=available_years.index(default_year) if default_year in available_years else 0
    )
    
    # Display PA benefit chart first
    st.subheader(f"PA Benefit Monthly Trends ({selected_year})")
    
    # Filter data for selected year
    pa_year_data = pa_benefit_monthly[pa_benefit_monthly["year"] == selected_year]
    
    # Sort data by month for proper timeline
    pa_year_data = pa_year_data.sort_values("month")
    
    # Get top benefits by total cost
    top_benefits = pa_year_data.groupby("Benefit")["total_cost"].sum().nlargest(10).index.tolist()
    
    # Create plotly figure for PA benefit comparison
    pa_benefit_fig = go.Figure()
    
    # Add trace for each benefit
    for benefit in top_benefits:
        benefit_data = pa_year_data[pa_year_data["Benefit"] == benefit]
        
        pa_benefit_fig.add_trace(go.Scatter(
            x=benefit_data["month_name"],
            y=benefit_data["total_cost"],
            mode='lines+markers',
            name=benefit
        ))
    
    # Update layout
    pa_benefit_fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Total Cost",
        legend_title="Benefit Type",
        template="plotly_white",
        height=500,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(pa_benefit_fig, use_container_width=True)
    
    # Add some space between charts
    st.markdown("---")
    
    # Display Claims benefit chart second
    st.subheader(f"Claims Benefit Monthly Trends ({selected_year})")
    
    # Filter data for selected year
    claims_year_data = claims_benefit_monthly[claims_benefit_monthly["year"] == selected_year]
    
    # Sort data by month for proper timeline
    claims_year_data = claims_year_data.sort_values("month")
    
    # Get top benefits by total cost
    top_benefits = claims_year_data.groupby("Benefit")["total_cost"].sum().nlargest(10).index.tolist()
    
    # Create plotly figure for Claims benefit comparison
    claims_benefit_fig = go.Figure()
    
    # Add trace for each benefit
    for benefit in top_benefits:
        benefit_data = claims_year_data[claims_year_data["Benefit"] == benefit]
        
        claims_benefit_fig.add_trace(go.Scatter(
            x=benefit_data["month_name"],
            y=benefit_data["total_cost"],
            mode='lines+markers',
            name=benefit
        ))
    
    # Update layout
    claims_benefit_fig.update_layout(
        xaxis_title="Month",
        yaxis_title="Total Cost",
        legend_title="Benefit Type",
        template="plotly_white",
        height=500,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(claims_benefit_fig, use_container_width=True)
    
    # Add top procedures analysis
    st.subheader("Top Procedures Analysis")
    
    # Add tabs for different years
    year_tabs = st.tabs(["2024 Data", "2025 YTD Data", "Monthly Trends"])
    
    with year_tabs[0]:  # 2024 Data
        st.markdown("### Top 30 Procedures in 2024")
        
        # Filter for 2024 data
        pa_2024_full = pa_data.filter(pl.col("year") == 2024)
        claims_2024_full = claims_data.filter(pl.col("year") == 2024)
        
        # Create columns for PA and Claims
        pa_col, claims_col = st.columns(2)
        
        with pa_col:
            st.markdown("#### PA Data")
            
            # Top 30 by cost for PA
            top_pa_by_cost = (pa_2024_full
                .group_by("code")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("code").alias("procedure_count")
                )
                .sort("total_cost", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 PA Procedures by Cost**")
            display_dataframe(top_pa_by_cost, use_container_width=True)
            
            # Top 30 by count for PA
            top_pa_by_count = (pa_2024_full
                .group_by("code")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("code").alias("procedure_count")
                )
                .sort("procedure_count", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 PA Procedures by Count**")
            display_dataframe(top_pa_by_count, use_container_width=True)
        
        with claims_col:
            st.markdown("#### Claims Data")
            
            # Top 30 by cost for Claims
            top_claims_by_cost = (claims_2024_full
                .group_by("procedurecode")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("procedurecode").alias("procedure_count")
                )
                .sort("total_cost", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 Claims Procedures by Cost**")
            display_dataframe(top_claims_by_cost, use_container_width=True)
            
            # Top 30 by count for Claims
            top_claims_by_count = (claims_2024_full
                .group_by("procedurecode")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("procedurecode").alias("procedure_count")
                )
                .sort("procedure_count", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 Claims Procedures by Count**")
            display_dataframe(top_claims_by_count, use_container_width=True)
    
    with year_tabs[1]:  # 2025 YTD Data
        st.markdown("### Top 30 Procedures in 2025 (Year to Date)")
        
        # Filter for 2025 data
        pa_2025_full = pa_data.filter(pl.col("year") == 2025)
        claims_2025_full = claims_data.filter(pl.col("year") == 2025)
        
        # Create columns for PA and Claims
        pa_col, claims_col = st.columns(2)
        
        with pa_col:
            st.markdown("#### PA Data")
            
            # Top 30 by cost for PA
            top_pa_by_cost_2025 = (pa_2025_full
                .group_by("code")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("code").alias("procedure_count")
                )
                .sort("total_cost", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 PA Procedures by Cost (2025 YTD)**")
            display_dataframe(top_pa_by_cost_2025, use_container_width=True)
            
            # Top 30 by count for PA
            top_pa_by_count_2025 = (pa_2025_full
                .group_by("code")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("code").alias("procedure_count")
                )
                .sort("procedure_count", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 PA Procedures by Count (2025 YTD)**")
            display_dataframe(top_pa_by_count_2025, use_container_width=True)
        
        with claims_col:
            st.markdown("#### Claims Data")
            
            # Top 30 by cost for Claims
            top_claims_by_cost_2025 = (claims_2025_full
                .group_by("procedurecode")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("procedurecode").alias("procedure_count")
                )
                .sort("total_cost", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 Claims Procedures by Cost (2025 YTD)**")
            display_dataframe(top_claims_by_cost_2025, use_container_width=True)
            
            # Top 30 by count for Claims
            top_claims_by_count_2025 = (claims_2025_full
                .group_by("procedurecode")
                .agg(
                    pl.sum("cost").alias("total_cost"),
                    pl.count("procedurecode").alias("procedure_count")
                )
                .sort("procedure_count", descending=True)
                .limit(30)
            )
            
            st.markdown("**Top 30 Claims Procedures by Count (2025 YTD)**")
            display_dataframe(top_claims_by_count_2025, use_container_width=True)
    
    with year_tabs[2]:  # Monthly Trends
        st.markdown("### Monthly Trends for Top 10 Procedures")
        
        # Get top 10 procedure codes by cost from 2025
        pa_2025_full = convert_lazyframe_to_df(pa_2025_full)
        top_10_pa_codes = pa_2025_full.group_by("code").agg(
            pl.sum("cost").alias("total_cost")
        ).sort("total_cost", descending=True).head(10).get_column("code").to_list()

        # Get top 10 claims codes by cost from 2025
        claims_2025_full = convert_lazyframe_to_df(claims_2025_full)
        top_10_claims_codes = claims_2025_full.group_by("procedurecode").agg(
            pl.sum("cost").alias("total_cost")
        ).sort("total_cost", descending=True).head(10).get_column("procedurecode").to_list()
        
        # Create tabs for PA and Claims data
        pa_tab, claims_tab = st.tabs(["PA Monthly Trends", "Claims Monthly Trends"])
        
        with pa_tab:
            # 2025 Monthly trends
            st.markdown("#### 2025 Monthly Trends - Top 10 PA Procedures")
            
            # Get monthly data for top 10 procedures for 2025
            pa_monthly_top10_2025 = (pa_2025_full
                .filter(pl.col("code").is_in(top_10_pa_codes))
                .group_by(["month", "code"])
                .agg(pl.sum("cost").alias("total_cost"))
                .sort(["month", "code"])
            ).to_pandas()
            
            # Add month names
            pa_monthly_top10_2025["month_name"] = pa_monthly_top10_2025["month"].apply(lambda x: month_names[x-1])
            
            # Create line chart for 2025
            fig_pa_2025 = go.Figure()
            
            for proc_code in top_10_pa_codes:
                proc_data = pa_monthly_top10_2025[pa_monthly_top10_2025["code"] == proc_code]
                if not proc_data.empty:
                    fig_pa_2025.add_trace(go.Scatter(
                        x=proc_data["month_name"],
                        y=proc_data["total_cost"],
                        mode='lines+markers',
                        name=f'Code: {proc_code}',
                    ))
            
            fig_pa_2025.update_layout(
                title="Top 10 PA Procedures Monthly Trends (2025)",
                xaxis_title="Month",
                yaxis_title="Total Cost",
                legend_title="Procedure Code",
                template="plotly_white",
                height=500,
                xaxis=dict(
                    tickmode='array',
                    tickvals=month_names[:len(set(pa_monthly_top10_2025["month_name"]))]
                )
            )
            
            st.plotly_chart(fig_pa_2025, use_container_width=True)
            
            # 2024 Monthly trends
            st.markdown("#### 2024 Monthly Trends - Top 10 PA Procedures")
            
            # Get monthly data for top 10 procedures for 2024
            pa_2024_full = convert_lazyframe_to_df(pa_2024_full)
            pa_monthly_top10_2024 = (pa_2024_full
                .filter(pl.col("code").is_in(top_10_pa_codes))
                .group_by(["month", "code"])
                .agg(pl.sum("cost").alias("total_cost"))
                .sort(["month", "code"])
            ).to_pandas()
            
            # Add month names
            pa_monthly_top10_2024["month_name"] = pa_monthly_top10_2024["month"].apply(lambda x: month_names[x-1])
            
            # Create line chart for 2024
            fig_pa_2024 = go.Figure()
            
            for proc_code in top_10_pa_codes:
                proc_data = pa_monthly_top10_2024[pa_monthly_top10_2024["code"] == proc_code]
                if not proc_data.empty:
                    fig_pa_2024.add_trace(go.Scatter(
                        x=proc_data["month_name"],
                        y=proc_data["total_cost"],
                        mode='lines+markers',
                        name=f'Code: {proc_code}',
                    ))
            
            fig_pa_2024.update_layout(
                title="Top 10 PA Procedures Monthly Trends (2024)",
                xaxis_title="Month",
                yaxis_title="Total Cost",
                legend_title="Procedure Code",
                template="plotly_white",
                height=500,
                xaxis=dict(
                    tickmode='array',
                    tickvals=month_names
                )
            )
            
            st.plotly_chart(fig_pa_2024, use_container_width=True)

        with claims_tab:
            # 2025 Monthly trends
            st.markdown("#### 2025 Monthly Trends - Top 10 Claims Procedures")
            
            # Get monthly data for top 10 procedures for 2025
            claims_monthly_top10_2025 = (claims_2025_full
                .filter(pl.col("procedurecode").is_in(top_10_claims_codes))
                .group_by(["month", "procedurecode"])
                .agg(pl.sum("cost").alias("total_cost"))
                .sort(["month", "procedurecode"])
            ).to_pandas()
            
            # Add month names
            claims_monthly_top10_2025["month_name"] = claims_monthly_top10_2025["month"].apply(lambda x: month_names[x-1])
            
            # Create line chart for 2025
            fig_claims_2025 = go.Figure()
            
            for proc_code in top_10_claims_codes:
                proc_data = claims_monthly_top10_2025[claims_monthly_top10_2025["procedurecode"] == proc_code]
                if not proc_data.empty:
                    fig_claims_2025.add_trace(go.Scatter(
                        x=proc_data["month_name"],
                        y=proc_data["total_cost"],
                        mode='lines+markers',
                        name=f'Code: {proc_code}',
                    ))
            
            fig_claims_2025.update_layout(
                title="Top 10 Claims Procedures Monthly Trends (2025)",
                xaxis_title="Month",
                yaxis_title="Total Cost",
                legend_title="Procedure Code",
                template="plotly_white",
                height=500,
                xaxis=dict(
                    tickmode='array',
                    tickvals=month_names[:len(set(claims_monthly_top10_2025["month_name"]))]
                )
            )
            
            st.plotly_chart(fig_claims_2025, use_container_width=True)
            
            # 2024 Monthly trends
            st.markdown("#### 2024 Monthly Trends - Top 10 Claims Procedures")
            
            # Get monthly data for top 10 procedures for 2024
            claims_2024_full = convert_lazyframe_to_df(claims_2024_full)
            claims_monthly_top10_2024 = (claims_2024_full
                .filter(pl.col("procedurecode").is_in(top_10_claims_codes))
                .group_by(["month", "procedurecode"])
                .agg(pl.sum("cost").alias("total_cost"))
                .sort(["month", "procedurecode"])
            ).to_pandas()
            
            # Add month names
            claims_monthly_top10_2024["month_name"] = claims_monthly_top10_2024["month"].apply(lambda x: month_names[x-1])
            
            # Create line chart for 2024
            fig_claims_2024 = go.Figure()
            
            for proc_code in top_10_claims_codes:
                proc_data = claims_monthly_top10_2024[claims_monthly_top10_2024["procedurecode"] == proc_code]
                if not proc_data.empty:
                    fig_claims_2024.add_trace(go.Scatter(
                        x=proc_data["month_name"],
                        y=proc_data["total_cost"],
                        mode='lines+markers',
                        name=f'Code: {proc_code}',
                    ))
            
            fig_claims_2024.update_layout(
                title="Top 10 Claims Procedures Monthly Trends (2024)",
                xaxis_title="Month",
                yaxis_title="Total Cost",
                legend_title="Procedure Code",
                template="plotly_white",
                height=500,
                xaxis=dict(
                    tickmode='array',
                    tickvals=month_names
                )
            )
            
            st.plotly_chart(fig_claims_2024, use_container_width=True)

with tab2:
    st.header("Percentage Difference (2024 vs 2025)")
    
    # Calculate percentage differences between 2024 and 2025
    
    # For PA data
    pa_pct_diff = []
    pa_months = []
    
    for month in range(1, 13):  # For each month
        pa_2024_cost = pa_monthly_pd[(pa_monthly_pd["year"] == 2024) & (pa_monthly_pd["month"] == month)]["total_cost"].values
        pa_2025_cost = pa_monthly_pd[(pa_monthly_pd["year"] == 2025) & (pa_monthly_pd["month"] == month)]["total_cost"].values
        
        if len(pa_2024_cost) > 0 and len(pa_2025_cost) > 0 and pa_2024_cost[0] != 0:
            pct_diff = ((pa_2025_cost[0] - pa_2024_cost[0]) / pa_2024_cost[0]) * 100
            pa_pct_diff.append(pct_diff)
            pa_months.append(month_names[month-1])
    
    # For Claims data
    claims_pct_diff = []
    claims_months = []
    
    for month in range(1, 13):  # For each month
        claims_2024_cost = claims_monthly_pd[(claims_monthly_pd["year"] == 2024) & (claims_monthly_pd["month"] == month)]["total_cost"].values
        claims_2025_cost = claims_monthly_pd[(claims_monthly_pd["year"] == 2025) & (claims_monthly_pd["month"] == month)]["total_cost"].values
        
        if len(claims_2024_cost) > 0 and len(claims_2025_cost) > 0 and claims_2024_cost[0] != 0:
            pct_diff = ((claims_2025_cost[0] - claims_2024_cost[0]) / claims_2024_cost[0]) * 100
            claims_pct_diff.append(pct_diff)
            claims_months.append(month_names[month-1])
    
    # Create plotly figure for percentage differences
    fig_pct = go.Figure()
    
    # Add PA percentage difference
    if pa_pct_diff:
        fig_pct.add_trace(go.Scatter(
            x=pa_months, 
            y=pa_pct_diff,
            mode='lines+markers',
            name='PA % Change',
            line=dict(color='blue', width=2)
        ))
    
    # Add Claims percentage difference
    if claims_pct_diff:
        fig_pct.add_trace(go.Scatter(
            x=claims_months, 
            y=claims_pct_diff,
            mode='lines+markers',
            name='Claims % Change',
            line=dict(color='red', width=2)
        ))
    
    # Add zero line for reference
    fig_pct.add_shape(
        type="line",
        x0=month_names[0],
        y0=0,
        x1=month_names[-1],
        y1=0,
        line=dict(color="gray", width=1, dash="dash")
    )
    
    # Update layout
    fig_pct.update_layout(
        xaxis_title="Month",
        yaxis_title="Percentage Change (%)",
        legend_title="Data Series",
        template="plotly_white",
        height=600,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(fig_pct, use_container_width=True)
    
    # Show summary statistics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("PA Cost Change Summary")
        if pa_pct_diff:
            avg_change = sum(pa_pct_diff) / len(pa_pct_diff)
            max_increase = max(pa_pct_diff)
            max_decrease = min(pa_pct_diff)
            
            st.metric("Average Change", f"{avg_change:.2f}%")
            st.metric("Maximum Increase", f"{max_increase:.2f}%")
            st.metric("Maximum Decrease", f"{max_decrease:.2f}%")
        else:
            st.write("Insufficient data to calculate.")
    
    with col2:
        st.subheader("Claims Cost Change Summary")
        if claims_pct_diff:
            avg_change = sum(claims_pct_diff) / len(claims_pct_diff)
            max_increase = max(claims_pct_diff)
            max_decrease = min(claims_pct_diff)
            
            st.metric("Average Change", f"{avg_change:.2f}%")
            st.metric("Maximum Increase", f"{max_increase:.2f}%")
            st.metric("Maximum Decrease", f"{max_decrease:.2f}%")
        else:
            st.write("Insufficient data to calculate.")
    
    # ======= NEW ADDITION: TOP PROVIDERS ANALYSIS =======
    st.markdown("---")
    st.header("Top Providers Analysis")
    
    # Function to create dataframe of top providers
    def get_top_providers(data, year, metric_column, groupby_column='providername', count_column=None, top_n=30):
        """
        Get top providers by a specific metric for a given year
        
        Parameters:
        - data: polars DataFrame
        - year: year to filter data
        - metric_column: column to sum for metric (e.g., 'cost')
        - groupby_column: column to group by (default 'providername')
        - count_column: column to count unique values of (optional)
        - top_n: number of top providers to return
        
        Returns:
        - Pandas DataFrame with top providers
        """
        filtered_data = data.filter(pl.col("year") == year)
        
        if count_column:
            # Calculate both sum of metric and unique count
            result = (filtered_data
                      .group_by(groupby_column)
                      .agg([
                          pl.sum(metric_column).alias("total"),
                          pl.n_unique(count_column).alias("unique_count")
                      ])
                      .sort("unique_count", descending=True)
                      .limit(top_n)
                     )
            
            # Collect the LazyFrame before checking height
            result = result.collect()
            
            # Check if we have any data
            if result.height == 0:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=[groupby_column, 'total', 'unique_count'])
        else:
            # Just calculate sum of metric
            result = (filtered_data
                      .group_by(groupby_column)
                      .agg(pl.sum(metric_column).alias("total"))
                      .sort("total", descending=True)
                      .limit(top_n)
                     )
            
            # Collect the LazyFrame before checking height
            result = result.collect()
            
            # Check if we have any data
            if result.height == 0:
                # Return empty DataFrame with expected columns
                return pd.DataFrame(columns=[groupby_column, 'total'])
        
        return result.to_pandas()
    
    # Function to highlight new providers in 2025 vs 2024
    def highlight_new_providers(df, reference_providers, provider_column='Provider Name'):
        """Highlights only the provider's name cell if it's a new provider in 2025."""
        def highlight_font(val):
            if val not in reference_providers:
                return 'color: green; font-weight: bold'
            return ''
        
        return df.style.applymap(highlight_font, subset=[provider_column])

    
    # Create tabs for PA and Claims data
    pa_tab, claims_tab = st.tabs(["PA Providers", "Claims Providers"])
    
    with pa_tab:
        st.subheader("PA Data - Top 30 Providers")
        
        # Get top providers by cost
        pa_providers_2024_cost = get_top_providers(pa_data, 2024, "cost", top_n=30)
        pa_providers_2025_cost = get_top_providers(pa_data, 2025, "cost", top_n=30)
        
        # Get top providers by unique PA numbers
        pa_providers_2024_count = get_top_providers(pa_data, 2024, "cost", count_column="panumber", top_n=30)
        pa_providers_2025_count = get_top_providers(pa_data, 2025, "cost", count_column="panumber", top_n=30)
        
        # For count dataframes, we need to drop the 'total' column as we only care about unique counts
        pa_providers_2024_count = pa_providers_2024_count[['providername', 'unique_count']]
        pa_providers_2025_count = pa_providers_2025_count[['providername', 'unique_count']]
        
        # Get lists of provider names for comparison
        pa_2024_cost_providers = pa_providers_2024_cost['providername'].tolist()
        pa_2024_count_providers = pa_providers_2024_count['providername'].tolist()
        
        # Make copies of the dataframes before renaming columns
        pa_providers_2024_cost_display = pa_providers_2024_cost.copy()
        pa_providers_2025_cost_display = pa_providers_2025_cost.copy()
        pa_providers_2024_count_display = pa_providers_2024_count.copy()
        pa_providers_2025_count_display = pa_providers_2025_count.copy()
        
        # Format cost columns with currency
        if 'total' in pa_providers_2024_cost_display.columns:
            pa_providers_2024_cost_display['total'] = pa_providers_2024_cost_display['total'].map('₦{:,.2f}'.format)
        if 'total' in pa_providers_2025_cost_display.columns:
            pa_providers_2025_cost_display['total'] = pa_providers_2025_cost_display['total'].map('₦{:,.2f}'.format)
        
        
        
        # Rename columns before styling
        pa_providers_2024_cost_display.columns = ['Provider Name', 'Total Cost (2024)']
        pa_providers_2025_cost_display.columns = ['Provider Name', 'Total Cost (2025)']
        pa_providers_2024_count_display.columns = ['Provider Name', 'Unique PA Count (2024)']
        pa_providers_2025_count_display.columns = ['Provider Name', 'Unique PA Count (2025)']

        # Apply styling
        pa_providers_2025_cost_styled = highlight_new_providers(pa_providers_2025_cost_display, pa_2024_cost_providers)
        pa_providers_2025_count_styled = highlight_new_providers(pa_providers_2025_count_display, pa_2024_count_providers)

        
        # Display tables in tabs within PA tab
        cost_tab, count_tab = st.tabs(["By Cost", "By Unique PA Count"])
        
        with cost_tab:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Top Providers by Cost (2024)")
                display_dataframe(pa_providers_2024_cost_display, height=600)
            
            with col2:
                st.write("Top Providers by Cost (2025) - New providers highlighted")
                display_dataframe(pa_providers_2025_cost_styled, height=600)
        
        with count_tab:
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("Top Providers by Unique PA Count (2024)")
                display_dataframe(pa_providers_2024_count_display, height=600)
            
            with col2:
                st.write("Top Providers by Unique PA Count (2025) - New providers highlighted")
                display_dataframe(pa_providers_2025_count_styled, height=600)
    
    with claims_tab:
        st.subheader("Claims Data - Top 30 Providers")
        
        # Get top providers by cost
        claims_providers_2024_cost = get_top_providers(claims_data, 2024, "cost", top_n=30)
        claims_providers_2025_cost = get_top_providers(claims_data, 2025, "cost", top_n=30)
        
        # Get lists of provider names for comparison
        claims_2024_cost_providers = claims_providers_2024_cost['providername'].tolist()
        
        # Make copies of the dataframes before renaming columns
        claims_providers_2024_cost_display = claims_providers_2024_cost.copy()
        claims_providers_2025_cost_display = claims_providers_2025_cost.copy()
        
        # Format cost columns with currency
        if 'total' in claims_providers_2024_cost_display.columns:
            claims_providers_2024_cost_display['total'] = claims_providers_2024_cost_display['total'].map('₦{:,.2f}'.format)
        if 'total' in claims_providers_2025_cost_display.columns:
            claims_providers_2025_cost_display['total'] = claims_providers_2025_cost_display['total'].map('₦{:,.2f}'.format)

        # Rename columns BEFORE styling
        claims_providers_2024_cost_display.columns = ['Provider Name', 'Total Cost (2024)']
        claims_providers_2025_cost_display.columns = ['Provider Name', 'Total Cost (2025)']

        # Highlight new providers in 2025
        claims_providers_2025_cost_styled = highlight_new_providers(claims_providers_2025_cost_display, claims_2024_cost_providers)

        
        # Display tables
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("Top Providers by Cost (2024)")
            display_dataframe(claims_providers_2024_cost_display, height=600)
        
        with col2:
            st.write("Top Providers by Cost (2025) - New providers highlighted")
            display_dataframe(claims_providers_2025_cost_styled, height=600)
        
        # Add information about color coding
        st.info("Providers highlighted in light green are new to the top 30 in 2025 compared to 2024.")
        
        # Add visualization of provider comparison
        st.subheader("Top 10 Provider Comparison (2024 vs 2025)")
        
        # Get top 10 providers for both years
        top10_claims_2024 = get_top_providers(claims_data, 2024, "cost", top_n=10)
        top10_claims_2025 = get_top_providers(claims_data, 2025, "cost", top_n=10)
        
        # Make sure the dataframes have the expected structure and aren't empty
        if not top10_claims_2024.empty and 'total' in top10_claims_2024.columns:
            # Convert totals back to numeric for plotting if they were formatted as currency strings
            if not pd.api.types.is_numeric_dtype(top10_claims_2024['total']):
                top10_claims_2024['total'] = pd.to_numeric(top10_claims_2024['total'].replace('₦', '', regex=True)
                                                         .replace(',', '', regex=True), errors='coerce')
        
        if not top10_claims_2025.empty and 'total' in top10_claims_2025.columns:
            if not pd.api.types.is_numeric_dtype(top10_claims_2025['total']):
                top10_claims_2025['total'] = pd.to_numeric(top10_claims_2025['total'].replace('₦', '', regex=True)
                                                         .replace(',', '', regex=True), errors='coerce')
        
        # Create side-by-side bar chart if we have data
        fig = make_subplots(rows=1, cols=2, subplot_titles=("Top 10 Providers 2024", "Top 10 Providers 2025"),
                           specs=[[{"type": "bar"}, {"type": "bar"}]])
        
        # Add 2024 data if available
        if not top10_claims_2024.empty and 'total' in top10_claims_2024.columns and 'providername' in top10_claims_2024.columns:
            fig.add_trace(
                go.Bar(x=top10_claims_2024['total'], y=top10_claims_2024['providername'],
                      orientation='h', name='2024', marker_color='blue'),
                row=1, col=1
            )
        
        # Add 2025 data if available
        if not top10_claims_2025.empty and 'total' in top10_claims_2025.columns and 'providername' in top10_claims_2025.columns:
            fig.add_trace(
                go.Bar(x=top10_claims_2025['total'], y=top10_claims_2025['providername'],
                      orientation='h', name='2025', marker_color='red'),
                row=1, col=2
            )
        
        # Update layout
        fig.update_layout(height=500, showlegend=False)
        fig.update_xaxes(title_text="Total Cost (₦)")
        
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.header("Unique PA Numbers Monthly Trend")
    
    # Create plotly figure for unique PA numbers
    fig_unique = go.Figure()


    # Add PA 2022 unique panumber counts
    pa_unique_2022 = pa_monthly_pd[pa_monthly_pd["year"] == 2022].sort_values("month")
    if not pa_unique_2022.empty:
        fig_unique.add_trace(go.Scatter(
            x=pa_unique_2022["month_name"], 
            y=pa_unique_2022["unique_panumber"],
            mode='lines+markers',
            name='Unique PAs 2022',
            line=dict(color='blue', width=2)
        ))

    # Add PA 2023 unique panumber counts
    pa_unique_2023 = pa_monthly_pd[pa_monthly_pd["year"] == 2023].sort_values("month")
    if not pa_unique_2023.empty:
        fig_unique.add_trace(go.Scatter(
            x=pa_unique_2023["month_name"], 
            y=pa_unique_2023["unique_panumber"],
            mode='lines+markers',
            name='Unique PAs 2023',
            line=dict(color='purple', width=2)
        ))    
    
    # Add PA 2024 unique panumber counts
    pa_unique_2024 = pa_monthly_pd[pa_monthly_pd["year"] == 2024].sort_values("month")
    if not pa_unique_2024.empty:
        fig_unique.add_trace(go.Scatter(
            x=pa_unique_2024["month_name"], 
            y=pa_unique_2024["unique_panumber"],
            mode='lines+markers',
            name='Unique PAs 2024',
            line=dict(color='green', width=2)
        ))
    
    # Add PA 2025 unique panumber counts
    pa_unique_2025 = pa_monthly_pd[pa_monthly_pd["year"] == 2025].sort_values("month")
    if not pa_unique_2025.empty:
        fig_unique.add_trace(go.Scatter(
            x=pa_unique_2025["month_name"], 
            y=pa_unique_2025["unique_panumber"],
            mode='lines+markers',
            name='Unique PAs 2025',
            line=dict(color='lightgreen', width=2, dash='dash')
        ))
    
    # Update layout
    fig_unique.update_layout(
        xaxis_title="Month",
        yaxis_title="Unique PA Numbers Count",
        legend_title="Year",
        template="plotly_white",
        height=600,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(fig_unique, use_container_width=True)
    
    # Calculate percentage change in unique PA numbers
    if not pa_unique_2024.empty and not pa_unique_2025.empty:
        st.subheader("Year-over-Year Change in Unique PA Numbers")
        
        pa_unique_pct_diff = []
        pa_unique_months = []
        
        for month in range(1, 13):  # For each month
            pa_2024_count = pa_monthly_pd[(pa_monthly_pd["year"] == 2024) & (pa_monthly_pd["month"] == month)]["unique_panumber"].values
            pa_2025_count = pa_monthly_pd[(pa_monthly_pd["year"] == 2025) & (pa_monthly_pd["month"] == month)]["unique_panumber"].values
            
            if len(pa_2024_count) > 0 and len(pa_2025_count) > 0 and pa_2024_count[0] != 0:
                pct_diff = ((pa_2025_count[0] - pa_2024_count[0]) / pa_2024_count[0]) * 100
                pa_unique_pct_diff.append(pct_diff)
                pa_unique_months.append(month_names[month-1])
        
        if pa_unique_pct_diff:
            avg_change = sum(pa_unique_pct_diff) / len(pa_unique_pct_diff)
            max_increase = max(pa_unique_pct_diff)
            max_decrease = min(pa_unique_pct_diff)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Average Change", f"{avg_change:.2f}%")
            col2.metric("Maximum Increase", f"{max_increase:.2f}%")
            col3.metric("Maximum Decrease", f"{max_decrease:.2f}%")

    st.markdown("---")
    st.subheader("Cost per Procedure Code Over Time")

    # Filter 2024 and 2025 data
    pa_trend_data = pa_data.filter((pl.col("year") == 2024) | (pl.col("year") == 2025)).collect()

    # Group by year, month, and code
    cost_per_code = (
        pa_trend_data
        .group_by(["year", "month", "code"])
        .agg([
            pl.sum("cost").alias("total_cost"),
            pl.count("code").alias("volume")
        ])
        .with_columns([
            (pl.col("total_cost") / pl.col("volume")).alias("cost_per_unit")
        ])
        .sort(["code", "year", "month"])
        .to_pandas()
    )

    # Add month name
    cost_per_code["month_name"] = cost_per_code["month"].apply(lambda x: month_names[x-1])

    # Let user pick a procedure code to explore
    unique_codes = sorted(cost_per_code["code"].unique())
    selected_code = st.selectbox("Select a Procedure Code to Explore", unique_codes)

    code_data = cost_per_code[cost_per_code["code"] == selected_code]

    # Plot cost per unit over time for the selected code
    fig_code_trend = go.Figure()

    for year in [2024, 2025]:
        data = code_data[code_data["year"] == year]
        if not data.empty:
            fig_code_trend.add_trace(go.Scatter(
                x=data["month_name"],
                y=data["cost_per_unit"],
                mode='lines+markers',
                name=f"{year}",
            ))

    fig_code_trend.update_layout(
        title=f"Monthly Cost per Unit for Procedure Code: {selected_code}",
        xaxis_title="Month",
        yaxis_title="Cost per Unit (₦)",
        template="plotly_white",
        height=500,
        legend_title="Year"
    )

    st.plotly_chart(fig_code_trend, use_container_width=True)

    # Optional: Volume chart for the same code
    fig_volume = go.Figure()

    for year in [2024, 2025]:
        data = code_data[code_data["year"] == year]
        if not data.empty:
            fig_volume.add_trace(go.Scatter(
                x=data["month_name"],
                y=data["volume"],
                mode='lines+markers',
                name=f"{year}",
            ))

    fig_volume.update_layout(
        title=f"Monthly Volume for Procedure Code: {selected_code}",
        xaxis_title="Month",
        yaxis_title="Volume (Count)",
        template="plotly_white",
        height=400,
        legend_title="Year"
    )

    st.plotly_chart(fig_volume, use_container_width=True)


with tab4:
    st.header("Cash Received Comparison (2024 vs 2025)")
    
    # Create plotly figure for cash received
    fig_cash = go.Figure()
    
    # Add Cash Received 2024 data
    cash_2024 = cash_monthly_pd[cash_monthly_pd["year"] == 2024].sort_values("month")
    if not cash_2024.empty:
        fig_cash.add_trace(go.Scatter(
            x=cash_2024["month_name"], 
            y=cash_2024["total_cost"],
            mode='lines+markers',
            name='Cash Received 2024',
            line=dict(color='purple', width=2)
        ))
    
    # Add Cash Received 2025 data
    cash_2025 = cash_monthly_pd[cash_monthly_pd["year"] == 2025].sort_values("month")
    if not cash_2025.empty:
        fig_cash.add_trace(go.Scatter(
            x=cash_2025["month_name"], 
            y=cash_2025["total_cost"],
            mode='lines+markers',
            name='Cash Received 2025',
            line=dict(color='violet', width=2, dash='dash')
        ))
    
    # Update layout
    fig_cash.update_layout(
        xaxis_title="Month",
        yaxis_title="Total Cash Received",
        legend_title="Year",
        template="plotly_white",
        height=600,
        xaxis=dict(
            tickmode='array',
            tickvals=month_names
        )
    )
    
    st.plotly_chart(fig_cash, use_container_width=True)
    
    # Calculate percentage differences between 2024 and 2025 for cash received
    cash_pct_diff = []
    cash_months = []
    
    for month in range(1, 13):  # For each month
        cash_2024_amount = cash_monthly_pd[(cash_monthly_pd["year"] == 2024) & (cash_monthly_pd["month"] == month)]["total_cost"].values
        cash_2025_amount = cash_monthly_pd[(cash_monthly_pd["year"] == 2025) & (cash_monthly_pd["month"] == month)]["total_cost"].values
        
        if len(cash_2024_amount) > 0 and len(cash_2025_amount) > 0 and cash_2024_amount[0] != 0:
            pct_diff = ((cash_2025_amount[0] - cash_2024_amount[0]) / cash_2024_amount[0]) * 100
            cash_pct_diff.append(pct_diff)
            cash_months.append(month_names[month-1])
    
    # Show summary statistics for cash received
    if cash_pct_diff:
        st.subheader("Cash Received Change Summary")
        
        avg_change = sum(cash_pct_diff) / len(cash_pct_diff)
        max_increase = max(cash_pct_diff)
        max_decrease = min(cash_pct_diff)
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Average Change", f"{avg_change:.2f}%")
        col2.metric("Maximum Increase", f"{max_increase:.2f}%")
        col3.metric("Maximum Decrease", f"{max_decrease:.2f}%")
        
        # Calculate total cash received for each year
        if not cash_2024.empty:
            total_2024 = cash_2024["total_cost"].sum()
        else:
            total_2024 = 0
            
        if not cash_2025.empty:
            total_2025 = cash_2025["total_cost"].sum()
        else:
            total_2025 = 0
        
        # Display total cash received
        st.subheader("Total Cash Received")
        col1, col2, col3 = st.columns(3)
        col1.metric("2024 Total", f"₦{total_2024:,.2f}")
        col2.metric("2025 Total", f"₦{total_2025:,.2f}")
        
        if total_2024 > 0:
            yoy_change = ((total_2025 - total_2024) / total_2024) * 100
            col3.metric("Year-over-Year Change", f"{yoy_change:.2f}%", 
                       delta=f"{yoy_change:.2f}%", 
                       delta_color="normal")
            
with tab5:
    st.markdown("---")
    st.subheader("📈 Forecasting PA Cost and Volume for 2025")

    # Prepare data
    monthly_df = pa_monthly.to_pandas()
    monthly_df["date"] = pd.to_datetime(monthly_df["year"].astype(str) + "-" + monthly_df["month"].astype(str) + "-01")
    monthly_df = monthly_df.sort_values("date")

    # Train cutoff (e.g. March 2025)
    train_df = monthly_df[monthly_df["date"] < "2025-04-01"]

    # Prophet expects two columns: ds (date), y (value to predict)

    # ---- COST FORECAST ---- #
    cost_df = train_df[["date", "total_cost"]].rename(columns={"date": "ds", "total_cost": "y"})

    cost_model = Prophet()
    cost_model.fit(cost_df)

    future_cost = cost_model.make_future_dataframe(periods=9, freq='MS')  # 9 months ahead
    forecast_cost = cost_model.predict(future_cost)

    # Plot with plotly
    fig_cost = plot_plotly(cost_model, forecast_cost)
    fig_cost.update_layout(
        title="Forecasted PA Cost (₦) for 2025",
        xaxis_title="Month",
        yaxis_title="Cost (₦)",
        height=500,
        template="plotly_white"
    )

    st.plotly_chart(fig_cost, use_container_width=True)

    # ---- VOLUME FORECAST ---- #
    volume_df = train_df[["date", "unique_panumber"]].rename(columns={"date": "ds", "unique_panumber": "y"})

    volume_model = Prophet()
    volume_model.fit(volume_df)

    future_volume = volume_model.make_future_dataframe(periods=9, freq='MS')
    forecast_volume = volume_model.predict(future_volume)

    fig_volume = plot_plotly(volume_model, forecast_volume)
    fig_volume.update_layout(
        title="Forecasted PA Volume (Number of Procedures) for 2025",
        xaxis_title="Month",
        yaxis_title="Volume",
        height=500,
        template="plotly_white"
    )

    st.plotly_chart(fig_volume, use_container_width=True)
