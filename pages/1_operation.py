import streamlit as st
from st_aggrid import AgGrid
import polars as pl
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, JsCode
from utils.data_loader import (
    get_data, 
    prepare_claims_comparison, 
    convert_to_pandas_if_needed, 
    calculate_average_costs_per_enrollee, 
    prepare_active_plans, 
    online_pa_usage, 
    benefit, 
    enrollee_comparison, 
    revenue_pa, 
    pharmacy_carecord, 
    load_excel_data,
    process_debit_notes,
    calculate_pa_mlr,
    calculate_claims_mlr
)
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, datetime  # Import date separately
import traceback
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
# pages/operations.py
from utils.requires_data_loading import requires_data_loading

@requires_data_loading
def main():
    try:
        st.title("📊 Operational Analytics")
        st.markdown("---")
        
        # Add a refresh button at the top of the page
        col1, col2 = st.columns([1, 5])
        with col1:
            if st.button('🔄 Refresh Data'):
                # Clear only data-related session state
                if 'data' in st.session_state:
                    del st.session_state['data']
                # Clear specific caches if needed
                st.cache_data.clear()
                st.rerun()
        
        with col2:
            st.info("Click refresh to reload all data")
        
        # Initialize data if needed
        if 'data' not in st.session_state:
            initialize_data()
        
        # Get PA data from session state
        if 'PA' not in st.session_state.data:
            st.error("PA data not found in session state")
            return
        
        PA = st.session_state.data['PA']
        
        # Calculate daily PA statistics
        pa_daily_stats = PA.group_by(
            pl.col('requestdate').dt.date().alias('date')
        ).agg([
            pl.col('panumber').n_unique().alias('pa_count')
        ]).sort('date')
        
        # Get data
        data, metrics = get_data()
        
        # Check if metrics are available
        if not metrics:
            metrics = {
                'TOTAL_ACTIVE_ENROLLEES': 0,
                'TOTAL_PROVIDER_COUNT': 0,
                'TOTAL_ACTIVE_CONTRACTS': 0,
                'DENIAL_RATE': 0,
                # Add other metrics as needed
            }
        
        # Dashboard layout
        st.title("Operations Dashboard")
        
        # Key Metrics Section
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            enrollees = metrics.get('TOTAL_ACTIVE_ENROLLEES', 0)
            st.metric("Active Enrollees", enrollees)
            if enrollees == 0:
                st.warning("No active enrollees found")
            
        with col2:
            providers = metrics.get('TOTAL_PROVIDER_COUNT', 0)
            st.metric("Active Providers", providers)
            if providers == 0:
                st.warning("No active providers found")
            
        with col3:
            contracts = metrics.get('TOTAL_ACTIVE_CONTRACTS', 0)
            st.metric("Active Contracts", contracts)
            if contracts == 0:
                st.warning("No active contracts found")
            
        with col4:
            denial_rate = metrics.get('DENIAL_RATE', 0)
            st.metric("Denial Rate", f"{denial_rate}%")
            if denial_rate == 0:
                st.warning("No denial rate calculated")

        # Section 1: Claims Analysis
        st.subheader("Claims Analysis")
        claims_comparison = prepare_claims_comparison(data)
        
        # Add date filter
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=datetime(2024, 1, 1).date(),
                format="YYYY-MM-DD"
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=datetime.now().date(),
                format="YYYY-MM-DD"
            )

        # Handle Claims Comparison visualization
        if 'claims_comparison' in st.session_state:
            claims_comparison = st.session_state.claims_comparison
            if isinstance(claims_comparison, pl.LazyFrame):
                claims_comparison = claims_comparison.collect()
            
            if claims_comparison.height > 0:
                # Add toggle button for detailed table view
                show_table = st.toggle('Show Detailed Monthly Breakdown')
                
                try:
                    # Filter out None values and convert Month strings to datetime
                    valid_months = claims_comparison.filter(pl.col('Month').is_not_null())
                    if valid_months.height > 0:
                        month_dates = []
                        for month in valid_months['Month']:
                            try:
                                month_date = datetime.strptime(month, '%B %Y').date()
                                month_dates.append(month_date)
                            except (ValueError, TypeError):
                                continue
                        
                        if month_dates:
                            # Create a mask for filtering
                            date_mask = [start_date <= date <= end_date for date in month_dates]
                            
                            # Convert mask to Polars series for filtering
                            mask_series = pl.Series(date_mask)
                            
                            # Apply the filter
                            filtered_claims = valid_months.filter(mask_series)
                            
                            if filtered_claims.height > 0:
                                # Sort the filtered claims by month to ensure correct order
                                sorted_filtered_claims = filtered_claims.sort('Month')
                                
                                fig = px.bar(
                                    sorted_filtered_claims.to_pandas(),
                                    x='Month',
                                    y=['PA Cost', 'Claims Cost', 'Unclaimed PA Cost'],
                                    title='Claims Analysis by Month',
                                    labels={'value': 'Amount (₦)', 'variable': 'Cost Type'},
                                    template='plotly_white',
                                    barmode='group'
                                )
                                
                                fig.update_layout(
                                    legend=dict(
                                        orientation="h",
                                        yanchor="bottom",
                                        y=1.02,
                                        xanchor="right",
                                        x=1
                                    ),
                                    xaxis_tickangle=-45,
                                    height=500
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
                                
                                # Calculate month-over-month changes
                                df_pandas = sorted_filtered_claims.to_pandas()
                                df_pandas = df_pandas.sort_values('Month')
                                
                                changes = {}
                                for col in ['PA Cost', 'Claims Cost', 'Unclaimed PA Cost']:
                                    if len(df_pandas) >= 2:
                                        last_value = df_pandas[col].iloc[-1]
                                        prev_value = df_pandas[col].iloc[-2]
                                        pct_change = ((last_value - prev_value) / prev_value * 100) if prev_value != 0 else 0
                                        total_value = df_pandas[col].sum()
                                        changes[col] = {
                                            'value': total_value,
                                            'change': pct_change,
                                            'trend': '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                                        }
                                    else:
                                        total_value = df_pandas[col].sum() if len(df_pandas) > 0 else 0
                                        changes[col] = {
                                            'value': total_value,
                                            'change': 0,
                                            'trend': '→'
                                        }
                                
                                # Display metrics with trends
                                cols = st.columns(3)
                                for i, (label, data) in enumerate(changes.items()):
                                    with cols[i]:
                                        st.metric(
                                            f"Total {label}",
                                            f"₦{data['value']:,.2f}",
                                            f"{data['change']:+.1f}% {data['trend']} compared to last month",
                                            delta_color="normal" if label != 'Unclaimed PA Cost' else "inverse"
                                        )
                                
                                # Show detailed table if toggle is on
                                if show_table:
                                    st.subheader("Monthly Breakdown")
                                    display_columns = ['Month', 'PA Cost', 'Claims Cost', 'Unclaimed PA Cost']
                                    formatted_df = df_pandas[display_columns].copy()
                                    
                                    for col in ['PA Cost', 'Claims Cost', 'Unclaimed PA Cost']:
                                        formatted_df[col] = formatted_df[col].apply(lambda x: f"₦{x:,.2f}")
                                        
                                    st.dataframe(
                                        formatted_df,
                                        use_container_width=True,
                                        hide_index=True
                                    )
                            else:
                                st.warning("No data available for the selected date range")
                        else:
                            st.warning("No valid month data available")
                    else:
                        st.warning("No valid month data available")
                    
                except Exception as e:
                    st.error(f"Error processing claims comparison: {str(e)}")
                    st.error(traceback.format_exc())
            else:
                st.warning("No claims comparison data available")
        else:
            st.warning("No claims comparison data available")

        # New Section: PA Benefit Analysis
        st.subheader("PA Benefit Analysis")

        pa_benefit, all_pa, all_claims = pharmacy_carecord()
        st.session_state.pa_benefit = pa_benefit
        st.session_state.all_pa = all_pa
        st.session_state.all_claims = all_claims

        # Get PA Benefit data
        if 'pa_benefit' not in st.session_state:
            st.warning("PA Benefit data not available. Please ensure data is loaded.")
            return

        # Check if pa_benefit is None or empty
        if pa_benefit is None:
            st.warning("No PA Benefit data available. Please check your data sources.")
            return

        # Collect the LazyFrame if it is one
        if isinstance(pa_benefit, pl.LazyFrame):
            pa_benefit = pa_benefit.collect()

        if pa_benefit.height == 0:
            st.warning("No PA Benefit data available. Please check your data sources.")
            return

        # Add date range filter
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=date(2025, 1, 1),
                min_value=date(2024, 1, 1),
                max_value=date(2025, 12, 31)
            )
        with col2:
            end_date = st.date_input(
                "End Date", 
                value=date.today(),
                min_value=date(2024, 1, 1),
                max_value=date(2025, 12, 31)
            )

        # Add a select button for year filter
        year_filter = st.selectbox(
            "Select Year",
            options=[2025, 2024],  # Ensure these are integers, not strings
            index=0  # Default to 2025
        )

        # Filter data based on selected date range and year
        filtered_pa_benefit = pa_benefit.filter(
            (pl.col('date').dt.date() >= pl.lit(start_date)) &
            (pl.col('date').dt.date() <= pl.lit(end_date)) &
            (pl.col('date').dt.year() == int(year_filter))
        )

        # Group by month and calculate total cost
        monthly_pa_cost = filtered_pa_benefit.group_by(
            pl.col('date').dt.strftime('%B %Y').alias('Month')
        ).agg(
            pl.col('cost').sum().alias('Total Cost')
        ).sort('Month')

        # Plot the bar chart
        fig = px.bar(
            monthly_pa_cost.to_pandas(),
            x='Month',
            y='Total Cost',
            title=f'PA Cost by Month ({year_filter})',
            labels={'Total Cost': 'Total Cost (₦)', 'Month': 'Month'},
            template='plotly_white'
        )

        fig.update_layout(
            xaxis_tickangle=-45,
            height=500
        )

        st.plotly_chart(fig, use_container_width=True)

        # Top 20 Providers by Cost
        st.subheader("Top 20 Providers by Cost")

        # Ensure required columns exist in the filtered dataframe
        required_columns = ['providername', 'granted', 'panumber', 'IID']
        if all(col in filtered_pa_benefit.columns for col in required_columns):
            top_providers = filtered_pa_benefit.group_by('providername').agg(
                pl.col('granted').sum().alias('Total Cost'),
                pl.col('panumber').n_unique().alias('Unique PA Count'),
                pl.col('IID').n_unique().alias('Unique Customer Count')
            ).sort('Total Cost', descending=True).head(200)

            st.dataframe(
                top_providers.to_pandas(),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.warning("Required columns for provider analysis are missing in the PA data.")

        # Top 20 Customers by Cost and visit
        st.subheader("Top 20 Customers by visit")

        top_customers = filtered_pa_benefit.group_by('IID').agg(
            pl.col('panumber').n_unique().alias('Unique PA Count')
        ).sort('Unique PA Count', descending=True).head(200)

        st.dataframe(
            top_customers.to_pandas(),
            use_container_width=True,
            hide_index=True
        )

        st.subheader("Top 20 Customers by cost")

        # Get most frequently visited provider for each IID
        most_visited_provider = filtered_pa_benefit.group_by(['IID', 'providername']).agg(
            pl.count().alias('visit_count')
        ).sort(['IID', 'visit_count'], descending=[False, True]).group_by('IID').agg(
            pl.col('providername').first().alias('Most Visited Provider')
        )

        # Get total cost by customer
        top_customers_cost = filtered_pa_benefit.group_by('IID').agg(
            pl.col('granted').sum().alias('Total Cost')
        ).sort('Total Cost', descending=True).head(200)

        # Join the information
        top_customers_cost_with_provider = top_customers_cost.join(
            most_visited_provider,
            on='IID',
            how='left'
        )

        # Reorder columns to have the provider first
        top_customers_cost_with_provider = top_customers_cost_with_provider.select(
            ['IID','Most Visited Provider', 'Total Cost']
        )

        st.dataframe(
            top_customers_cost_with_provider.to_pandas(),
            use_container_width=True,
            hide_index=True
        )

        # Top 20 Groups by Cost
        st.subheader("Top 20 Groups by Cost")

        top_groups = filtered_pa_benefit.group_by('groupname').agg(
            pl.col('granted').sum().alias('Total Cost'),
            pl.col('panumber').n_unique().alias('Unique PA Count'),
            pl.col('IID').n_unique().alias('Unique Customer Count')
        ).sort('Total Cost', descending=True).head(20)

        st.dataframe(
            top_groups.to_pandas(),
            use_container_width=True,
            hide_index=True
        )

        #PHARMACY ANALYSIS
        Total_cdr_enrollees = pa_benefit.filter(
            (pl.col('Benefit') == 'Chronic Medication') &
            (pl.col('requestdate').dt.year() == 2024)
        ).select(pl.col('IID').n_unique()).item()

        # Get unique IIDs for Chronic Medication in 2024 and 2025 grouped by groupname
        cdr_by_group = pa_benefit.filter(
            (pl.col('Benefit') == 'Chronic Medication') &
            ((pl.col('requestdate').dt.year() == 2024) | 
             (pl.col('requestdate').dt.year() == 2025))
        ).group_by('groupname').agg([
            pl.col('IID').filter(pl.col('requestdate').dt.year() == 2024).n_unique().alias('2024 Unique Enrollees'),
            pl.col('IID').filter(pl.col('requestdate').dt.year() == 2025).n_unique().alias('2025 Unique Enrollees')
        ]).sort('2024 Unique Enrollees', descending=True)

        #Profit and Revenue from Pharmacy
        # Calculate total revenue and profit for provider 5330
        provider_5330_data = filtered_pa_benefit.filter(pl.col('providerid') == '5330')
        total_revenue = provider_5330_data['granted'].sum()
        total_profit = total_revenue - (total_revenue/1.25)
        
        # Display metrics in columns
        col1, col2,col3= st.columns(3)
        with col1:
            st.metric("Total Revenue (Provider 5330)", f"₦{total_revenue:,.2f}")
        with col2:
            st.metric("Total Profit (Provider 5330)", f"₦{total_profit:,.2f}")
        with col3:
            st.metric("TOTAL CDR 2024", Total_cdr_enrollees)


        # Display the table
        st.subheader("CDR Enrollees by Group (2024 & 2025)")
        st.dataframe(
            cdr_by_group.to_pandas(),
            use_container_width=True,
            hide_index=True
        )
     

        # Section 2: Provider Analysis
        st.subheader("Provider Analysis")

        if 'base_metrics' in st.session_state and 'pivot_provider' in st.session_state.base_metrics:
            try:
                pivot_df = st.session_state.base_metrics['pivot_provider']
                
                # Convert numeric columns while preserving statename
                numeric_cols = [col for col in pivot_df.columns if col != 'statename']
                
                # Create a new DataFrame with converted columns
                pivot_df = pivot_df.with_columns([
                    pl.col(col).cast(pl.Int64, strict=False) for col in numeric_cols
                ]).with_columns([
                    pl.sum_horizontal(numeric_cols).alias('Total')
                ])
                
                # Sort by total
                pivot_df = pivot_df.sort('Total', descending=True)
                
                # Show the table
                st.dataframe(
                    pivot_df,
                    use_container_width=True,
                    hide_index=True
                )
                
                try:
                    # Calculate metrics
                    top_state = pivot_df.filter(pl.col('Total') == pl.col('Total').max())
                    top_state_name = top_state.select('statename').item()
                    top_state_count = top_state.select('Total').item()
                    
                    metrics = {
                        'Largest Provider Base': {
                            'value': f"{top_state_name}",
                            'count': f"{top_state_count:,}"
                        }
                    }
                    
                    # Add metrics for each band
                    for band in ['"Band D"', '"Band C"', '"Band B"', '"Band A"']:
                        if band in pivot_df.columns:
                            top_band = pivot_df.filter(pl.col(band) == pl.col(band).max())
                            metrics[f'Highest {band}'] = {
                                'value': top_band.select('statename').item(),
                                'count': f"{top_band.select(band).item():,}"
                            }
                    
                    # Display metrics in columns
                    cols = st.columns(len(metrics))
                    for i, (label, data) in enumerate(metrics.items()):
                        with cols[i]:
                            st.metric(
                                label,
                                data['value'],
                                f"Count: {data['count']}"
                            )
                    
                    # Provider Growth Analysis
                    st.subheader("Provider Growth Analysis")
                    
                    if 'previous_provider_counts' not in st.session_state:
                        st.session_state.previous_provider_counts = pivot_df.select(numeric_cols + ['Total']).sum()
                    
                    current_counts = pivot_df.select(numeric_cols + ['Total']).sum()
                    previous_counts = st.session_state.previous_provider_counts
                    
                    # Create two separate containers for the rows
                    row1_container = st.container()
                    st.markdown("---")  # Add a separator
                    row2_container = st.container()
                    
                    # Helper function to safely get numeric value
                    def get_safe_value(counts, col):
                        try:
                            if isinstance(counts, pl.DataFrame):
                                return float(counts[col].item() if col in counts.columns else 0)
                            elif isinstance(counts, pl.Series):
                                return float(counts[col] if col in counts.index else 0)
                            else:
                                return float(counts[col]) if col in counts else 0
                        except:
                            return 0.0
                    
                    # First row - Band metrics (D and C)
                    with row1_container:
                        st.write("Band D and C Distribution")
                        col1, col2 = st.columns(2)
                        
                        # Band D
                        with col1:
                            col = 'Band D'
                            current = get_safe_value(current_counts, col)
                            previous = get_safe_value(previous_counts, col)
                            
                            pct_change = ((current - previous) / previous * 100) if previous != 0 else 0
                            trend = '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                            
                            st.metric(
                                "Band D Providers",
                                f"{int(current):,}",
                                f"{pct_change:+.1f}% {trend}",
                                delta_color="normal"
                            )
                        
                        # Band C
                        with col2:
                            col = 'Band C'
                            current = get_safe_value(current_counts, col)
                            previous = get_safe_value(previous_counts, col)
                            
                            pct_change = ((current - previous) / previous * 100) if previous != 0 else 0
                            trend = '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                            
                            st.metric(
                                "Band C Providers",
                                f"{int(current):,}",
                                f"{pct_change:+.1f}% {trend}",
                                delta_color="normal"
                            )
                    
                    # Second row - Band metrics (B and A) and Total
                    with row2_container:
                        st.write("Band B, A and Total Distribution")
                        col1, col2, col3 = st.columns(3)
                        
                        # Band B
                        with col1:
                            col = 'Band B'
                            current = get_safe_value(current_counts, col)
                            previous = get_safe_value(previous_counts, col)
                            
                            pct_change = ((current - previous) / previous * 100) if previous != 0 else 0
                            trend = '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                            
                            st.metric(
                                "Band B Providers",
                                f"{int(current):,}",
                                f"{pct_change:+.1f}% {trend}",
                                delta_color="normal"
                            )
                        
                        # Band A
                        with col2:
                            col = 'Band A'
                            current = get_safe_value(current_counts, col)
                            previous = get_safe_value(previous_counts, col)
                            
                            pct_change = ((current - previous) / previous * 100) if previous != 0 else 0
                            trend = '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                            
                            st.metric(
                                "Band A Providers",
                                f"{int(current):,}",
                                f"{pct_change:+.1f}% {trend}",
                                delta_color="normal"
                            )
                        
                        # Total
                        with col3:
                            col = 'Total'
                            current = get_safe_value(current_counts, col)
                            previous = get_safe_value(previous_counts, col)
                            
                            pct_change = ((current - previous) / previous * 100) if previous != 0 else 0
                            trend = '↑' if pct_change > 0 else '↓' if pct_change < 0 else '→'
                            
                            st.metric(
                                "Total Providers",
                                f"{int(current):,}",
                                f"{pct_change:+.1f}% {trend}",
                                delta_color="normal"
                            )
                    
                    # Update previous counts for next comparison
                    st.session_state.previous_provider_counts = current_counts
                    
                except Exception as e:
                    st.error(f"Error in metrics calculation: {str(e)}")
                    st.error(traceback.format_exc())
                    
            except Exception as e:
                st.error(f"Error in Provider Analysis: {str(e)}")
                st.error(traceback.format_exc())
        else:
            st.warning("Provider distribution data not available")

        #CLAIMS ANALYSIS
        st.subheader("Claims Analysis")
        data = load_excel_data()
        claims = data['CLAIMS']
        claims = claims.filter(pl.col("datesubmitted").dt.year() == 2025)
        enc_claims = claims.filter(pl.col("encounterdatefrom").dt.year() == 2025)

        claims = claims.filter(pl.col("nhislegacynumber").str.contains("CL"))
        current_year = datetime.now().year
        
        # Group by month and calculate totals
        monthly_submitted = claims.group_by(
            pl.col("datesubmitted").dt.strftime("%B %Y").alias("Month")
        ).agg(
            pl.col("approvedamount").sum().alias("Total Submitted Amount")
        ).sort("Month").collect()  # Collect here

        monthly_encountered = claims.group_by(
            pl.col("encounterdatefrom").dt.strftime("%B %Y").alias("Month") 
        ).agg(
            pl.col("approvedamount").sum().alias("Total Encountered Amount")
        ).sort("Month").collect()  # Collect here

        # Join the two dataframes
        # Create list of months for 2025 from January to current month
        current_month = datetime.now().month
        months_2025 = [datetime(2025, m, 1).strftime("%B %Y") for m in range(1, current_month + 1)]

        monthly_totals = monthly_submitted.join(
            monthly_encountered,
            on="Month",
            how="outer"
        ).filter(pl.col("Month").is_in(months_2025))
        
        try:
            # Sort by month chronologically using a more explicit datetime conversion
            monthly_totals = monthly_totals.with_columns([
                pl.col("Month").str.to_datetime(format="%B %Y", strict=False).alias("sort_col")
            ]).sort("sort_col").drop("sort_col")

            # Fill nulls and format amounts as currency
            monthly_totals = monthly_totals.fill_null(0).with_columns([
                pl.col("Total Submitted Amount").map_elements(lambda x: f"₦{x:,.2f}"),
                pl.col("Total Encountered Amount").map_elements(lambda x: f"₦{x:,.2f}")
            ])

            # Display totals in metrics
            col1, col2 = st.columns(2)
            with col1:
                total_submitted = claims.select(pl.col("approvedamount").sum()).collect().item()  # Collect before item()
                st.metric("Total Submitted Amount", f"₦{total_submitted:,.2f}")
            with col2:
                total_encountered = enc_claims.select(pl.col("approvedamount").sum()).collect().item()  # Collect before item()
                st.metric("Total Encountered Amount", f"₦{total_encountered:,.2f}")

            # Display monthly breakdown table
            st.subheader("Monthly Claims Breakdown")
            st.dataframe(monthly_totals, use_container_width=True)

        except Exception as e:
            st.error(f"Error processing monthly totals: {str(e)}")
            # Fallback to displaying unsorted data
            st.warning("Displaying unsorted monthly data due to sorting error")
            monthly_totals = monthly_totals.fill_null(0).with_columns([
                pl.col("Total Submitted Amount").map_elements(lambda x: f"₦{x:,.2f}"),
                pl.col("Total Encountered Amount").map_elements(lambda x: f"₦{x:,.2f}")
            ])
            st.dataframe(monthly_totals, use_container_width=True)

        print(monthly_totals["Month"].unique())

        # Section 3: PA(40%) Analysis
        st.subheader("MLR Analysis")

        # Calculate PA_mlr and claims_mlr
        PA_mlr = calculate_pa_mlr().collect() if isinstance(calculate_pa_mlr(), pl.LazyFrame) else calculate_pa_mlr()
        claims_mlr = calculate_claims_mlr().collect() if isinstance(calculate_claims_mlr(), pl.LazyFrame) else calculate_claims_mlr()
        
        # Store in session state for future use
        st.session_state.PA_mlr = PA_mlr
        st.session_state.claims_mlr = claims_mlr

        # Process debit notes first
        valid_debits, combined_data, invalid_debit, invalid_contract = process_debit_notes()
        if valid_debits is None or valid_debits.height == 0:
            st.error("No valid debit notes found. Please check your data.")
            return

        # Store valid_debits in session state
        st.session_state.valid_debits = valid_debits

        # Get revenue metrics
        metrics = revenue_pa()
        if not metrics:
            st.error("Unable to load revenue metrics")
            return

        # Get payment analysis data
        payment_analysis = metrics['current_month']['payment_analysis']
        
        # Ensure we have the correct columns
        if not isinstance(payment_analysis, pl.DataFrame):
            st.error("Payment analysis data is not in the expected format. Please check the data structure.")
            return
        
        # Convert payment_analysis to pandas if it's a Polars DataFrame
        payment_analysis_pandas = payment_analysis.to_pandas()
        
        
        # Check if PA_mlr and claims_mlr are empty
        if PA_mlr.height == 0:
            st.warning("No PA MLR data available")
            return
        if claims_mlr.height == 0:
            st.warning("No Claims MLR data available")
            return
            
        # Convert MLR DataFrames to pandas
        pa_mlr_pandas = PA_mlr.to_pandas()
        claims_mlr_pandas = claims_mlr.to_pandas()
        
        
        # Check if we need to rename columns for merging
        if 'CompanyName' in payment_analysis_pandas.columns:
            payment_analysis_pandas = payment_analysis_pandas.rename(columns={'CompanyName': 'groupname'})
        
        # Perform the merges
        try:
            # First check if the required columns exist
            if 'groupname' not in pa_mlr_pandas.columns:
                st.error(f"Missing 'groupname' column in PA MLR data. Available columns: {pa_mlr_pandas.columns.tolist()}")
                return
            if 'groupname' not in claims_mlr_pandas.columns:
                st.error(f"Missing 'groupname' column in Claims MLR data. Available columns: {claims_mlr_pandas.columns.tolist()}")
                return
            if 'groupname' not in payment_analysis_pandas.columns:
                st.error(f"Missing 'groupname' column in Payment Analysis data. Available columns: {payment_analysis_pandas.columns.tolist()}")
                return
            
            # Perform the merges
            pa_mlr_pandas = pa_mlr_pandas.merge(
                payment_analysis_pandas[['groupname', 'DebitAmount']],
                on='groupname',
                how='left'
            )
            
            claims_mlr_pandas = claims_mlr_pandas.merge(
                payment_analysis_pandas[['groupname', 'DebitAmount']],
                on='groupname',
                how='left'
            )
            
        except Exception as e:
            st.error(f"Error during merge: {str(e)}")
            st.error(f"Available columns in payment_analysis_pandas: {payment_analysis_pandas.columns.tolist()}")
            st.error(f"Available columns in pa_mlr_pandas: {pa_mlr_pandas.columns.tolist()}")
            st.error(f"Available columns in claims_mlr_pandas: {claims_mlr_pandas.columns.tolist()}")
            return
        
        # Calculate commission (10% of DebitAmount)
        pa_mlr_pandas['Commission'] = pa_mlr_pandas['DebitAmount'] * 0.10
        claims_mlr_pandas['Commission'] = claims_mlr_pandas['DebitAmount'] * 0.10
        
        # Calculate MLR for PA data: (PA(40%) + commission) / DebitAmount * 100
        pa_mlr_pandas['MLR'] = pa_mlr_pandas.apply(
            lambda row: ((row['PA(40%)'] + row['Commission']) / row['DebitAmount'] * 100) 
            if pd.notnull(row['DebitAmount']) and row['DebitAmount'] > 0 
            else None, 
            axis=1
        )
        
        # Calculate MLR for Claims data: (Total cost + commission) / DebitAmount * 100
        claims_mlr_pandas['MLR'] = claims_mlr_pandas.apply(
            lambda row: ((row['Total cost'] + row['Commission']) / row['DebitAmount'] * 100) 
            if pd.notnull(row['DebitAmount']) and row['DebitAmount'] > 0 
            else None, 
            axis=1
        )
        
        # Format numeric columns
        numeric_columns = ['Total cost', 'PA(40%)', 'DebitAmount', 'Commission', 'MLR']
        for col in numeric_columns:
            if col in pa_mlr_pandas.columns:
                pa_mlr_pandas[col] = pd.to_numeric(pa_mlr_pandas[col], errors='coerce')
                pa_mlr_pandas[col] = pa_mlr_pandas[col].apply(
                    lambda x: f'₦{x:,.2f}' if pd.notnull(x) else 'N/A'
                )
            if col in claims_mlr_pandas.columns:
                claims_mlr_pandas[col] = pd.to_numeric(claims_mlr_pandas[col], errors='coerce')
                claims_mlr_pandas[col] = claims_mlr_pandas[col].apply(
                    lambda x: f'₦{x:,.2f}' if pd.notnull(x) else 'N/A'
                )
        
        # Add % sign to MLR values
        pa_mlr_pandas['MLR'] = pa_mlr_pandas['MLR'].apply(
            lambda x: f'{x}%' if x != 'N/A' else x
        )
        claims_mlr_pandas['MLR'] = claims_mlr_pandas['MLR'].apply(
            lambda x: f'{x}%' if x != 'N/A' else x
        )

        # Add switch button to toggle between tables
        show_pa_table = st.toggle("Show PA Table", value=True)

        if show_pa_table:
            if not pa_mlr_pandas.empty:
                # Create a copy of the dataframe to avoid modifying the original
                display_df = pa_mlr_pandas[['groupname', 'PA(40%)', 'DebitAmount', 'Commission', 'MLR']].copy()
                
                st.write("### PA Data")
                st.dataframe(
                    display_df,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("No data available for PA analysis.")
        else:
            if not claims_mlr_pandas.empty:
                # Create a copy of the dataframe to avoid modifying the original
                display_dff = claims_mlr_pandas[['groupname', 'Total cost', 'DebitAmount', 'Commission', 'MLR']].copy()
                
                st.write("### Claims Data")
                st.dataframe(
                    display_dff,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.warning("No data available for Claims analysis.")


        # Calculate average costs per enrollee
        average_costs = calculate_average_costs_per_enrollee()
        
        # Display Average Claims Cost per Enrollee
        claims_cost = average_costs.get('claims_cost_per_enrollee', 0)
        st.metric(
            label="Claims Cost per Enrollee (Current Year)",
            value=f"₦{claims_cost:,.2f}"
        )

        # Display Average PA Cost per Enrollee
        pa_cost = average_costs.get('pa_cost_per_enrollee', 0)
        st.metric(
            label="PA Cost per Enrollee (Current Year)",
            value=f"₦{pa_cost:,.2f}"
        )

        # Section 4: Active Plans Visualization
        st.subheader("Active Plans Distribution")
        
        # Get active plans data
        active_plan, merged_PA, claims_mp = prepare_active_plans()
        
        if active_plan is not None and not active_plan.is_empty():
            # Store in session state for future use
            st.session_state.active_plan = active_plan
            
            # Convert to Pandas DataFrame for visualization
            active_plans_pd = active_plan.to_pandas()
            
            # Create a pie chart using Plotly
            fig = px.pie(
                active_plans_pd,
                values='count',
                names='price_category',
                title='Distribution of Enrollees Across Price Categories',
                labels={'price_category': 'Price Category', 'count': 'Number of Enrollees'}
            )
            
            # Display the pie chart
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("No active plans data available.")

        #Section 4b Plan breakdown
        st.title("Price Category Analysis Dashboard")
        
        # Get data
        data, metrics = get_data()
        active_plan, merged_PA, claims_mp = prepare_active_plans()
        
        if merged_PA is not None and not merged_PA.is_empty():
            # Store in session state
            st.session_state.active_plan = active_plan
            st.session_state.merged_PA = merged_PA

            # Convert to pandas and filter for 2025
            df = merged_PA.to_pandas()
            df = df[df['year'] == 2025]
            
            # Create summary DataFrame for AgGrid
            summary_df = df.groupby('price_category')['granted'].sum().reset_index()
            summary_df.columns = ['Price Category', 'Total Granted Amount']

            # Format the 'Total Granted Amount' column with commas
            summary_df['Total Granted Amount'] = summary_df['Total Granted Amount'].apply(lambda x: '{:,}'.format(x))

            gb = GridOptionsBuilder.from_dataframe(summary_df)
            gb.configure_selection(selection_mode='multiple', use_checkbox=True)
            gb.configure_column('Price Category', headerCheckboxSelection=True)
            gb.configure_column('Total Granted Amount', type=["numericColumn", "numberColumnFilter", "customNumericFormat"], valueFormatter="data.Total Granted Amount.toLocaleString('en-US', {style: 'currency', currency: 'USD', minimumFractionDigits: 0})")

            grid_options = gb.build()
            grid_response = AgGrid(
                summary_df,
                gridOptions=grid_options,
                update_mode=GridUpdateMode.SELECTION_CHANGED,
                fit_columns_on_grid_load=True,
                height=300,
                allow_unsafe_jscode=True
            )

            selected_rows = grid_response['selected_rows']

            # Check if selected_rows is None or empty
            if selected_rows is None or (hasattr(selected_rows, 'empty') and selected_rows.empty):
                st.info("Select one or more price categories from the table above to view the group breakdown")
            else:
                try:
                    # If rows are selected, process them
                    # Convert selected_rows to a list of dictionaries if it's not already
                    if isinstance(selected_rows, pd.DataFrame):
                        selected_categories = selected_rows['Price Category'].tolist()
                    else:
                        selected_categories = [row['Price Category'] for row in selected_rows]
                    
                    filtered_df = df[df['price_category'].isin(selected_categories)]
                    
                    if not filtered_df.empty:
                        grouped_df = filtered_df.groupby('groupname')['granted'].sum().reset_index()
                        grouped_df.columns = ['Group Name', 'Total Granted Amount']
                        
                        # Format the Total Granted Amount column
                        grouped_df['Total Granted Amount'] = grouped_df['Total Granted Amount'].apply(lambda x: f'₦{x:,.2f}')

                        st.subheader("Total Granted Amount by Group for Selected Price Categories")
                        st.dataframe(grouped_df, use_container_width=True)

                    else:
                        st.warning("No data found for the selected categories")
                
                except Exception as e:
                    st.error(f"Error processing selected rows: {str(e)}")
                    st.error(traceback.format_exc())

        # Get online PA usage statistics
        try:
            # Get online PA usage statistics
            pa_daily_stats, pa_yearly_stats, Online_pa = online_pa_usage()
            
            # Validate the returned data
            if pa_daily_stats is None or pa_yearly_stats is None or Online_pa is None:
                st.error("Failed to load online PA data. Please check your data sources.")
                return
                
            # Check if any of the DataFrames are empty
            if pa_daily_stats.height == 0 or pa_yearly_stats.height == 0 or Online_pa.height == 0:
                st.warning("No online PA usage data available. Please check your data sources.")
                return
                
            st.session_state.Online_pa = Online_pa

            # 1. Line Chart: Percentage of Online PA vs Total PA for each Provider per day (last 14 days)
            st.header("Percentage of Online PA vs Total PA (Last 14 Days)")
            daily_df = pa_daily_stats.to_pandas()

            # Sidebar Date Filter
            st.sidebar.header("Filter by Date Range for Online PA")
            start_date = st.sidebar.date_input("Start Date", date.today().replace(month=1, day=1))
            end_date = st.sidebar.date_input("End Date", date.today())

            # Filter Data
            filtered_df = Online_pa.filter(
                (pl.col("RequestDate") >= pl.lit(start_date)) &
                (pl.col("RequestDate") <= pl.lit(end_date))
            )

            # Compute Unique PANumber Count
            unique_pa_count = filtered_df.select(pl.col("PANumber").n_unique()).to_series()[0]

            # Compute Response Time (ResolutionTime - DateAdded)
            filtered_df = filtered_df.with_columns(
                (pl.col("ResolutionTime") - pl.col("DateAdded")).alias("ResponseTime")
            )

            # Convert ResponseTime to Minutes
            filtered_df = filtered_df.with_columns(
                (pl.col("ResponseTime").dt.total_seconds() / 60).alias("ResponseTimeMins")
            )

            filtered_df = filtered_df.with_columns(
                pl.col("ResponseTimeMins").fill_null(0)
            )

            # Compute Average Response Time
            average_response_time = filtered_df.select(pl.col("ResponseTimeMins").mean()).to_series()[0]

            # Top 10 Providers by Unique PANumber Count
            top_providers = (
                filtered_df.group_by("Providerid")
                .agg(pl.col("PANumber").n_unique().alias("UniquePANumberCount"))
                .sort("UniquePANumberCount", descending=True)
                .limit(10)
                .to_pandas()
            )

            # Categorizing Response Times for Bar Chart
            response_bins = filtered_df.with_columns([
                pl.when(pl.col("ResponseTimeMins") < 2)
                .then(pl.lit("Less than 2 mins"))
                .when(pl.col("ResponseTimeMins") < 5)
                .then(pl.lit("2 - 5 mins"))
                .when(pl.col("ResponseTimeMins") < 10)
                .then(pl.lit("5 - 10 mins"))
                .otherwise(pl.lit("More than 10 mins"))
                .alias("ResponseCategory")
            ])

            response_count = (
                response_bins.group_by("ResponseCategory")
                .agg(pl.col("PANumber").n_unique().alias("Count"))
                .sort("ResponseCategory")
                .to_pandas()
            )

            # Display Metrics
            st.metric("Total Unique PANumber", unique_pa_count)
            st.metric("Average Response Time (mins)", round(average_response_time, 2))

            # Display Top Providers Table
            st.subheader("Top 10 Providers by Unique PANumber")
            st.dataframe(top_providers)

            # Bar Chart for Response Time Categories
            st.subheader("Response Time Distribution")
            st.bar_chart(response_count.set_index("ResponseCategory"))

            # Display Daily and Yearly Stats Tables
            st.header("Percentage of Online PA vs Total PA (Last 14 Days)")
            if not daily_df.empty:
                st.dataframe(daily_df)
            else:
                st.write("No data available for the last 14 days.")
            
            st.header("Online PA vs Total PA Percentage per Provider (This Year)")
            yearly_df = pa_yearly_stats.to_pandas()
            if not yearly_df.empty:
                st.dataframe(yearly_df)
            else:
                st.write("No data available for yearly PA.")

        except Exception as e:
            st.error(f"Error processing online PA data: {str(e)}")
            st.error(traceback.format_exc())

        # Section 5: Benefit Summary Visualization
        st.subheader("Benefit Summary Analysis")
        try:

            claims_with_benefit = benefit(selected_group=None, excluded_group='1799', start_date=None, end_date=None)


            # Check if benefit_summary exists in session state
            if 'claims_with_benefit' in st.session_state:
                claims_with_benefit = benefit(selected_group=None, excluded_group='1799', start_date=None, end_date=None)
                claims_with_benefit= st.session_state.claims_with_benefit

                # Convert benefit_summary to Pandas DataFrame for easier manipulation
                benefit_summary_pd = claims_with_benefit.to_pandas()

                # Add a date range filter
                st.write("### Date Range Filter")
                col1, col2 = st.columns(2)
                with col1:
                    start_date = st.date_input(
                        "Start Date",
                        value=datetime(2024, 1, 1).date(),
                        format="YYYY-MM-DD",
                        key="benefit_start_date"
                    )
                with col2:
                    end_date = st.date_input(
                        "End Date",
                        value=datetime.now().date(),
                        format="YYYY-MM-DD",
                        key="benefit_end_date"
                    )

                # Add a group filter dropdown with "All Groups" option
                st.write("### Group Filter")
                all_groups = benefit_summary_pd['groupname'].unique().tolist()
                all_groups.sort() # Sort alphabetically for easier finding
                
                # Add "All Groups" as the first option
                group_options = ["All Groups"] + all_groups
                selected_group = st.selectbox(
                    "Select Group to Analyze",
                    options=group_options,
                    index=0,  # Default to "All Groups"
                    key="group_filter"
                )
                
                # Convert selection to list of groups for filtering
                selected_groups = all_groups if selected_group == "All Groups" else [selected_group]

                # Filter the data based on selected groups and date range
                filtered_data = benefit_summary_pd[
                    (benefit_summary_pd['groupname'].isin(selected_groups)) &
                    (benefit_summary_pd['date'] >= pd.to_datetime(start_date)) &
                    (benefit_summary_pd['date'] <= pd.to_datetime(end_date))
                ]

                # Group by Benefit and calculate total cost
                grouped_data = filtered_data.groupby('Benefit')['cost'].sum().reset_index()

                # Create a bar chart using Plotly
                st.write("### Benefit Summary Bar Chart")
                fig = px.bar(
                    grouped_data,
                    x='Benefit',
                    y='cost',
                    title='Total Cost by Benefit',
                    labels={'Benefit': 'Benefit Type', 'Total Cost': 'Total Cost (₦)'},
                    template='plotly_white'
                )

                # Update layout for better visualization
                fig.update_layout(
                    xaxis_tickangle=-45,
                    height=500,
                    xaxis_title="Benefit Type",
                    yaxis_title="Total Cost (₦)",
                    showlegend=False
                )

                # Display the bar chart
                st.plotly_chart(fig, use_container_width=True)
            
            else:
                st.warning("No benefit summary data available. Please ensure the data is loaded correctly.")
        except Exception as e:
                st.error(f"Error processing benefit")
                st.error(traceback.format_exc()) 

        #ENROLLEE COMPARISON
        try:
            table1, table2, table3 = enrollee_comparison()
            st.session_state.table1 = table1
            st.session_state.table2 = table2
            st.session_state.table3 = table3

            st.write("ENROLLEE COUNT- CBA VS HR")
            st.dataframe(table1)
            
            st.write("UNIQUE COUNT OF ENROLLEE")
            st.dataframe(table2)

            st.write("DETAILED ENROLLEE COMPARISON")
            st.dataframe(table3)


        except Exception as e:
                st.error(f"Error processing benefit")
                st.error(traceback.format_exc())    


    except Exception as e:
        st.error(f"Error in main: {str(e)}")
        st.error(traceback.format_exc())
 

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
    
if __name__ == "__main__":
    main()