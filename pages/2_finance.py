import streamlit as st
from st_aggrid import AgGrid
import polars as pl
import plotly.express as px
import plotly.graph_objs as go
import numpy as np
from datetime import datetime, timedelta
import traceback
import pandas as pd
import sys
from utils.data_loader import revenue_pa, initialize_data, process_debit_notes, auto_invoice
from utils.requires_data_loading import requires_data_loading

def display_revenue_metrics():
    """Display revenue metrics and visualizations."""
    try:
        # Initialize data if needed
        if 'data' not in st.session_state:
            st.warning("Initializing data...")
            initialize_data()
        
        # Get fresh metrics
        st.info("Fetching revenue metrics...")
        metrics = revenue_pa()
        if not metrics:
            st.error("Unable to load revenue metrics")
            return
            
        # Get timestamp for debugging
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        st.sidebar.info(f"Last reload: {current_time}")
            
        # Create three columns for key metrics
        col1, col2, col3= st.columns(3)
        
        # Current month receivables
        with col1:
            try:
                st.metric(
                    f"📈 {metrics['current_month']['month']} Receivables",
                    f"₦{metrics['current_month']['receivables']:,.2f}"
                )
            except (KeyError, TypeError) as e:
                st.error(f"Error displaying receivables: {str(e)}")
        
        # Current month PA cost
        with col2:
            try:
                st.metric(
                    f"💉 {metrics['current_month']['month']} PA Cost",
                    f"₦{metrics['current_month']['pa_cost']:,.2f}"
                )
            except (KeyError, TypeError) as e:
                st.error(f"Error displaying PA cost: {str(e)}")
        
        # Current month allocated premium
        with col3:
            try:
                # Get current month name
                current_month = metrics['current_month']['month']
                
                # Sum the current month's allocated premium
                current_month_premium = sum(
                    item[current_month] for item in metrics['current_month']['allo_premium']
                    if current_month in item
                )
                
                st.metric(
                    f"💰 {metrics['current_month']['month']} Allocated Premium",
                    f"₦{current_month_premium:,.2f}"
                )
            except (KeyError, TypeError) as e:
                st.error(f"Error displaying allocated premium: {str(e)}")
        
        # Create tab for this month's payers
        tab1 = st.tabs(["This Month's Payers"])[0]
        
        # Tab 1: This Month's Payers
        with tab1:
            try:
                st.subheader(f"Expected Payments for {metrics['current_month']['month']}")
                if 'next_month' in metrics and 'payers' in metrics['next_month']:
                    payers_df = pl.DataFrame(metrics['next_month']['payers'])
                    
                    # Format the amount column with currency symbol and commas
                    month_col = metrics['current_month']['month']
                    if month_col in payers_df.columns:
                        payers_df = payers_df.with_columns([
                            pl.col(month_col).map_elements(lambda x: f"₦{x:,.2f}").alias(month_col)
                        ])
                    
                    st.dataframe(payers_df, use_container_width=True)
                else:
                    st.warning("No payers data available for next month")
            except Exception as e:
                st.error(f"Error displaying payers: {str(e)}")
        
        # Add Financial Summary Table
        st.subheader("📊 Financial Summary 2025")
        
        try:
            # Get GLL data from metrics
            if 'current_month' in metrics:
                GLT = metrics['current_month'].get('General Ledger')
                if GLT is not None:
                    payment_analysis = pl.DataFrame(metrics['current_month'].get('payment_analysis', []))
                    if 'data' in st.session_state and 'revenue_data' in st.session_state.data:
                        revenue_data = st.session_state.data['revenue_data']
                        
                        st.dataframe(payment_analysis)
                        # Get invoice data for analysis but don't display it
                        G_PLAN_with_date = auto_invoice()
                        if isinstance(G_PLAN_with_date, pl.DataFrame) and not G_PLAN_with_date.is_empty():
                            INVOICE = G_PLAN_with_date
                            st.session_state.INVOICE = INVOICE  # Store in session state for later use
                        else:
                            st.warning("No invoice data available. Please check if all required data files are loaded correctly.")
                            st.info("Required files: GROUP_CONTRACT, G_PLAN, GROUPS, PLANS")
                            return
                    else:
                        st.warning("Revenue data not found in session state")
                else:
                    st.warning("General Ledger data not available")
            else:
                st.warning("Current month data not available")
                return
                
            #NEGATE AMOUNT
            # First ensure GLAmount is numeric
            GLT = GLT.with_columns(
                pl.col("GLAmount").cast(pl.Float64, strict=False)
            )

            # Convert to absolute values
            GLT = GLT.with_columns(
                pl.col("GLAmount").abs().alias("GLAmount")
            )
            
            # Extract year from GLDate and filter for 2025
            GLT = GLT.with_columns(
                pl.col("GLDate").dt.year().alias("Year")
            )
            
            # Filter GLT25 to only include rows where Year = 2025
            GLT25 = GLT.filter(pl.col("Year") == 2025)
            GLT25CASH = GLT25.filter(pl.col("Year") == 2025)
            
            # Calculate cash receivables (from revenue_data)
            month_cols = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE', 
                          'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
            cash_receivables = revenue_data.select([
                pl.col(month).sum() for month in month_cols
            ]).row(0)

            Cash_keywords = ["POLARIS BANK 1", "KEYSTONE BANK", "MICROCRED MFB", "ASTRAPOLARIS MFB", "ALERT MICRO BANK", "MONEYFIELD MFB", "KAYVEE  MFB" ]
            
            # Calculate cash received (CORPORATE PREMIUM RECEIVABLE)
            cash_received = GLT25CASH.filter(
                pl.col('GLDesc').is_in(Cash_keywords)
            ).group_by('Month').agg(
                pl.col('GLAmount').sum().alias('Cash_Received')
            )
            
            # Calculate expenses
            expense_keywords = [
                'EXPENSES', 'expenses',
                'OFFICE EQUIPMENT',
                'PLANT & MACHINERY', 
                'MOTOR VECHICLE',
                'FURNITURE & FITTING',
                'ELECTRICITY & RATES',
                'PRINTING & STATIONERY', 
                'REPAIRS & MAINTENANCE',
                'GENERATOR RUNNING',
                'MOTOR RUNNING EXPENSES',
                'TRANSPORT & TRAVELLING'
            ]
            expenses = GLT25.filter(
                pl.col('DESCRIPTION').str.to_lowercase().str.contains('expenses') |
                pl.col('DESCRIPTION').is_in(expense_keywords)
            ).group_by('Month').agg(
                pl.col('GLAmount').sum().alias('Expenses')
            )
            
            # Calculate salary related expenses
            salary_keywords = ['DIRECTORS ALLOWANCE', 'PERSONNEL EMOLUMENTS', 
                              'MANAGEMENT ALLOWANCE', 'LEAVE ALLOWANCE', 'STAFF WELFARE']
            salary = GLT25.filter(
                pl.col('DESCRIPTION').is_in(salary_keywords)
            ).group_by('Month').agg(
                pl.col('GLAmount').sum().alias('Salary')
            )

            # Calculate medical refund related expenses
            medical_refund_keywords = ['MEDICAL REFUND PAYABLE']
            medical_refund= GLT25.filter(
                pl.col('DESCRIPTION').is_in(medical_refund_keywords)
            ).group_by('Month').agg(
                pl.col('GLAmount').sum().alias('Medical refund'))
            

            # Calculate commision related expenses
            commission_keywords = ['ACCRUED COMMISSION']
            commission= GLT25.filter(
                pl.col('DESCRIPTION').is_in(commission_keywords)
            ).group_by('Month').agg(
                pl.col('GLAmount').sum().alias('Commission'))

            
            # Create summary DataFrame
            summary_data = []
            for month_num, month_name in enumerate(month_cols, 1):
                # Get the values using item() or collecting to list and accessing first element
                cash_received_value = cash_received.filter(pl.col('Month') == month_num)['Cash_Received'].to_list()[0] if cash_received.filter(pl.col('Month') == month_num).height > 0 else 0
                expenses_value = expenses.filter(pl.col('Month') == month_num)['Expenses'].to_list()[0] if expenses.filter(pl.col('Month') == month_num).height > 0 else 0
                salary_value = salary.filter(pl.col('Month') == month_num)['Salary'].to_list()[0] if salary.filter(pl.col('Month') == month_num).height > 0 else 0
                medical_refund_value = medical_refund.filter(pl.col('Month') == month_num)['Medical refund'].to_list()[0] if medical_refund.filter(pl.col('Month') == month_num).height > 0 else 0
                commission_value = commission.filter(pl.col('Month') == month_num)['Commission'].to_list()[0] if commission.filter(pl.col('Month') == month_num).height > 0 else 0
                row = {
                    'Month': month_name,
                    'Cash Receivables': cash_receivables[month_num - 1],
                    'Cash Received': cash_received_value,
                    'Expenses': expenses_value,
                    'Salary': salary_value,
                    'Medical refund' : medical_refund_value,
                    'Commission' : commission_value
                }
                summary_data.append(row)
            
            summary_df = pl.DataFrame(summary_data)
            
            # Display the table with formatting
            st.dataframe(
                summary_df,
                use_container_width=True,
                column_config={
                    'Cash Receivables': st.column_config.NumberColumn(format="₦%.2f"),
                    'Cash Received': st.column_config.NumberColumn(format="₦%.2f"),
                    'Expenses': st.column_config.NumberColumn(format="₦%.2f"),
                    'Salary': st.column_config.NumberColumn(format="₦%.2f")
                }
            )
            #DEBIT ANALYSIS
            st.subheader("💳 DEBIT Notes Analysis")

            # Process debit notes with fresh data
            valid_debits, combined_data, invalid_debit, invalid_contract = process_debit_notes()
            
            # Display valid debits
            st.markdown("#### ✅ Valid DEBIT Notes")
            if valid_debits.height > 0:
                st.dataframe(
                    valid_debits,
                    use_container_width=True,
                    column_config={
                        "Amount": st.column_config.NumberColumn(format="₦%.2f")
                    }
                )
            else:
                st.info("No valid DEBIT notes found")
                
            # Display grouped debits
            st.markdown("#### 📊 DEBIT Notes Summary by Company")
            if combined_data.height > 0:
                st.dataframe(
                    combined_data,
                    use_container_width=True,
                    column_config={
                        "Total_Amount": st.column_config.NumberColumn(format="₦%.2f")
                    }
                )
            else:
                st.info("No grouped DEBIT data available")
                
            # Display invalid debits
            st.markdown("#### ❌ Invalid DEBIT Notes")
            if invalid_debit.height > 0:
                st.dataframe(
                    invalid_debit,
                    use_container_width=True,
                    column_config={
                        "Amount": st.column_config.NumberColumn(format="₦%.2f")
                    }
                )
            else:
                st.info("No invalid DEBIT notes found")

            # Display invalid contract
            st.markdown("#### ❌ Invalid Contract")
            if invalid_contract.height > 0:
                # Select only the required columns
                filtered_contract = invalid_contract.select([
                    "groupname", "startdate", "enddate"
                ])
                st.dataframe(
                    filtered_contract,
                    use_container_width=True
                )
            else:
                st.info("No invalid contract found")
        except Exception as e:
            st.error(f"Error processing revenue metrics: {str(e)}")
            st.exception(e)
    except Exception as e:
        st.error(f"Error processing revenue metrics: {str(e)}")
        st.exception(e)

def main():
    st.title("📊 Financial Analytics")
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
    
    # Call the display_revenue_metrics function to show the content
    display_revenue_metrics()

if __name__ == "__main__":
    main()
