import streamlit as st
from datetime import datetime
import polars as pl
import threading
import time
from pathlib import Path
import logging
from .data_loader import load_excel_data, calculate_base_metrics, calculate_pa_mlr, calculate_claims_mlr, pharmacy_carecord, online_pa_usage, client_analysis, enrollee_comparison, benefit_limit
from .data_loader import prepare_claims_comparison, prepare_active_plans, revenue_pa, process_debit_notes, auto_invoice

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self._thread = None
        self._stop_flag = False
        self._last_load_time = None
        self._load_interval = 3600  # Refresh every hour
        
    def start(self):
        """Start the background loading thread"""
        if self._thread is None or not self._thread.is_alive():
            self._stop_flag = False
            self._thread = threading.Thread(target=self._background_task, daemon=True)
            self._thread.start()
            logger.info("Background loader started")

    def stop(self):
        """Stop the background loading thread"""
        self._stop_flag = True
        if self._thread is not None:
            self._thread.join()
            logger.info("Background loader stopped")

    def _background_task(self):
        """Background task to load data periodically"""
        while not self._stop_flag:
            try:
                # Check if we need to reload
                current_time = time.time()
                if (self._last_load_time is None or 
                    current_time - self._last_load_time >= self._load_interval):
                    self._load_data()
                    self._last_load_time = current_time
                
                # Sleep for a short interval before checking again
                time.sleep(10)
                
            except Exception as e:
                logger.error(f"Error in background task: {str(e)}")
                time.sleep(60)  # Wait longer after an error

    def _load_data(self):
        """Load all data sources"""
        try:

            # Initialize progress in session state if not exists
            if 'data_loading_progress' not in st.session_state:
                st.session_state.data_loading_progress = 0

            logger.info("Starting data load")
            
            # Load Excel data
            data = load_excel_data()
            st.session_state.data_loading_progress = 25
            
            # Store data in session state
            st.session_state.data = data

            # Process debit notes
            valid_debit, DEBIT_grouped, invalid_debit, invalid_contract = process_debit_notes()
            st.session_state.valid_debit = valid_debit
            st.session_state.DEBIT_grouped = DEBIT_grouped
            st.session_state.invalid_debit = invalid_debit
            st.session_state.invalid_contract = invalid_contract
            
            # Calculate base metrics
            st.session_state.base_metrics = calculate_base_metrics(
                data['PA'],
                data['CLAIMS'],
                data['PROVIDER'],
                data['ACTIVE_ENROLLEE'],
                data['GROUP_CONTRACT'],
                data['GROUP_COVERAGE']
            )
            st.session_state.data_loading_progress = 50
            
            # Calculate additional metrics
            try:
                st.session_state.PA_mlr = calculate_pa_mlr()
                st.session_state.claims_mlr = calculate_claims_mlr()
                st.session_state.claims_comparison = prepare_claims_comparison(data) 
            except Exception as e:
                logger.error(f"Error calculating base metrics: {str(e)}")
                st.session_state.PA_mlr = None
                st.session_state.claims_mlr = None
                st.session_state.claims_comparison = None
                
            # Handle pharmacy_carecord
            try:
                pa_benefit, all_pa, all_claims = pharmacy_carecord()
                st.session_state.pa_benefit = pa_benefit
                st.session_state.all_pa = all_pa
                st.session_state.all_claims = all_claims
            except ValueError as e:
                logger.warning(f"Error in pharmacy_carecord: {str(e)}")
                st.session_state.pa_benefit = pl.DataFrame()
                st.session_state.all_pa = pl.DataFrame()
                st.session_state.all_claims = pl.DataFrame()
            
            st.session_state.data_loading_progress = 75
            
            # Calculate remaining metrics
            try:
                st.session_state.revenue_pa = revenue_pa()
            except Exception as e:
                logger.warning(f"Error in revenue_pa: {str(e)}")
                st.session_state.revenue_pa = None
            
            # Handle online_pa_usage
            try:
                pa_daily_stats, pa_yearly_stats, Online_pa = online_pa_usage()
                st.session_state.pa_yearly_stats = pa_yearly_stats
                st.session_state.pa_daily_stats = pa_daily_stats
                st.session_state.Online_pa = Online_pa
            except ValueError as e:
                logger.warning(f"Error in online_pa_usage: {str(e)}")
                st.session_state.pa_yearly_stats = pl.DataFrame()
                st.session_state.pa_daily_stats = pl.DataFrame()
                st.session_state.Online_pa = pl.DataFrame()
            
            # Handle prepare_active_plans
            try:
                active_plan, merged_PA, claims_mp = prepare_active_plans()
                st.session_state.active_plan = active_plan
                st.session_state.merged_PA = merged_PA
                st.session_state.claims_mp = claims_mp
            except ValueError as e:
                logger.warning(f"Error in prepare_active_plans: {str(e)}")
                st.session_state.active_plan = pl.DataFrame()
                st.session_state.merged_PA = pl.DataFrame()
                st.session_state.claims_mp = pl.DataFrame()
            
            # Handle enrollee_comparison
            try:
                table1, table2, table3 = enrollee_comparison()
                st.session_state.table1 = table1
                st.session_state.table2 = table2
                st.session_state.table3 = table3
            except ValueError as e:
                logger.warning(f"Error in enrollee_comparison: {str(e)}")
                st.session_state.table1 = pl.DataFrame()
                st.session_state.table2 = pl.DataFrame()
                st.session_state.table3 = pl.DataFrame()
            
            # Handle client_analysis
            try:
                main_dff, ledger_result, debit, groupname_plan = client_analysis()
                st.session_state.main_dff = main_dff
                st.session_state.ledger_result = ledger_result
                st.session_state.debit = debit
                st.session_state.groupname_plan = groupname_plan
            except ValueError as e:
                logger.warning(f"Error in client_analysis: {str(e)}")
                st.session_state.main_dff = pl.DataFrame()
                st.session_state.ledger_result = pl.DataFrame()
                st.session_state.debit = pl.DataFrame()
                st.session_state.groupname_plan = pl.DataFrame()
            
            # Handle benefit_limit
            try:
                BENEFIT_PA, claims_BENEFIT = benefit_limit()
                st.session_state.BENEFIT_PA = BENEFIT_PA
                st.session_state.claims_BENEFIT = claims_BENEFIT
            except ValueError as e:
                logger.warning(f"Error in benefit_limit: {str(e)}")
                st.session_state.BENEFIT_PA = pl.DataFrame()
                st.session_state.claims_BENEFIT = pl.DataFrame()
            
            # Handle auto_invoice
            try:
                st.session_state.G_PLAN_with_dates = auto_invoice()
            except Exception as e:
                logger.warning(f"Error in auto_invoice: {str(e)}")
                st.session_state.G_PLAN_with_dates = pl.DataFrame()
            
            st.session_state.data_loading_progress = 100
            
            # Store last update time
            st.session_state.last_update_time = datetime.now()
            st.session_state.last_heartbeat = datetime.now()
            
            logger.info("Data load completed successfully")
            
        except Exception as e:
            logger.error(f"Error in data load: {str(e)}")
            st.session_state.data_loading_progress = -1
            raise

    def force_refresh(self):
        """Force a refresh of the data"""
        try:
            self._load_data()
            return True
        except Exception as e:
            logger.error(f"Error during forced refresh: {str(e)}")
            return False
