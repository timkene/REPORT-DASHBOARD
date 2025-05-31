import streamlit as st
from datetime import datetime
import polars as pl
import threading
import time
from pathlib import Path
import logging
from .data_loader import (
    load_excel_data, calculate_base_metrics, calculate_pa_mlr, calculate_claims_mlr,
    pharmacy_carecord, online_pa_usage, client_analysis, enrollee_comparison,
    benefit_limit, prepare_claims_comparison, prepare_active_plans, revenue_pa,
    process_debit_notes, auto_invoice
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self._thread = None
        self._stop_flag = False
        self._last_load_time = None
        self._load_interval = 3600  # Refresh every hour
        self.data_cache = {}  # ✅ Store data here instead of st.session_state
        
    def start(self):
        if self._thread is None or not self._thread.is_alive():
            self._stop_flag = False
            self._thread = threading.Thread(target=self._background_task, daemon=True)
            self._thread.start()
            logger.info("Background loader started")

    def stop(self):
        self._stop_flag = True
        if self._thread is not None:
            self._thread.join()
            logger.info("Background loader stopped")

    def _background_task(self):
        while not self._stop_flag:
            try:
                current_time = time.time()
                if (self._last_load_time is None or 
                    current_time - self._last_load_time >= self._load_interval):
                    self._load_data()
                    self._last_load_time = current_time
                time.sleep(10)
            except Exception as e:
                logger.error(f"Error in background task: {str(e)}")
                time.sleep(60)

    def _load_data(self):
        try:
            logger.info("Starting data load")
            cache = {}

            # Load Excel data
            data = load_excel_data()
            cache['data'] = data

            # Process debit notes
            cache['valid_debit'], cache['DEBIT_grouped'], cache['invalid_debit'], cache['invalid_contract'] = process_debit_notes()

            # Calculate base metrics
            cache['base_metrics'] = calculate_base_metrics(
                data['PA'],
                data['CLAIMS'],
                data['PROVIDER'],
                data['ACTIVE_ENROLLEE'],
                data['GROUP_CONTRACT'],
                data['GROUP_COVERAGE']
            )

            # Additional metrics
            try:
                cache['PA_mlr'] = calculate_pa_mlr()
                cache['claims_mlr'] = calculate_claims_mlr()
                cache['claims_comparison'] = prepare_claims_comparison(data)
            except Exception as e:
                logger.error(f"Error calculating base metrics: {str(e)}")
                cache['PA_mlr'] = None
                cache['claims_mlr'] = None
                cache['claims_comparison'] = None

            try:
                pa_benefit, all_pa, all_claims = pharmacy_carecord()
                cache['pa_benefit'] = pa_benefit
                cache['all_pa'] = all_pa
                cache['all_claims'] = all_claims
            except Exception as e:
                logger.warning(f"Error in pharmacy_carecord: {str(e)}")
                cache['pa_benefit'] = pl.DataFrame()
                cache['all_pa'] = pl.DataFrame()
                cache['all_claims'] = pl.DataFrame()

            try:
                cache['revenue_pa'] = revenue_pa()
            except Exception as e:
                logger.warning(f"Error in revenue_pa: {str(e)}")
                cache['revenue_pa'] = None

            try:
                pa_daily_stats, pa_yearly_stats, Online_pa = online_pa_usage()
                cache['pa_yearly_stats'] = pa_yearly_stats
                cache['pa_daily_stats'] = pa_daily_stats
                cache['Online_pa'] = Online_pa
            except Exception as e:
                logger.warning(f"Error in online_pa_usage: {str(e)}")
                cache['pa_yearly_stats'] = pl.DataFrame()
                cache['pa_daily_stats'] = pl.DataFrame()
                cache['Online_pa'] = pl.DataFrame()

            try:
                active_plan, merged_PA, claims_mp = prepare_active_plans()
                cache['active_plan'] = active_plan
                cache['merged_PA'] = merged_PA
                cache['claims_mp'] = claims_mp
            except Exception as e:
                logger.warning(f"Error in prepare_active_plans: {str(e)}")
                cache['active_plan'] = pl.DataFrame()
                cache['merged_PA'] = pl.DataFrame()
                cache['claims_mp'] = pl.DataFrame()

            try:
                table1, table2, table3 = enrollee_comparison()
                cache['table1'] = table1
                cache['table2'] = table2
                cache['table3'] = table3
            except Exception as e:
                logger.warning(f"Error in enrollee_comparison: {str(e)}")
                cache['table1'] = pl.DataFrame()
                cache['table2'] = pl.DataFrame()
                cache['table3'] = pl.DataFrame()

            try:
                main_dff, ledger_result, debit, groupname_plan = client_analysis()
                cache['main_dff'] = main_dff
                cache['ledger_result'] = ledger_result
                cache['debit'] = debit
                cache['groupname_plan'] = groupname_plan
            except Exception as e:
                logger.warning(f"Error in client_analysis: {str(e)}")
                cache['main_dff'] = pl.DataFrame()
                cache['ledger_result'] = pl.DataFrame()
                cache['debit'] = pl.DataFrame()
                cache['groupname_plan'] = pl.DataFrame()

            try:
                BENEFIT_PA, claims_BENEFIT = benefit_limit()
                cache['BENEFIT_PA'] = BENEFIT_PA
                cache['claims_BENEFIT'] = claims_BENEFIT
            except Exception as e:
                logger.warning(f"Error in benefit_limit: {str(e)}")
                cache['BENEFIT_PA'] = pl.DataFrame()
                cache['claims_BENEFIT'] = pl.DataFrame()

            try:
                cache['G_PLAN_with_dates'] = auto_invoice()
            except Exception as e:
                logger.warning(f"Error in auto_invoice: {str(e)}")
                cache['G_PLAN_with_dates'] = pl.DataFrame()

            cache['last_update_time'] = datetime.now()
            cache['last_heartbeat'] = datetime.now()

            self.data_cache = cache  # ✅ Store the loaded data
            logger.info("Data load completed successfully")

        except Exception as e:
            logger.error(f"Error in data load: {str(e)}")
            self.data_cache = {'error': str(e)}  # Save error message to cache

    def get_cached_data(self):
        """Return the cached data safely to main thread"""
        return self.data_cache

    def force_refresh(self):
        try:
            self._load_data()
            return True
        except Exception as e:
            logger.error(f"Error during forced refresh: {str(e)}")
            return False
