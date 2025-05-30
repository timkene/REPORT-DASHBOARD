# utils.py (create if you don't have one)
import streamlit as st
from functools import wraps
from utils.data_loader import initialize_data

def requires_data_loading(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        if 'data' not in st.session_state or 'base_metrics' not in st.session_state:
            st.warning("Loading required data, please wait...")
            initialize_data()
            st.rerun()
        return func(*args, **kwargs)
    return wrapper