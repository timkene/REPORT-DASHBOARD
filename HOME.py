import streamlit as st

# Configure page settings
st.set_page_config(
    page_title="Clearline HMO Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

from utils.background_loader import DataLoader
from datetime import datetime, timedelta
import plotly.graph_objects as go
import time

# Initialize background loader
if 'data_loader' not in st.session_state:
    st.session_state.data_loader = DataLoader()
    st.session_state.data_loader.start()

data_loader = st.session_state.data_loader
cached_data = data_loader.get_cached_data()

# Sync cached data to session state if not already done
if cached_data and 'base_metrics' not in st.session_state:
    for key, value in cached_data.items():
        st.session_state[key] = value

# Check if data is still missing
if 'base_metrics' not in st.session_state:
    st.warning("Waiting for data to load. Please refresh shortly.")
    st.stop()

# Sidebar controls
with st.sidebar:
    if st.button('🔄 Refresh Data'):
        st.cache_data.clear()
        with st.spinner('Refreshing data...'):
            if data_loader.force_refresh():
                for key, value in data_loader.get_cached_data().items():
                    st.session_state[key] = value
                st.rerun()
            else:
                st.error("Data refresh failed.")

    if 'last_update_time' in st.session_state:
        st.caption(f"Last updated: {st.session_state.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")

    heartbeat = st.session_state.get('last_heartbeat')
    if heartbeat:
        now = datetime.now()
        delta = now - heartbeat
        if delta < timedelta(minutes=30):
            status = "🟢 **Fresh**"
        elif delta < timedelta(minutes=300):
            status = "🟡 **Slightly Old**"
        else:
            status = "🔴 **Stale**"
        
        st.markdown(
            f"🧠 Background loader: {status}  \n"
            f"<small>Last heartbeat: {heartbeat.strftime('%Y-%m-%d %H:%M:%S')}</small>",
            unsafe_allow_html=True
        )
    else:
        st.markdown("⚠️ **Background loader not running**")


def main():
    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("Clearline HMO Analytics Dashboard")
        st.markdown("Welcome to the comprehensive healthcare analytics platform")
    with col2:
        st.image("Clearline.png", width=150)

    # Quick Stats
    if 'base_metrics' in st.session_state:
        metrics = st.session_state.base_metrics
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Active Enrollees", f"{metrics.get('TOTAL_ACTIVE_ENROLLEES', 0):,}", "Total Registered Members")
        with col2:
            st.metric("Active Providers", f"{metrics.get('TOTAL_PROVIDER_COUNT', 0):,}", "Healthcare Partners")
        with col3:
            st.metric("Active Contracts", f"{metrics.get('TOTAL_ACTIVE_CONTRACTS', 0):,}", "Client Agreements")
        with col4:
            st.metric("Denial Rate", f"{metrics.get('DENIAL_RATE', 0)}%", "Claims Processing")

    # Overview Section
    st.header("Dashboard Overview")
    st.markdown("""
    This dashboard provides comprehensive insights into CBA Healthcare operations:

    1. **Operations Dashboard**
        - Claims Analysis
        - MLR Tracking
        - Provider Distribution

    2. **Financial Analytics**
        - Revenue Tracking
        - Cost Analysis
        - MLR Trends

    3. **Provider Network**
        - Geographic Distribution
        - Specialty Analysis
        - Performance Metrics

    4. **Member Services**
        - Enrollment Trends
        - Plan Distribution
        - Service Utilization
    """)

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center'>
            <p>© 2024 CBA Healthcare. All rights reserved.</p>
            <p>For support, contact: support@cbahealthcare.com</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
