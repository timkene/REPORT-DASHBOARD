
import streamlit as st

# Configure page settings - must be the first Streamlit command
st.set_page_config(
    page_title="Clearline HMO Dashboard",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded"
)

from utils.background_loader import DataLoader
from datetime import datetime
import plotly.graph_objects as go
import time 
from datetime import datetime, timedelta
from utils.data_loader import initialize_data

# Initialize background loader if not already initialized
if 'data_loader' not in st.session_state:
    st.session_state.data_loader = DataLoader()
    st.session_state.data_loader.start()

# Initialize data if needed
if 'data' not in st.session_state or 'base_metrics' not in st.session_state:
    with st.spinner("Initializing application data..."):
        data = initialize_data()
        if data is None:
            st.error("Failed to initialize data. Please refresh the page.")
            st.stop()
        
        # Wait for data loading to complete
        while 'data_loading_progress' not in st.session_state or st.session_state.data_loading_progress < 100:
            if st.session_state.data_loading_progress == -1:
                st.error("Error loading data. Please refresh the page.")
                st.stop()
            time.sleep(0.1)

# Show loading progress
if 'data_loading_progress' in st.session_state:
    progress = st.session_state.data_loading_progress
    if progress > 0 and progress < 100:
        st.progress(progress / 100, "Loading data...")
    elif progress == -1:
        st.error("Error loading data. Please refresh the page.")
        st.stop()

# Add refresh button in sidebar
with st.sidebar:
    # Manual Refresh Button
    if st.button('🔄 Refresh Data'):
        # Clear only data-related state
        data_keys = ['data', 'data_loading_progress', 'base_metrics']
        for key in data_keys:
            if key in st.session_state:
                del st.session_state[key]
        # Clear cache
        st.cache_data.clear()
        with st.spinner('Refreshing data...'):
            st.session_state.data_loader.force_refresh()
            st.rerun()

    # Last successful full update
    if 'last_update_time' in st.session_state:
        st.caption(f"Last updated: {st.session_state.last_update_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Background loader heartbeat check
    heartbeat = st.session_state.get('last_heartbeat')
    if heartbeat:
        now = datetime.now()
        delta = now - heartbeat
        
        # Set freshness status
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
    # Dashboard Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.title("Clearline HMO Analytics Dashboard")
        st.markdown("Welcome to the comprehensive healthcare analytics platform")
    with col2:
        st.image("Clearline.png", width=150)  # Add your company logo
    
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
    
    # Dashboard Overview
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
