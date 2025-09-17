import streamlit as st
import polars as pl
import pandas as pd
from datetime import datetime
import io

# Set page config
st.set_page_config(
    page_title="CDR Reconciliation Tool",
    page_icon="📞",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for enhanced UI
st.markdown("""
<style>
    /* Main styling */
    .main {
        background-color: #f8f9fa;
    }
    
    /* Headers */
    .main-header {
        font-size: 2.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, #0062cc 0%, #00a8ff 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        text-align: center;
        margin-bottom: 1.5rem;
        padding-bottom: 0.5rem;
        border-bottom: 2px solid #e9ecef;
    }
    
    .section-header {
        font-size: 1.6rem;
        font-weight: 600;
        color: #2c3e50;
        margin-top: 1.8rem;
        margin-bottom: 1rem;
        padding-left: 0.5rem;
        border-left: 4px solid #3498db;
    }
    
    /* Cards and containers */
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8f9fa 100%);
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.07);
        margin: 0.8rem;
        text-align: center;
        border: 1px solid #e9ecef;
        transition: transform 0.3s ease, box-shadow 0.3s ease;
    }
    
    .metric-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 15px rgba(0, 0, 0, 0.1);
    }
    
    .upload-container {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.07);
        margin-bottom: 1.5rem;
        border: 1px solid #e9ecef;
    }
    
    /* Buttons */
    .stButton button {
        background: linear-gradient(135deg, #3498db 0%, #2c73d2 100%);
        color: white;
        border: none;
        padding: 0.7rem 1.5rem;
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.3s ease;
    }
    
    .stButton button:hover {
        transform: translateY(-2px);
        box-shadow: 0 4px 8px rgba(52, 152, 219, 0.4);
        background: linear-gradient(135deg, #2c73d2 0%, #3498db 100%);
    }
    
    .download-btn {
        background: linear-gradient(135deg, #27ae60 0%, #2ecc71 100%) !important;
        width: 100%;
        margin-top: 0.5rem;
    }
    
    .download-btn:hover {
        background: linear-gradient(135deg, #2ecc71 0%, #27ae60 100%) !important;
        box-shadow: 0 4px 8px rgba(46, 204, 113, 0.4) !important;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background: linear-gradient(180deg, #2c3e50 0%, #3498db 100%);
    }
    
    .sidebar-header {
        color: white !important;
        font-weight: 700;
        font-size: 1.8rem;
        margin-bottom: 1.5rem;
    }
    
    .sidebar-section {
        background: rgba(255, 255, 255, 0.1);
        padding: 1rem;
        border-radius: 8px;
        margin-bottom: 1.5rem;
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #3498db 0%, #2ecc71 100%);
    }
    
    /* Success/error messages */
    .success-box {
        background: linear-gradient(135deg, #d4edda 0%, #c3e6cb 100%);
        color: #155724;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 1.2rem 0;
        border-left: 5px solid #28a745;
    }
    
    .error-box {
        background: linear-gradient(135deg, #f8d7da 0%, #f5c6cb 100%);
        color: #721c24;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 1.2rem 0;
        border-left: 5px solid #dc3545;
    }
    
    .warning-box {
        background: linear-gradient(135deg, #fff3cd 0%, #ffeaa7 100%);
        color: #856404;
        padding: 1.2rem;
        border-radius: 12px;
        margin: 1.2rem 0;
        border-left: 5px solid #ffc107;
    }
    
    /* Tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        background: #e9ecef;
        border-radius: 8px 8px 0 0;
        padding: 0.8rem 1.2rem;
        font-weight: 600;
    }
    
    .stTabs [aria-selected="true"] {
        background: #3498db;
        color: white;
    }
    
    /* File uploader */
    .stFileUploader {
        border: 2px dashed #3498db;
        border-radius: 8px;
        padding: 1.5rem;
    }
    
    /* Sliders */
    .stSlider {
        color: #3498db;
    }
    
    /* Metrics */
    [data-testid="stMetricValue"] {
        font-size: 1.8rem;
        font-weight: 700;
    }
    
    [data-testid="stMetricLabel"] {
        font-size: 1rem;
        font-weight: 600;
    }
    
    [data-testid="stMetricDelta"] {
        font-size: 0.9rem;
        font-weight: 500;
    }
</style>
""", unsafe_allow_html=True)

def process_cdr_data(airtel_file, mtn_file, duration_threshold, time_threshold):
    """Process CDR data with given thresholds"""
    try:
        # Load datasets
        df = pl.read_csv(airtel_file)
        trn = pl.read_csv(mtn_file)
        
        # Remove duplicates
        df = df.unique()
        trn = trn.unique()
        
        # Process Airtel data
        df = df.with_columns(pl.col("call_time").cast(pl.Utf8))
        df = df.with_columns([
            pl.col("a_number").str.slice(-10).alias("a_number"),
            pl.col("b_number").str.slice(-10).alias("b_number"),
        ])
        
        # Process time conversion for Airtel
        try:
            df = df.with_columns(
                (pl.col("call_time").str.split(":").list.get(0).cast(pl.Int64) * 3600 +
                 pl.col("call_time").str.split(":").list.get(1).cast(pl.Int64) * 60 +
                 pl.col("call_time").str.split(":").list.get(2).cast(pl.Int64))
                .alias("call_time_secs")
            )
        except:
            df = df.with_columns(pl.lit(0).alias("call_time_secs"))
        
        df = df.with_columns([
            (pl.col("a_number") + pl.col("b_number")).alias("look_col"),
            pl.col("duration").alias("event_duration"),
        ])
        
        # Process MTN data
        trn = trn.with_columns(pl.col("time_field").cast(pl.Utf8))
        trn = trn.with_columns([
            pl.col("originating_number").str.slice(-10).alias("a_number"),
            pl.col("terminating_number").str.slice(-10).alias("b_number"),
        ])
        
        # Process time conversion for MTN
        try:
            trn = trn.with_columns(
                (pl.col("time_field").str.split(":").list.get(0).cast(pl.Int64) * 3600 +
                 pl.col("time_field").str.split(":").list.get(1).cast(pl.Int64) * 60 +
                 pl.col("time_field").str.split(":").list.get(2).cast(pl.Int64))
                .alias("time_field_secs")
            )
        except:
            trn = trn.with_columns(pl.lit(0).alias("time_field_secs"))
        
        trn = trn.with_columns([
            (pl.col("a_number") + pl.col("b_number")).alias("look_col"),
            pl.col("duration").alias("event_duration"),
        ])
        
        # Join datasets
        df_for_join = df.select(["look_col", "event_duration", "call_time_secs"])
        trn_for_join = trn.select(["look_col", "event_duration", "time_field_secs"])
        
        merged = df_for_join.join(trn_for_join, on="look_col", how="full", suffix="_mtn")
        
        # Filter for common matches using user-defined thresholds
        common = merged.filter(
            (pl.col("event_duration").is_not_null()) &
            (pl.col("event_duration_mtn").is_not_null()) &
            ((pl.col("event_duration") - pl.col("event_duration_mtn")).abs() <= duration_threshold) &
            (pl.col("call_time_secs").is_not_null()) &
            (pl.col("time_field_secs").is_not_null()) &
            ((pl.col("call_time_secs") - pl.col("time_field_secs")).abs() <= time_threshold)
        )
        
        # Exclusive records
        common_look_cols = common["look_col"].unique().implode()
        df_only = df.filter(~pl.col("look_col").is_in(common_look_cols))
        trn_only = trn.filter(~pl.col("look_col").is_in(common_look_cols))
        
        # Convert to pandas for display
        common_pd = common.to_pandas()
        df_only_pd = df_only.to_pandas()
        trn_only_pd = trn_only.to_pandas()
        
        return {
            'df': df,
            'trn': trn,
            'common': common,
            'df_only': df_only,
            'trn_only': trn_only,
            'common_pd': common_pd,
            'df_only_pd': df_only_pd,
            'trn_only_pd': trn_only_pd,
            'success': True
        }
        
    except Exception as e:
        return {'success': False, 'error': str(e)}

def main():
    # Header with logo and title
    col1, col2, col3 = st.columns([1, 3, 1])
    with col2:
        st.markdown('<h1 class="main-header">📞 CDR Reconciliation Dashboard</h1>', unsafe_allow_html=True)
        st.markdown("### Efficiently reconcile Airtel and MTN CDR data with precision controls")
    
    # Sidebar for user inputs
    with st.sidebar:
        st.markdown('<h2 class="sidebar-header">⚙️ Control Panel</h2>', unsafe_allow_html=True)
        
        # Date selection
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.subheader("📅 Date Selection")
        reconciliation_date = st.date_input(
            "Select Reconciliation Date",
            datetime.now(),
            help="Select the date for which you want to reconcile records"
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Threshold settings
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.subheader("🎚️ Threshold Settings")
        duration_threshold = st.slider(
            "Duration Tolerance (seconds)",
            min_value=1,
            max_value=10,
            value=5,
            help="Maximum allowed difference in call duration between matched records"
        )
        
        time_threshold = st.slider(
            "Time Tolerance (seconds)", 
            min_value=1,
            max_value=10,
            value=5,
            help="Maximum allowed difference in call time between matched records"
        )
        
        st.info(f"🔧 Current settings: Duration ±{duration_threshold}s, Time ±{time_threshold}s")
        st.markdown('</div>', unsafe_allow_html=True)
        
        # File uploads
        st.markdown('<div class="sidebar-section">', unsafe_allow_html=True)
        st.subheader("📁 File Upload")
        
        st.markdown("#### Airtel CDR File")
        airtel_file = st.file_uploader(
            "Upload Airtel CSV",
            type=["csv"],
            key="airtel_upload",
            help="Upload Airtel CDR file with columns: a_number, b_number, call_time, duration"
        )
        
        st.markdown("#### MTN CDR File")
        mtn_file = st.file_uploader(
            "Upload MTN CSV", 
            type=["csv"],
            key="mtn_upload",
            help="Upload MTN CDR file with columns: originating_number, terminating_number, time_field, duration"
        )
        st.markdown('</div>', unsafe_allow_html=True)
        
        # Process button
        process_btn = st.button(
            "🚀 Start Reconciliation", 
            type="primary", 
            use_container_width=True,
            disabled=not (airtel_file and mtn_file)
        )
    
    # Main content
    if airtel_file and mtn_file and process_btn:
        st.success(f"📅 Reconciling for date: {reconciliation_date.strftime('%Y-%m-%d')}")
        
        # Display current settings
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Duration Threshold", f"±{duration_threshold} seconds")
            st.markdown('</div>', unsafe_allow_html=True)
        with col2:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Time Threshold", f"±{time_threshold} seconds")
            st.markdown('</div>', unsafe_allow_html=True)
        with col3:
            st.markdown('<div class="metric-card">', unsafe_allow_html=True)
            st.metric("Reconciliation Date", reconciliation_date.strftime("%Y-%m-%d"))
            st.markdown('</div>', unsafe_allow_html=True)
        
        # Process data
        with st.spinner("Reconciling CDR data..."):
            progress_bar = st.progress(0)
            
            result = process_cdr_data(airtel_file, mtn_file, duration_threshold, time_threshold)
            progress_bar.progress(100)
            
        if result['success']:
            st.markdown('<div class="success-box">✅ Reconciliation completed successfully!</div>', unsafe_allow_html=True)
            
            # Display summary statistics
            st.markdown("---")
            st.markdown('<h2 class="section-header">📊 Reconciliation Results</h2>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Airtel Records", f"{result['df'].height:,}", 
                         f"{- (result['df'].height - result['df_only'].height):,} matched")
                st.metric("Airtel Duration", 
                         f"{result['df']['event_duration'].sum() / 60:,.0f} min", 
                         f"{result['df']['event_duration'].sum():,.0f} sec")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col2:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("MTN Records", f"{result['trn'].height:,}", 
                         f"{- (result['trn'].height - result['trn_only'].height):,} matched")
                st.metric("MTN Duration", 
                         f"{result['trn']['event_duration'].sum() / 60:,.0f} min", 
                         f"{result['trn']['event_duration'].sum():,.0f} sec")
                st.markdown('</div>', unsafe_allow_html=True)
            
            with col3:
                st.markdown('<div class="metric-card">', unsafe_allow_html=True)
                st.metric("Matched Records", f"{result['common'].height:,}")
                if result['common'].height > 0:
                    st.metric("Match Rate", 
                             f"{(result['common'].height / min(result['df'].height, result['trn'].height)) * 100:.1f}%")
                st.markdown('</div>', unsafe_allow_html=True)
            
            # Download section
            st.markdown("---")
            st.markdown('<h2 class="section-header">📥 Download Results</h2>', unsafe_allow_html=True)
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv_common = result['common_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download Matched Records",
                    data=csv_common,
                    file_name=f"matched_cdr_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records that matched between Airtel and MTN",
                    use_container_width=True
                )
            
            with col2:
                csv_airtel_only = result['df_only_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download Airtel Only",
                    data=csv_airtel_only,
                    file_name=f"airtel_only_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records found only in Airtel",
                    use_container_width=True
                )
            
            with col3:
                csv_mtn_only = result['trn_only_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download MTN Only",
                    data=csv_mtn_only,
                    file_name=f"mtn_only_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records found only in MTN",
                    use_container_width=True
                )
            
            # Data preview
            st.markdown("---")
            st.markdown('<h2 class="section-header">🔍 Data Preview</h2>', unsafe_allow_html=True)
            
            tab1, tab2, tab3 = st.tabs(["📋 Matched Records", "📱 Airtel Only", "📶 MTN Only"])
            
            with tab1:
                st.dataframe(result['common_pd'].head(100), use_container_width=True)
            
            with tab2:
                st.dataframe(result['df_only_pd'].head(100), use_container_width=True)
            
            with tab3:
                st.dataframe(result['trn_only_pd'].head(100), use_container_width=True)
                
        else:
            st.markdown(f'<div class="error-box">❌ Error during reconciliation: {result["error"]}</div>', unsafe_allow_html=True)
    
    elif process_btn and not (airtel_file and mtn_file):
        st.markdown('<div class="warning-box">⚠️ Please upload both Airtel and MTN files to start reconciliation.</div>', unsafe_allow_html=True)
    
    else:
        # Welcome and instructions
        st.markdown("---")
        st.markdown("""
        <div style="background: linear-gradient(135deg, #e3f2fd 0%, #bbdefb 100%); padding: 2rem; border-radius: 12px; border-left: 5px solid #2196f3;">
            <h2 style="color: #1565c0; margin-top: 0;">Welcome to the CDR Reconciliation Tool!</h2>
            <h4 style="color: #1976d2;">How to use:</h4>
            <ol style="color: #424242;">
                <li>📅 Select the reconciliation date in the sidebar</li>
                <li>⚙️ Set your preferred duration and time thresholds (1-10 seconds)</li>
                <li>📁 Upload Airtel and MTN CDR files</li>
                <li>🚀 Click 'Start Reconciliation'</li>
                <li>📊 View results and download reconciled files</li>
            </ol>
            
            <h4 style="color: #1976d2;">Expected file formats:</h4>
            <ul style="color: #424242;">
                <li><strong>Airtel CSV</strong>: <code>a_number</code>, <code>b_number</code>, <code>call_time</code>, <code>duration</code></li>
                <li><strong>MTN CSV</strong>: <code>originating_number</code>, <code>terminating_number</code>, <code>time_field</code>, <code>duration</code></li>
            </ul>
            
            <h4 style="color: #1976d2;">Threshold explanation:</h4>
            <ul style="color: #424242;">
                <li><strong>Duration Tolerance</strong>: Max allowed difference in call duration (seconds)</li>
                <li><strong>Time Tolerance</strong>: Max allowed difference in call time (seconds)</li>
            </ul>
        </div>
        """, unsafe_allow_html=True)
        
        # Example preview
        with st.expander("📋 Example Data Format", expanded=True):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Airtel Format:**")
                example_airtel = pd.DataFrame({
                    'a_number': ['2347070350149', '2347063496606'],
                    'b_number': ['2348120811866', '2347010761254'], 
                    'call_time': ['12:30:45', '08:15:22'],
                    'duration': [120.5, 45.2]
                })
                st.dataframe(example_airtel, use_container_width=True)
            with col2:
                st.write("**MTN Format:**")
                example_mtn = pd.DataFrame({
                    'originating_number': ['2348066748079', '2349136342550'],
                    'terminating_number': ['2348122200960', '2349076670283'],
                    'time_field': ['04:42:45', '00:24:01'],
                    'duration': [50.0, 11.0]
                })
                st.dataframe(example_mtn, use_container_width=True)

if __name__ == "__main__":
    main()

