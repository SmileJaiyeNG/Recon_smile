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

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .section-header {
        font-size: 1.5rem;
        color: #2ca02c;
        margin-top: 1.5rem;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem;
        text-align: center;
    }
    .success-box {
        background-color: #d4edda;
        color: #155724;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 1rem 0;
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
    st.title("📞 CDR Reconciliation Dashboard")
    st.markdown("Reconcile Airtel and MTN CDR data with flexible thresholds")
    
    # Sidebar for user inputs
    with st.sidebar:
        st.header("⚙️ Reconciliation Settings")
        
        # Date selection
        reconciliation_date = st.date_input(
            "Select Reconciliation Date",
            datetime.now(),
            help="Select the date for which you want to reconcile records"
        )
        
        # Threshold settings
        st.subheader("Threshold Settings")
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
        
        # File uploads
        st.header("📁 Upload CDR Files")
        
        st.subheader("Airtel CDR File")
        airtel_file = st.file_uploader(
            "Upload Airtel CSV",
            type=["csv"],
            key="airtel_upload",
            help="Upload Airtel CDR file with columns: a_number, b_number, call_time, duration"
        )
        
        st.subheader("MTN CDR File")
        mtn_file = st.file_uploader(
            "Upload MTN CSV", 
            type=["csv"],
            key="mtn_upload",
            help="Upload MTN CDR file with columns: originating_number, terminating_number, time_field, duration"
        )
        
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
            st.metric("Duration Threshold", f"±{duration_threshold} seconds")
        with col2:
            st.metric("Time Threshold", f"±{time_threshold} seconds")
        with col3:
            st.metric("Reconciliation Date", reconciliation_date.strftime("%Y-%m-%d"))
        
        # Process data
        with st.spinner("Reconciling CDR data..."):
            progress_bar = st.progress(0)
            
            result = process_cdr_data(airtel_file, mtn_file, duration_threshold, time_threshold)
            progress_bar.progress(100)
            
        if result['success']:
            st.success("✅ Reconciliation completed successfully!")
            
            # Display summary statistics
            st.markdown("---")
            st.header("📊 Reconciliation Results")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Airtel Records", f"{result['df'].height:,}", 
                         f"{- (result['df'].height - result['df_only'].height):,} matched")
                st.metric("Airtel Duration", 
                         f"{result['df']['event_duration'].sum() / 60:,.0f} min", 
                         f"{result['df']['event_duration'].sum():,.0f} sec")
            
            with col2:
                st.metric("MTN Records", f"{result['trn'].height:,}", 
                         f"{- (result['trn'].height - result['trn_only'].height):,} matched")
                st.metric("MTN Duration", 
                         f"{result['trn']['event_duration'].sum() / 60:,.0f} min", 
                         f"{result['trn']['event_duration'].sum():,.0f} sec")
            
            with col3:
                st.metric("Matched Records", f"{result['common'].height:,}")
                if result['common'].height > 0:
                    st.metric("Match Rate", 
                             f"{(result['common'].height / min(result['df'].height, result['trn'].height)) * 100:.1f}%")
            
            # Download section
            st.markdown("---")
            st.header("📥 Download Results")
            
            col1, col2, col3 = st.columns(3)
            
            with col1:
                csv_common = result['common_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download Matched Records",
                    data=csv_common,
                    file_name=f"matched_cdr_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records that matched between Airtel and MTN"
                )
            
            with col2:
                csv_airtel_only = result['df_only_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download Airtel Only",
                    data=csv_airtel_only,
                    file_name=f"airtel_only_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records found only in Airtel"
                )
            
            with col3:
                csv_mtn_only = result['trn_only_pd'].to_csv(index=False)
                st.download_button(
                    label="📄 Download MTN Only",
                    data=csv_mtn_only,
                    file_name=f"mtn_only_{reconciliation_date.strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                    help="Download records found only in MTN"
                )
            
            # Data preview
            st.markdown("---")
            st.header("🔍 Data Preview")
            
            tab1, tab2, tab3 = st.tabs(["Matched Records", "Airtel Only", "MTN Only"])
            
            with tab1:
                st.dataframe(result['common_pd'].head(100), use_container_width=True)
            
            with tab2:
                st.dataframe(result['df_only_pd'].head(100), use_container_width=True)
            
            with tab3:
                st.dataframe(result['trn_only_pd'].head(100), use_container_width=True)
                
        else:
            st.error(f"❌ Error during reconciliation: {result['error']}")
    
    elif process_btn and not (airtel_file and mtn_file):
        st.warning("⚠️ Please upload both Airtel and MTN files to start reconciliation.")
    
    else:
        # Welcome and instructions
        st.markdown("""
        ### Welcome to the CDR Reconciliation Tool!
        
        **How to use:**
        1. 📅 Select the reconciliation date
        2. ⚙️ Set your preferred duration and time thresholds (1-10 seconds)
        3. 📁 Upload Airtel and MTN CDR files
        4. 🚀 Click 'Start Reconciliation'
        5. 📊 View results and download reconciled files
        
        **Expected file formats:**
        - **Airtel CSV**: `a_number`, `b_number`, `call_time`, `duration`
        - **MTN CSV**: `originating_number`, `terminating_number`, `time_field`, `duration`
        
        **Threshold explanation:**
        - **Duration Tolerance**: Max allowed difference in call duration (seconds)
        - **Time Tolerance**: Max allowed difference in call time (seconds)
        """)
        
        # Example preview
        with st.expander("📋 Example Data Format"):
            col1, col2 = st.columns(2)
            with col1:
                st.write("**Airtel Format:**")
                st.table(pd.DataFrame({
                    'a_number': ['7070350149', '7063496606'],
                    'b_number': ['8120811866', '7010761254'], 
                    'call_time': ['12:30:45', '08:15:22'],
                    'duration': [120.5, 45.2]
                }))
            with col2:
                st.write("**MTN Format:**")
                st.table(pd.DataFrame({
                    'originating_number': ['2348066748079', '2349136342550'],
                    'terminating_number': ['2348122200960', '2349076670283'],
                    'time_field': ['04:42:45', '00:24:01'],
                    'duration': [50.0, 11.0]
                }))

if __name__ == "__main__":
    main()