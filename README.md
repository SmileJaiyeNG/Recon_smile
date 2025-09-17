# CDR Reconciliation Tool

A Streamlit web application for reconciling Airtel and MTN CDR (Call Detail Record) data.

## Features

- 📊 Compare Airtel and MTN CDR files
- ⚙️ Adjustable duration and time thresholds
- 📥 Download matched and unmatched records
- 📱 Responsive web interface

## Installation

1. Clone this repository
2. Install dependencies: `pip install -r requirements.txt`
3. Run the app: `streamlit run CDR_Recon.py`

## Usage

1. Upload Airtel and MTN CSV files
2. Set your preferred thresholds
3. Click "Start Reconciliation"
4. View results and download files

## File Formats

### Airtel CSV Format:
- `a_number`, `b_number`, `call_time`, `duration`

### MTN CSV Format:
- `originating_number`, `terminating_number`, `time_field`, `duration`
