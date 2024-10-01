from utils import store_data_in_session
import streamlit as st
import pandas as pd

def validate(df):
    '''function to add file validation while uploading training data files.'''

    # replace / with hyphen to avoid problems while creating folders from databricks
    df.replace({r'/': '-'}, regex=True, inplace=True)

    if 'WeekStart' in df.columns:
        if df['WeekStart'].dtype == 'object':
            try:
                df['WeekStart'] = pd.to_datetime(df['WeekStart'])
            except ValueError:
                st.error("Format of column: WeekStart is not correct. Dates should be in YYYY/MM/DD format")
        else:
            if df['WeekStart'].isnull().any():
                st.error("Column 'WeekStart' has missing/NULL values. Please reupload file with correct values.")    
                return None
    else:
        store_data_in_session('file_error', True)
        st.error("⚠️ Column 'WeekStart' not found")
    return True