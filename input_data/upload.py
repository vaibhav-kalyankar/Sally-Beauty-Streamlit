import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import streamlit as st
import pandas as pd
from datetime import datetime
from utils import store_data_in_session, column_info, get_data_from_session
from validation import validate

def input_upload():
    hide_table_row_index = """
                <style>
                    table {
                        width: 100%;
                    }
                    th, td {
                        text-align: center !important;
                        padding: 8px !important;
                    }
                    th:first-child, td:first-child {
                        display: none;
                    }
                    th {
                        background-color: #5CB335 !important;
                        color: white !important;
                    }
                    [role="alert"]{
                        border-radius: 0;
                    }
                </style>
                """

    # Inject CSS with Markdown
    st.html(hide_table_row_index)
    
    data_file = st.file_uploader("Upload Training Dataset", type=["csv", "xlsx"]) 
    # st.write("Please check column naming conventions [here]()")         

    if data_file is None:
        st.html('<span id="button-after"></span>')
        if st.button('Column Name Rules', type='secondary'):
            column_info()

    if data_file is not None:
        store_data_in_session('file_error', False)
    
        try:
            data_file.seek(0)
            if data_file.name.endswith('.csv'):
                df = pd.read_csv(data_file, index_col=False)
            else:
                df = pd.read_excel(data_file)
            
            if validate(df):
                # explicitly convert dates to YYYY-MM-DD format
                df['WeekStart'] = pd.to_datetime(df['WeekStart'])
                
                # store data in session so that it can be referred in other tabs
                store_data_in_session('training_data', df)
                error = get_data_from_session('file_error')
                if not error:
                    if st.button("Process Data"):
                        st.write("Uploaded file Summary:")
                        summary_data = {
                            "Rows": [df.shape[0]],
                            "Columns": [df.shape[1]],
                            "Date Uploaded": str(datetime.now().date()),
                            "Time Uploaded": datetime.now().strftime("%I:%M %p")
                        }
                        st.table(pd.DataFrame(summary_data))

                        st.write("Data Preview:")
                        st.table(df.head())
        except Exception as e:
                    st.error(f"Error while processing file")
                    print(f"Error while processing file: {e}")
