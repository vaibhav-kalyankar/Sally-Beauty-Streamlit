from utils import get_data_from_session
from config import constants as cs
import streamlit as st
import pandas as pd

def run_model():
    st.html('''
            <style>
            [data-testid="baseButton-primary"]{
                background-color: rgb(92,179,53);
                border: 1px solid rgb(92,179,53);
            }
            [data-testid="baseButton-primary"]:hover{
                background-color: rgb(92,179,53, 0.8);
            }
            </style>
    ''')

    df = get_data_from_session('training_data')
    if df is None:
        st.warning('No training data uploaded')
    else:

        st.write("Verify the inputs selected: ")
        selected_values = {
            "Date column" : get_data_from_session('selected_year'),
            "Date From" : get_data_from_session('lower_limit_date'),
            "Date To" : get_data_from_session('upper_limit_date'),
        }
        
        df = pd.DataFrame([{'Key': i, 'Value': selected_values[i]} for i in selected_values])

        st.data_editor(
            df,
            use_container_width=True,
            height=200,
        )
        with st.container():

            col1, col2, col3 = st.columns([9, 2, 1], vertical_alignment='bottom')
            response = None

            with col2:
                run_name = st.text_input('Run Name', help="This will help you identify your run")

            with col3:
                placeholder = st.empty()
                if not run_name:
                    button_click = placeholder.button("Run Model", type='primary', use_container_width=True, disabled=True)
                else:
                    button_click = placeholder.button("Run Model", type='primary', use_container_width=True)
                if button_click:
                    if not run_name:
                        st.toast('Please select run name')
                        st.stop()
                    with st.spinner('Sending inputs to Robyn..'):
                        response = {"run_id":"30901234567832802"}
                    
        with st.container():
            if response:
                st.success(f'''
                    Run ID: {response['run_id']}
                    Model running.. It normally take 30-40 mins depending on your data.
                    Please check your job status in "Jobs" tab
                '''
                )