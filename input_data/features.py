from utils import get_data_from_session, store_data_in_session
import streamlit as st

def input_features():
    st.html(
        """
        <style display='hidden'>
        [class*="st-emotion-"] {
            border-radius: 0px;
        }
        [class*="st-"]{
            border-radius: 0px;
        }
        [class*="st-emotion-cache-"] span div p{
            font-size: 18px;
            font-weight: bold;
        }
        [data-testid="baseButton-primary"]{
            background-color: rgb(92,179,53);
            border: 1px solid rgb(92,179,53);
        }
        [data-testid="baseButton-primary"]:hover{
            background-color: rgb(92,179,53, 0.8);
        }
        [data-testid="baseButton-secondaryFormSubmit"]{
            background-color: rgba(28, 131, 225, 0.1);
        }
        </style>
        """
    )

    df = get_data_from_session('training_data')
    if df is not None and not df.empty:
        with st.expander("Time Variables", expanded=True):
            col1, col2 = st.columns(2)

            with col1:
                year_selected = [*df]
                year_selected.insert(0, 'None')
                year_selected = st.selectbox(
                    'Select Date Column',
                    year_selected,
                    index=df.columns.get_loc("WeekStart") + 1,
                    disabled=True
                )

            with col2:
                sub_col1, sub_col2 = st.columns(2)

                with sub_col1:
                    lower_limit_date = st.date_input('Date From', value=min(df['WeekStart']),
                                                     min_value=min(df['WeekStart']),max_value=max(df['WeekStart']))

                with sub_col2:
                    upper_limit_date = st.date_input('Date To', value=max(df['WeekStart']),
                                                     min_value=min(df['WeekStart']),max_value=max(df['WeekStart']))

        save = st.button('Save Feature Definitions', type='primary')
        if save:
    
            
            features = {
                'selected_year': year_selected,
                'lower_limit_date': lower_limit_date,
                'upper_limit_date': upper_limit_date,
            }

            # store all values in session for accesssing them in other tabs
            for key, value in features.items():
                store_data_in_session(key, value)
            
            st.toast('Features have been saved successfully')
    else:
        st.warning('No training data uploaded')