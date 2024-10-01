from config import constants as cs
from datetime import datetime, timedelta, timezone
from utils import get_job_runs, store_data_in_session, get_data_from_session, get_active_job_runs, cancel_run
import streamlit as st
import pandas as pd
import humanize
import ast


@st.cache_data(show_spinner=False)
def split_frame(input_df, rows):
    df = [input_df.loc[i : i + rows - 1, :] for i in range(0, len(input_df), rows)]
    return df


def on_button_click(page_token):
    store_data_in_session('page_token', page_token)


def cancel_run_callback(run_id):
    response = cancel_run(run_id)
    if response.status_code == 200:
        st.toast('Run will be cancelled in sometime. Please refresh to see latest updates')
    return


def list_jobs():
    # creating a single-element container
    hide_table_row_index = """
                <style>
                    table {
                        width: 100%;
                    }
                    th, td {
                        text-align: center !important;
                        padding: 8px !important;
                    }
                    th {
                        background-color: #5CB335 !important;
                        color: white !important;
                    }
                    [role="alert"]{
                        border-radius: 0;
                    }
                    [class*="st-"]{
                        border-radius: 0px;
                    }
                    [class*="st-"] i{
                        border-radius: 50%;
                    }
                    [class*="st-emotion-cache-"]:disabled{
                        border-radius: 0px;
                    }
                    [data-testid="stNumberInput-StepUp"]{
                        border-radius: 0;
                    }
                    [data-testid="baseButton-secondary"]{
                        border: 1px solid rgb(92,179,53);
                    }
                    [data-testid="baseButton-secondary"]:hover{
                        background-color: rgb(92,179,53, 0.8);
                        border: 1px solid rgb(92,179,53);
                        color: white;
                    }
                </style>
                """

    # Inject CSS with Markdown
    st.html(hide_table_row_index)

    refresh = st.button('↻ Refresh')
    if refresh:
        store_data_in_session('page_token', '')

    # with st.spinner('Fetching job runs..'):
    #     response = get_job_runs(page_token=get_data_from_session('page_token'))
    #     active_job_runs = get_active_job_runs()
        
    #     if response.status_code != 200:
    #         st.error('Could not connect to Databricks')
    #         st.stop()

    #     # display progress bar for active runs
    #     if 'runs' in  active_job_runs.json().keys():
    #         for run in active_job_runs.json().get('runs'):
    #             if 'life_cycle_state' in run['state'].keys():
    #                 if run['state']['life_cycle_state'] == 'RUNNING':
    #                     if 'overriding_parameters' in run.keys():
    #                         date = pd.to_datetime(run['start_time'], unit="ms")
    #                         current_date = datetime.utcnow()
    #                         # total_iterations = run['overriding_parameters']['notebook_params']['no_of_iterations']
    #                         # total_trials = run['overriding_parameters']['notebook_params']['no_of_trials']

    #                         if 'run_name' in run['overriding_parameters']['notebook_params'].keys():
    #                             run_id = run['overriding_parameters']['notebook_params'].get('run_name')
    #                         else:
    #                             run_id = run['run_id']
                            
    #                         try:
    #                             total_locations = len(ast.literal_eval(run['overriding_parameters']['notebook_params'].get('geographies')))
    #                         except:
    #                             total_locations = len(ast.literal_eval(run['overriding_parameters']['notebook_params'].get('opt_geographies')))
    #                         eta_locations_in_min = 20

    #                         actual_time = date + timedelta(minutes=eta_locations_in_min * total_locations)
    #                         diff = actual_time - current_date
    #                         actual_diff = actual_time - date
    #                         actual_mins = actual_diff.total_seconds() // 60
    #                         remaining_mins = diff.total_seconds() // 60
    #                         remaining_mins = 100 - (remaining_mins/actual_mins * 100)
                            
    #                         with st.container(border=True):
    #                             col1, col2, col3 = st.columns([8, 3, 1], vertical_alignment='bottom')

    #                             with col1:
    #                                 my_bar = st.progress(0, text=f'Run: {run_id}')
    #                                 actual_time = humanize.precisedelta(actual_time, minimum_unit='minutes', format="%2d")
    #                                 my_bar.progress(remaining_mins /100 , text=f'Run: {run_id} (Estimated Run Time: {actual_time})')
                                
    #                             with col2:
    #                                 st.text(f"{round(remaining_mins, 2)} % complete")

    #                             with col3:
    #                                 st.button('Cancel run', on_click=cancel_run_callback, args=(run["run_id"], ))
            
    #     _list = []
        
    #     if 'runs' in  response.json().keys():
    #         for run in response.json().get('runs'):
    #             _dict = {}
    #             # _dict['Job ID'] = run['job_id']

    #             if 'overriding_parameters' in run.keys():
    #                 # run name
    #                 if 'run_name' in run['overriding_parameters']['notebook_params'].keys():
    #                     _dict['Run Name'] = run['overriding_parameters']['notebook_params'].get('run_name')
    #                 else:
    #                     _dict['Run Name'] = run['job_id']

    #                 # run type
    #                 if run['overriding_parameters']['notebook_params'].get('robyn_run_id', '') == '':
    #                         _dict['Run Purpose'] = "Robyn"
    #                 else:
    #                     _dict['Run Purpose'] = "Optimization"
    #             else:
    #                 _dict['Run Purpose'] = 'Robyn'

    #             _dict['Run ID'] = run['run_id']

    #             # get current state of run
    #             if 'result_state' in run['state'].keys():
    #                 _dict['Status'] = run['state']['result_state']
    #             else:
    #                 _dict['Status'] = run['state']['life_cycle_state']

    #             _dict['databricks_url'] = run['run_page_url']
    #             _dict['Start Time'] = pd.to_datetime(run['start_time'], unit="ms")

    #             seconds, milliseconds = divmod(run['execution_duration'], 1000)
    #             minutes, seconds = divmod(seconds, 60)
    #             _dict['Execution Duration'] = f'{int(minutes):02d} mins {int(seconds):02d} secs'
    #             _list.append(_dict)

    #             df = pd.DataFrame(_list)
    #             df['Run ID'] = df.apply(lambda x: f'<a target="_blank" href="{x["databricks_url"]}">{x["Run ID"]}</a>', axis=1)
    #             df = df.drop('databricks_url', axis=1)
    #     else:
    #         st.info('End of runs.. Please click refresh to go over all runs again')
    #         st.stop()

        pagination = st.container()

    #     bottom_menu = st.columns((4, 1, 1), vertical_alignment='bottom')
    #     with bottom_menu[2]:
    #         if 'next_page_token' in response.json().keys():
    #             token = response.json().get('next_page_token')
    #         else:
    #             token = ""
    #         st.button(
    #             'Next Page',
    #             use_container_width=True,
    #             on_click=on_button_click,
    #             args=[token]
    #         )

    #     with bottom_menu[1]:
    #         if 'prev_page_token' in response.json().keys():
    #             token = response.json().get('prev_page_token')
    #         else:
    #             token = ""
    #         st.button(
    #             'Previous Page',
    #             use_container_width=True,
    #             on_click=on_button_click,
    #             args=[token]
    #         )

        
        df= pd.read_csv(r"C:\Users\Krushna_Kadam\Downloads\databricks_job_202410011321.csv")

        # pagination.table(data=pages[current_page - 1])
        html = df.to_html(escape=False, index=False)
        pagination.markdown(html, unsafe_allow_html=True)