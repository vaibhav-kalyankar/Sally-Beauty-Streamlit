from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score, mean_absolute_percentage_error
from azure.storage.blob import BlobServiceClient
from config import constants
import streamlit as st
import numpy as np
import pandas as pd
import os
import io
import requests
import re


def clean_column_name(column_name):
    # replace hyphen (-), whitespaces, dots (.) and hashtags (#) with underscores (_)
    # https://github.com/facebookexperimental/Robyn/issues/641
    column_name = column_name.replace('-', '').replace(' ', '_').replace('.', '_').replace('#', '')
    return column_name


def store_data_in_session(key, value):
    # if key not in st.session_state:
    st.session_state[key] = value
    return


def get_data_from_session(key):
    if key not in st.session_state:
        return None
    return st.session_state[key]


def get_session_data_with_df_index(df, column_name):
    try:
        index=df.columns.get_loc(column_name)
        return index
    except:
        return 0


def calculate_metrics(df):
    y_true = df['dep_var']
    y_pred = df['depVarHat']
    r_squared = r2_score(y_true, y_pred)

    try:
        adjusted_r_squared = 1 - (1 - r_squared) * (len(y_true) - 1) / (len(y_true) - len(df.columns) - 1)
    except ZeroDivisionError:
        adjusted_r_squared = float('nan')

    mape = (mean_absolute_percentage_error(y_true, y_pred))*100
    rmse = np.sqrt(mean_squared_error(y_true, y_pred))
    return r_squared, adjusted_r_squared, mape, rmse


def display_large_metric(label, value):
    st.markdown(
        f'''
        <style>
            .kpi-card{{
                overflow: hidden;
                position: relative;
                box-shadow: 1px 1px 3px rgba(0,0,0,0.25);;
                display: inline-block;
                float: left;
                padding: 1em;
                border-radius: 0;
                font-family: sans-serif;  
                width: 100%;
                min-width: 180px;
                margin-top: 0.5em;
                background-color: rgb(92,179,53, 0.7);
                color: white;
                justify-content: center;
            }}

            .card-value {{
                display: block;
                font-size: 200%;  
                font-weight: bolder;
            }}

            .card-text {{
                display:block;
                font-size: 90%;
                # font-weight: bold;
                padding-left: 0.2em;
            }}
        </style>
        <div class="kpi-card">
            <span class="card-value">{value}</span>
            <span class="card-text">{label}</span>
        </div>
        ''',
        unsafe_allow_html=True
    )


def reject_outliers(df, column):
    # Calculate Q1 (25th percentile) and Q3 (75th percentile)
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    
    # Calculate IQR
    IQR = Q3 - Q1
    
    # Define lower and upper bounds for outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    
    # Filter the DataFrame to remove outliers
    df_no_outliers = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    
    return df_no_outliers


# streamlit runs entire code, st.cache_data avoids that
@st.cache_resource(show_spinner=False)
def upload_to_azure_storage(file, validate_file=False):
    '''Function to upload file to Azure Storage.'''

    file_name = file.name

    # validate file before uploading to azure
    if validate_file:
        if file.name.endswith('.csv'):
            df = pd.read_csv(file, index_col=False)
        else:
            df = pd.read_excel(file)

        # remove $ and , from all columns
        for cols in df.columns:
            if df[cols].dtype == 'object':
                df[cols] = df[cols].str.replace('$', '')
                df[cols] = df[cols].str.replace(',', '')
                df[cols] = df[cols].str.replace('#', '')

        # explicitly convert WeekStart column to a date
        df['WeekStart'] = pd.to_datetime(df['WeekStart'])

    # to do: make this dynamic
    # df = reject_outliers(df, 'FuelMargin')


    with st.spinner('Uploading file to Azure storage..'):
        blob_service_client = BlobServiceClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
        )
        filename = 'user_data/' + file_name
        blob_client = blob_service_client.get_blob_client(container=constants.container_name, blob=filename)
        blob_client.upload_blob(df.to_csv(index=False), overwrite=True)
        return file, filename
    

@st.cache_data(show_spinner=False)
def download_blob_to_file(container_name, filename, dest_filepath):
    '''function to download a file from azure cloud storage in specified filepath.'''
    
    blob_service_client = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)
    with open(file=os.path.join(dest_filepath), mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())


@st.cache_data(show_spinner=False)
def get_geos_from_blob(container_name, run_id):
    '''function to download a file from azure cloud storage in specified filepath.'''
    
    blob_service_client = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    container_client = blob_service_client.get_container_client(container=container_name)
    files = container_client.list_blobs(name_starts_with=run_id)
    geos = []
    for file in files:
        if str(run_id) in file['name']:
            try:
                geo = file['name'].split('/')
                if len(geo) > 2:
                    geos.append(geo[1])
            except:
                continue
    return set(geos)


@st.cache_data(show_spinner=False)
def download_blob_to_stream(container_name, filename):
    '''function to download azure file data into io stream.'''

    blob_service_client = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    # readinto() downloads the blob contents to a stream and returns the number of bytes read
    stream = io.BytesIO()
    num_bytes = blob_client.download_blob(encoding='utf8').readall()
    return num_bytes


# @st.cache_data(show_spinner=False)
def get_geos_combined(container_name, run_id, multiple=True):
    '''function to combine all robyn outputs into one dataframe.'''
    if multiple:
        geos = get_geos_from_blob(container_name, run_id)
        st.info(geos)
        list_of_df = [] 
        for geo in geos:
            df_geo = pd.read_csv(io.StringIO(download_blob_to_stream(container_name, str(run_id) + f'/{geo}/pareto_alldecomp_matrix.csv')))
            df_geo['geo_var'] = geo
            list_of_df.append(df_geo)
            # list_of_df.append(
            #     pd.read_csv(
            #         io.StringIO(
            #             download_blob_to_stream(container_name, str(run_id) + f'/{geo}/pareto_alldecomp_matrix.csv')
            #         )
            #     )
            # )
            # df.merge(df1, on=['ds'], how='outer').groupby(['ds'], as_index=False)["dep_var", "depVarHat"].sum()
        
        df = pd.concat(list_of_df)
        return df

    
def get_brands_and_media(df, delimiter='-'):
    brands = set()
    media = set()

    for col in df.columns:
        if '-' in col:
            brand, medium = col.rsplit(delimiter, 1)
            brands.add(brand)
            media.add(medium)
    return list(brands), list(media)


@st.experimental_dialog('Rules for naming columns', width='large')
def column_info():
    with st.container(border=True):
        st.markdown('''
                    - **Week Start Column**:
                    We require a date column in YYYY-MM-DD format.
                    The column name should always be **WeekStart**
        ''')

# Function to calculate total spend by state
def calculate_total_spend_by_state(df, brand=None, media=None, delimiter='-'):
    if brand:
        df = df[[col for col in df.columns if col.startswith(brand + delimiter) or col in ['State', 'DMA', 'GrossSales Revenue']]]
    if media:
        df = df[[col for col in df.columns if col.endswith(delimiter + media) or col in ['State', 'DMA', 'GrossSales Revenue']]]
    
    df = df.copy()
    df['Total_Spend'] = df[[col for col in df.columns if delimiter in col]].sum(axis=1)
    total_spend_by_state = df.groupby(['State'])['Total_Spend'].sum().reset_index()
    total_spend_by_state_dma = df.groupby(['State','DMA'])['Total_Spend'].sum().reset_index()
    return total_spend_by_state, total_spend_by_state_dma


def calculate_total_target_by_geo(df, target_variable, geo_variable, geo):
    if not isinstance(geo, list):
        df = df[df[geo_variable] == geo]
    df = df[[geo_variable, target_variable]]
    target_by_geo = df.groupby([geo_variable]).sum()

    return target_by_geo


def calculate_total_media_spend_by_geo(df, media_vars, geo_variable, geo):
    if not isinstance(geo, list):
        df = df[df[geo_variable] == geo]
    df = df[[geo_variable, *media_vars]]
    media_by_geo = df.groupby([geo_variable]).sum()
    media_by_geo = pd.DataFrame(media_by_geo.T.sum(), columns=['sum'])

    return media_by_geo


def calculate_total_spend_by_brand(df, geo_variable, delimiter='-'):
    # df = df[[col for col in df.columns if col.startswith(brand + delimiter) or col in ['State', 'DMA', 'GrossSales Revenue']]]
    df['Total_Spend'] = df[[col for col in df.columns if delimiter in col]].sum(axis=1)
    return df['Total_Spend']


def calculate_media_percent_of_sale_by_geo(df, geo_variable, target_variable, geo, delimiter='-'):
    try:
        if isinstance(geo, list):
            df['Total_Spend'] = calculate_total_spend_by_brand(df, geo_variable, delimiter)
        else:
            df['Total_Spend'] = calculate_total_spend_by_brand(df, geo_variable, delimiter)
            df = df[df[geo_variable] == geo]
        df['media'] = (df['Total_Spend'] / df[target_variable]) * 100
        df = df[['media', geo_variable]]
        df = df.groupby([geo_variable]).mean()
        return df
    except KeyError:
        return pd.DataFrame()


def apply_state_brand_filter(df, brand, geo, geo_variable, media_vars, delimiter='-'):
    # applying brand filter
    df = df[media_vars]
    if isinstance(geo, list):
        brand_filter = [col for col in df.columns if col.startswith(brand + delimiter)]
        df = df[brand_filter]
        df.columns = df.columns.str.lstrip(brand + delimiter)
    else:
        brand_filter = [col for col in df.columns if col.startswith(brand + delimiter)]
        df = df[df[geo_variable] == geo][brand_filter]
        df.columns = df.columns.str.lstrip(brand + delimiter)
    return pd.DataFrame(df.sum(), columns=['sum'])


def aggregate_spend(df, geo, geo_variable, media_vars, type, delimiter='-'):
    # If geo is a list, filter the DataFrame for those geographies
    if isinstance(geo, list):
        df = df[df[geo_variable].isin(geo)]
    else:
        df = df[df[geo_variable] == geo]

    df = df[media_vars]

    if type == 'channel':
        # Select columns that contain the delimiter (indicating brand-channel columns)
        brand_columns = [col.split(delimiter)[0] for col in df.columns if delimiter in col]

        # Strip the brand name from the column names, keeping only the channel names
        df.columns = [col.split(delimiter)[0] if delimiter in col else col for col in df.columns]

        # Sum across all brands for each channel
        aggregated_df = pd.DataFrame(df[brand_columns].sum(), columns=[f'Sum of all {type}s'])    
    else:
        # Select columns that contain the delimiter (indicating brand-channel columns)
        channel_columns = [col.split(delimiter)[-1] for col in df.columns if delimiter in col]

        # Strip the brand name from the column names, keeping only the channel names
        df.columns = [col.split(delimiter)[-1] if delimiter in col else col for col in df.columns]

        # Sum across all channels for each brand
        aggregated_df = pd.DataFrame(df[channel_columns].sum(), columns=[f'Sum of all {type}s'])

    return aggregated_df


def apply_brand_filter(df, channel, geo, geo_variable, delimiter='-'):
    # applying channel filter
    if isinstance(geo, list):
        df = df[[col for col in df.columns if col.endswith(delimiter + channel) or col in [geo_variable]]]
        df = df.groupby([geo_variable]).sum()
        df.columns = df.columns.str.strip(delimiter + channel)
    else:
        df = df[[col for col in df.columns if col.endswith(delimiter + channel) or col in [geo_variable]]]
        df = df[df[geo_variable] == geo]
        df = df.groupby([geo_variable]).sum()
        df.columns = df.columns.str.strip(delimiter + channel)
    return pd.DataFrame(df.sum(), columns=['sum'])


def calculate_media_percent_of_sale_by_time(df, from_date, to_date, target_variable, geo, geo_variable, delimiter='-'):
    try:
        # df = df[[col for col in df.columns if col.startswith(brand + delimiter) or col in ['State', 'GrossSales Revenue']]]
        df['Total_Spend'] = calculate_total_spend_by_brand(df, '', delimiter)
        df = df[pd.to_datetime(df['WeekStart']).dt.year.isin([pd.to_datetime(from_date).year,pd.to_datetime(to_date).year])]
        df['media'] = (df['Total_Spend'] / df[target_variable]) * 100
        if not isinstance(geo, list):
            df = df[df[geo_variable] == geo]
        df['Year'] = df['WeekStart']
        df = df[['media', 'Year']]
        
        df = df.groupby(['Year']).mean()
        return df
    except KeyError:
        return pd.DataFrame()


def get_active_job_runs():
    params = {
        "expand_tasks": True,
        "active_only": True
    }
    response = requests.get(
        constants.list_jobs_api,
        json=params,
        headers=constants.headers
    )
    return response


def cancel_run(run_id):
    params = {
        "run_id": run_id
    }
    response = requests.post(
        constants.cancel_run_api,
        json=params,
        headers=constants.headers
    )
    return response


def get_job_runs(completed=False, page_token=""):
    if completed:
        params = {
            "expand_tasks": True,
            "completed_only": True,
            "page_token": page_token
        }
    else:
        params = {
            "expand_tasks": True,
            "page_token": page_token
        }
    response = requests.get(
        constants.list_jobs_api,
        json=params,
        headers=constants.headers
    )
    return response


@st.cache_data(show_spinner=False)
def get_single_job_run(run_id):
    params = {
        'include_resolved_values': True,
        'run_id': run_id
    }
    response = requests.get(
        constants.single_job_run,
        json=params,
        headers=constants.headers
    )
    return response.json()


_list = []
def get_all_job_runs(completed=False, page_token=''):
    if completed:
        params = {
            "expand_tasks": True,
            "completed_only": True,
            "page_token": page_token
        }
    else:
        params = {
            "expand_tasks": True,
            "page_token": page_token
        }
    response = requests.get(
        constants.list_jobs_api,
        json=params,
        headers=constants.headers
    )

    if response.status_code == 200:
        if 'runs' in response.json().keys():
            _list.extend(response.json()['runs'])
            get_all_job_runs(completed=True, page_token=response.json()['next_page_token'])
        else:
            return _list
    return _list


@st.cache_data(show_spinner=False, ttl=120)
def get_successful_job_runs(completed=False):
    response = get_job_runs(completed)
    run_ids = []

    try:
        for run in response.json()['runs']:
            if run['state']['result_state'] == 'SUCCESS':
                if 'datafile_path' in run['overriding_parameters']['notebook_params'].keys():
                    if run['overriding_parameters']['notebook_params']['datafile_path'] != '':
                        run_ids.append(str(run['run_id']) + ' - ' + str(pd.to_datetime(run['start_time'], unit="ms")))
    except Exception as e:
        print(e)
        st.error('Cannot connect to Databricks')
    return run_ids

def get_overall_optimization_outputs(container_name, run_id, geo_variable, user_provided_margin,
                                     opt_type1=True,
                                     get_individual_data=False,
                                     required_geo=None,
                                     regex=r'(\d+_)+max_response_reallocated.csv',
                                     ):
    '''function to combine all robyn outputs into one dataframe.'''

    blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    container_client = blob_service_client.get_container_client(container=container_name)
    pattern = re.compile(regex)
    geos = get_geos_from_blob(container_name, run_id)
    list_of_df = [] 

    if not get_individual_data:
        for geo in geos:
            if opt_type1:
                state_dir = f'opt_{str(run_id)}/{geo}'
            else:
                state_dir = f'opt_{str(run_id)}/{geo}/optimized_result'
            blob_list = container_client.list_blobs(name_starts_with=state_dir)
            for blob in blob_list:
                if opt_type1:
                    if pattern.search(blob.name):
                        df = optimization_calculations_read_csv(container_name, blob.name, geo_variable,
                                                            user_provided_margin, geo, opt_type1=True)
                        list_of_df.append(df)
                        break
                else:
                    if pattern.search(blob.name):
                        df = optimization_calculations_read_csv(container_name,blob.name, geo_variable,
                                                            user_provided_margin, geo, opt_type1=False)
                        list_of_df.append(df)
                        break
        overall_df = pd.concat(list_of_df, ignore_index=True)
        return overall_df
    else:
        if opt_type1:
            state_dir = f'opt_{str(run_id)}/{required_geo}'
        else:
            state_dir = f'opt_{str(run_id)}/{required_geo}/optimized_result'
        blob_list = container_client.list_blobs(name_starts_with=state_dir)
        for blob in blob_list:
            if opt_type1:
                if pattern.search(blob.name):
                    df = optimization_calculations_read_csv(container_name, blob.name, geo_variable,
                                                            user_provided_margin, required_geo, opt_type1=True)
                    break
            else:
                if pattern.search(blob.name):
                    df = optimization_calculations_read_csv(container_name,blob.name, geo_variable,
                                                        user_provided_margin, required_geo, opt_type1=False)
                    break
        return df

def optimization_calculations_read_csv(container_name, blob_name, geo_variable, user_provided_margin,
                                       state_value, opt_type1=True):
    blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    container_client = blob_service_client.get_container_client(container=container_name)
    blob_client = container_client.get_blob_client(blob_name)
    blob_data = blob_client.download_blob()
    csv_data = blob_data.readall()

    df = pd.read_csv(io.BytesIO(csv_data))
    if opt_type1:
        df[geo_variable] = state_value
        periods = int(df["periods"].iloc[0].split()[0])
        df["initial_spend"] = df["initSpendUnit"] * periods
        df["initial_response"] = df["initResponseUnit"] * periods
        df["optimized_spend"] = df["optmSpendUnit"] * periods
        df["optimized_response"] = df["optmResponseUnit"] * periods
        df["initial_iROAS"] = (df["initial_response"] * float(user_provided_margin)) / df["initial_spend"]
        df["optimized_iROAS"] = (df["optimized_response"] * float(user_provided_margin)) / df["optimized_spend"]
        return df
    else:
        df = df.groupby(['column_shocked','date'], as_index=False).agg({
            'spend': 'sum',
            'column_shocked_new_spend' : 'sum',
            'prediction_original' : 'sum',
            'prediction_shocked' : 'sum'
        })
        df = df.rename(columns={
            'column_shocked': 'spend_variable',
            'spend':'initial_spend',
            'column_shocked_new_spend': 'optimized_spend',
            'prediction_original': 'initial_response',
            'prediction_shocked': 'optimized_response'
        }) 
        df[geo_variable] = state_value
        df["initial_iROAS"] = (df["initial_response"] * float(user_provided_margin)) / df["initial_spend"]
        df["optimized_iROAS"] = (df["optimized_response"] * float(user_provided_margin)) / df["optimized_spend"]
        return df


@st.cache_data(show_spinner=False, ttl=120)
def get_opt_successful_job_runs(completed=False):
    response = get_job_runs(completed)
    robyn_run_ids = []
    opt_run_ids = []

    try:
        for run in response.json()['runs']:
            if run['state']['result_state'] == 'SUCCESS':
                if 'datafile_path' in run['overriding_parameters']['notebook_params'].keys():
                    if run['overriding_parameters']['notebook_params']['datafile_path'] != '':
                        robyn_run_ids.append(str(run['run_id']) + ' - ' + str(pd.to_datetime(run['start_time'], unit="ms")))
                    else:
                        opt_run_ids.append(str(run['run_id']) + ' - ' + str(pd.to_datetime(run['start_time'], unit="ms")))
    except Exception as e:
        print(e)
        st.error('Cannot connect to Databricks')
    return robyn_run_ids, opt_run_ids

def get_data_frame_download_to_stream(container_name, filename):
    '''function to download azure file data into io stream.'''

    blob_service_client = BlobServiceClient.from_connection_string(
        f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    # readinto() downloads the blob contents to a stream and returns the number of bytes read
    with io.BytesIO() as buf:
        blob_client.download_blob().download_to_stream(buf)
        buf.seek(0)
        if filename.split("/")[-1].endswith('.csv'):
            data = pd.read_csv(buf)
        else:
            data = pd.read_excel(buf)
    return data


def extract_brand_channel_optimization(channel, brands):
    for brand in sorted(brands, key=len, reverse=True):
        if channel.startswith(brand):
            return brand, channel[len(brand):]
    return None, channel


def progress_bar(pg_caption, pg_int_percentage, pg_colour, pg_bgcolour, actual_time):
    pg_int_percentage = str(round(pg_int_percentage, 2)).zfill(2)
    pg_html = f"""<table style="width:100%; border-style: none;">
                <tr>
                    <td style='background-color:{pg_bgcolour};'>{pg_caption}: <span style='accent-color: {pg_colour}; bgcolor: transparent;'>
                        <progress value='{pg_int_percentage}' max='100'>{pg_int_percentage}%</progress> </span>{pg_int_percentage}% <span style="color: gray;">(Estimated Run Time: {actual_time})</span>
                    </td>
                </tr>
            </table><br>"""
    return pg_html

def get_optimization_outputs(container_name, run_id, regex=r".*\.csv$"):
    '''function to combine all robyn outputs into one dataframe.'''

    blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    container_client = blob_service_client.get_container_client(container=container_name)
    pattern = re.compile(regex)
    directory = f'opt_{str(run_id)}/optimized_result'
    blob_list = container_client.list_blobs(name_starts_with=directory)
    for blob in blob_list:
        if pattern.search(blob.name):
            blob_client = container_client.get_blob_client(blob.name)
            blob_data = blob_client.download_blob()
            csv_data = blob_data.readall()
            df = pd.read_csv(io.BytesIO(csv_data))
    
    return df

def get_shocked_outputs(container_name, run_id, regex=r".*\.csv$",get_individual_data=False, geo_val=None):
    '''function to combine all robyn outputs into one dataframe.'''

    blob_service_client = BlobServiceClient.from_connection_string(
    f"DefaultEndpointsProtocol=https;AccountName={constants.azure_storage_account_name};AccountKey={constants.azure_storage_account_key}"
    )
    container_client = blob_service_client.get_container_client(container=container_name)
    pattern = re.compile(regex)
    if not get_individual_data:
        geos = get_geos_from_blob(container_name, run_id)
        overall_df = pd.DataFrame()
        for geo in geos:
            directory = f'{str(run_id)}/{geo}/shocked_df'
            blob_list = container_client.list_blobs(name_starts_with=directory)
            for blob in blob_list:
                if pattern.search(blob.name):
                    blob_client = container_client.get_blob_client(blob.name)
                    blob_data = blob_client.download_blob()
                    csv_data = blob_data.readall()
                    df = pd.read_csv(io.BytesIO(csv_data))
                    df['geo'] = geo
                    overall_df = pd.concat([overall_df, df], ignore_index=True)
        return overall_df