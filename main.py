import streamlit as st
import pandas as pd
from streamlit_navigation_bar import st_navbar
from config import constants as cons
from assets.css.designing import css
from databricks_jobs import jobs
from database_connection import database
from dotenv import load_dotenv
from input_data import upload,features,run_model

# global design
design=css()

def main():

    st.set_page_config(
        layout="wide",
        page_title='Sally-Beauty'
    )
    
    styles = cons.styles

    options = cons.options

    st.html(f"""<style>{design}</style>""")

    page = st_navbar(
        ["Input Data", "Jobs"],
        options=options,
        styles=styles,
        selected="Input Data",
        logo_page=None,
    )

    if page == 'Input Data':

        sub_tabs = st.tabs(["Input Data", "Define Features", "Run Model"])

        with sub_tabs[0]:
            upload.input_upload()

        with sub_tabs[1]:
            features.input_features()

        with sub_tabs[2]:
            run_model.run_model()


    if page == "Jobs":
        new_dbobj=database.database()
        jobs.list_jobs(new_dbobj)

if __name__ == "__main__":
    main()