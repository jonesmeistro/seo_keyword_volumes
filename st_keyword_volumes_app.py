import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
from country_codes import country_code_dict  # Import country codes

# Load secrets
SEMRUSH_API_URL = 'https://api.semrush.com'
SEMRUSH_API_KEY = st.secrets["SEMRUSH_API_KEY"]  # Use Streamlit secrets management

# Set page configuration
st.set_page_config(page_title="Keyword Data Fetcher")

# Prioritized countries list
priority_countries = [
    ("GB", "United Kingdom"),
    ("US", "United States"),
    ("FR", "France"),
    ("DE", "Germany"),
    ("IT", "Italy"),
    ("ES", "Spain"),
    ("NL", "Netherlands")
]

# Sort the rest of the countries alphabetically by name
sorted_countries = sorted([(code, name) for code, name in country_code_dict.items() if (code, name) not in priority_countries], key=lambda x: x[1])

# Combine prioritized and sorted country lists
country_list = priority_countries + sorted_countries

# Functions
def fetch_semrush_data(keyword, database, display_date=None):
    """Fetch keyword volume data from SEMrush."""
    semrush_database = database.lower()  # Convert database code to lowercase
    params = {
        'type': 'phrase_this',
        'key': SEMRUSH_API_KEY,
        'phrase': keyword,
        'export_columns': 'Ph,Nq,Cp,Co,Nr,Td',
        'database': semrush_database,
    }
    if display_date:
        params['display_date'] = display_date
    response = requests.get(SEMRUSH_API_URL, params=params)
    if response.status_code == 200:
        lines = response.text.strip().split('\n')
        headers = lines[0].strip().split(';')
        headers = [header.strip() for header in headers]  # Clean up headers
        data = [dict(zip(headers, line.strip().split(';'))) for line in lines[1:]]
        return pd.DataFrame(data)
    else:
        st.error(f"API request failed with status {response.status_code} and message: {response.text}")
        return pd.DataFrame()

def calculate_monthly_volumes(row, end_date):
    """Calculate monthly search volumes from Trends data."""
    trends = list(map(float, row['Trends'].split(',')))
    search_volume = int(row['Search Volume'])
    trends_sum = sum(trends)
    monthly_volumes = [(trend / trends_sum) * search_volume for trend in trends]

    # Generate monthly columns in chronological order
    month_year_columns = []
    current_date = end_date - relativedelta(months=11)
    for i in range(12):
        month_year_columns.append(f"{current_date:%b-%Y}")
        current_date += relativedelta(months=1)

    return pd.Series(monthly_volumes, index=month_year_columns)

def simulate_ahrefs_data(keyword, start_date, end_date):
    """Simulate fetching historical data from Ahrefs."""
    simulated_data = {}
    current_date = start_date
    while current_date <= end_date:
        date_str = f"{current_date:%b-%Y}"
        simulated_data[date_str] = f"simulated_data_for_{keyword}_{date_str}"
        current_date += relativedelta(months=1)
    return pd.Series(simulated_data)

def simulate_ahrefs_data_last_12_months(keyword, end_date):
    """Simulate fetching historical data from Ahrefs for the last 12 months."""
    return simulate_ahrefs_data(keyword, end_date - relativedelta(months=11), end_date)

# Streamlit UI
st.title("Keyword Data Fetcher")

with st.form(key='keyword_form'):
    keyword = st.text_input("Keywords (comma-separated):")
    selected_country = st.selectbox("Country:", [name for _, name in country_list])
    datasource_se = st.checkbox("SEMrush", value=True)
    datasource_ah = st.checkbox("Ahrefs", value=True)
    start_date = st.date_input("Start Date:")
    end_date = st.date_input("End Date:")
    submit_button = st.form_submit_button(label='Fetch Data')

if submit_button:
    keywords = [k.strip() for k in keyword.split(',')]
    datasources = []
    if datasource_se:
        datasources.append('semrush')
    if datasource_ah:
        datasources.append('ahrefs')
    
    # Map selected country name to code
    selected_country_code = [code for code, name in country_list if name == selected_country][0]
    semrush_selected_country_code = [code for code, name in country_list if name == selected_country][0]
    if selected_country_code == "GB":
        semrush_selected_country_code = "UK"

    end_date = datetime.combine(end_date, datetime.min.time())
    start_date = datetime.combine(start_date, datetime.min.time()) if start_date else end_date - relativedelta(months=11)

    dataframes = []

    for keyword in keywords:
        if 'semrush' in datasources:
            df_semrush = fetch_semrush_data(keyword.strip(), database=semrush_selected_country_code, display_date=end_date.strftime("%Y%m15"))
            if not df_semrush.empty:
                df_semrush.columns = df_semrush.columns.str.strip()
                if 'Trends' in df_semrush.columns:
                    df_semrush['Trends'] = df_semrush['Trends'].str.strip()
                    monthly_df = df_semrush.apply(lambda row: calculate_monthly_volumes(row, end_date), axis=1)
                    combined_df = pd.concat([df_semrush, monthly_df], axis=1)
                    combined_df['Datasource'] = 'SEMrush'
                    dataframes.append(combined_df)
                else:
                    st.warning(f"Skipping keyword '{keyword}' as 'Trends' column is missing.")
            else:
                st.warning(f"No data found for keyword '{keyword}'.")

        if 'ahrefs' in datasources:
            if 'semrush' in datasources:
                df_ahrefs = simulate_ahrefs_data_last_12_months(keyword.strip(), end_date)
            else:
                df_ahrefs = simulate_ahrefs_data(keyword.strip(), start_date, end_date)
            
            if not df_ahrefs.empty:
                df_ahrefs = df_ahrefs.to_frame().T
                df_ahrefs['Keyword'] = keyword.strip()
                df_ahrefs['Search Volume'] = ''
                df_ahrefs['CPC'] = ''
                df_ahrefs['Competition'] = ''
                df_ahrefs['Number of Results'] = ''
                df_ahrefs['Datasource'] = 'Ahrefs'
                df_ahrefs = df_ahrefs[['Keyword', 'Search Volume', 'CPC', 'Competition', 'Number of Results', 'Datasource'] + list(df_ahrefs.columns[:-6])]
                dataframes.append(df_ahrefs)

    if dataframes:
        final_df = pd.concat(dataframes, ignore_index=True)
        st.session_state['data'] = final_df.to_csv(index=False)
        st.success('Data fetched and CSV available to download')
        st.dataframe(final_df)
        csv = st.session_state.get('data')
        st.download_button(
            label="Download CSV",
            data=csv,
            file_name='keyword_data.csv',
            mime='text/csv',
        )
    else:
        st.error('No data available for the provided criteria.')
