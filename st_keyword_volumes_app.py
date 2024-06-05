import streamlit as st
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from io import StringIO
from country_codes import country_code_dict  # Import country codes


# Load secrets
SEMRUSH_API_KEY = st.secrets["SEMRUSH_API_KEY"]  # Use Streamlit secrets management
AHREFS_API_KEY = st.secrets["AHREFS_API_KEY"]  # Use Streamlit secrets management

SEMRUSH_API_URL = 'https://api.semrush.com'
AHREFS_OVERVIEW_API_URL = "https://api.ahrefs.com/v3/keywords-explorer/overview"
AHREFS_HISTORY_API_URL = "https://api.ahrefs.com/v3/keywords-explorer/volume-history"


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
    semrush_database = database.lower()
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
        headers = [header.strip() for header in headers]
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

def fetch_ahrefs_overview_data(keyword, country):
    """Fetch keyword overview data from Ahrefs."""
    headers = {
        'Authorization': 'Bearer ' + AHREFS_API_KEY,
        'Accept': 'application/json'
    }
    params = {
        'keywords': keyword,
        'country': country.lower(),
        'output': 'json',
        'select': 'volume,cpc,global_volume,parent_volume'
    }
    response = requests.get(AHREFS_OVERVIEW_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        if 'keywords' in data:
            return data['keywords'][0]
        else:
            return {}
    else:
        st.error(f"API request failed with status {response.status_code} and message: {response.text}")
        return {}

def fetch_ahrefs_history_data(keyword, country, start_date, end_date, fetch_last_12_months=False):
    """Fetch historical keyword volume data from Ahrefs."""
    headers = {
        'Authorization': 'Bearer ' + AHREFS_API_KEY,
        'Accept': 'application/json'
    }
    if fetch_last_12_months:
        end_date = datetime.today()
        start_date = end_date - relativedelta(months=11)
    
    params = {
        'country': country.lower(),
        'keyword': keyword,
        'output': 'json',
        'start_date': start_date.strftime('%Y-%m-%d'),
        'end_date': end_date.strftime('%Y-%m-%d')
    }
    
    response = requests.get(AHREFS_HISTORY_API_URL, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        monthly_data = []
        for metric in data['metrics']:
            date_str = datetime.strptime(metric['date'], '%Y-%m-%dT%H:%M:%SZ').strftime('%b-%Y')
            monthly_data.append({'Month-Year': date_str, 'Volume': metric['volume']})
        return pd.DataFrame(monthly_data)
    else:
        st.error(f"API request failed with status {response.status_code} and message: {response.text}")
        return pd.DataFrame()

# Streamlit UI
st.title("Keyword Data Fetcher")

with st.form(key='keyword_form'):
    keywords = st.text_area("Keywords (one per line):")
    selected_country = st.selectbox("Country:", [name for _, name in country_list])
    datasource_se = st.checkbox("SEMrush", value=True)
    datasource_ah = st.checkbox("Ahrefs", value=True)
    start_date = st.date_input("Start Date:")
    end_date = st.date_input("End Date:")
    submit_button = st.form_submit_button(label='Fetch Data')

if submit_button:
    # Split the keywords by new lines and remove any extra whitespace
    keywords = [k.strip() for k in keywords.split('\n') if k.strip()]
    datasources = []
    if datasource_se:
        datasources.append('semrush')
    if datasource_ah:
        datasources.append('ahrefs')
    
    selected_country_code = [code for code, name in country_list if name == selected_country][0]
    semrush_selected_country_code = [code for code, name in country_list if name == selected_country][0]
    if selected_country_code == "GB":
        semrush_selected_country_code = "UK"

    end_date = datetime.combine(end_date, datetime.min.time())
    start_date = datetime.combine(start_date, datetime.min.time()) if start_date else end_date - relativedelta(months=11)

    dataframes = []

    for keyword in keywords:
        combined_df = pd.DataFrame()
        
        if 'semrush' in datasources:
            df_semrush = fetch_semrush_data(keyword.strip(), database=semrush_selected_country_code, display_date=end_date.strftime("%Y%m15"))
            if not df_semrush.empty:
                df_semrush.columns = df_semrush.columns.str.strip()
                if 'Trends' in df_semrush.columns:
                    df_semrush['Trends'] = df_semrush['Trends'].str.strip()
                    monthly_df = df_semrush.apply(lambda row: calculate_monthly_volumes(row, end_date), axis=1)
                    df_semrush = pd.concat([df_semrush, monthly_df], axis=1)
                    df_semrush['Datasource'] = 'SEMrush'
                    dataframes.append(df_semrush)
                else:
                    st.warning(f"Skipping keyword '{keyword}' as 'Trends' column is missing.")
            else:
                st.warning(f"No data found for keyword '{keyword}'.")

        if 'ahrefs' in datasources:
            if 'semrush' in datasources:
                df_ahrefs_history = fetch_ahrefs_history_data(keyword, selected_country_code, start_date, end_date, fetch_last_12_months=True)
                ahrefs_overview_data = fetch_ahrefs_overview_data(keyword, selected_country_code)
                date_columns = [datetime.strptime(col, '%b-%Y').strftime('%b-%Y') for col in df_semrush.columns if col not in ['Keyword', 'Search Volume', 'CPC', 'Competition', 'Number of Results', 'Trends', 'Datasource']]
            else:
                df_ahrefs_history = fetch_ahrefs_history_data(keyword, selected_country_code, start_date, end_date)
                ahrefs_overview_data = fetch_ahrefs_overview_data(keyword, selected_country_code)
                dates = [start_date + relativedelta(months=i) for i in range((end_date.year - start_date.year) * 12 + end_date.month - start_date.month + 1)]
                date_columns = [date.strftime('%b-%Y') for date in dates]
                combined_df = pd.DataFrame(columns=['Keyword', 'Search Volume', 'CPC', 'Global Volume', 'Parent Volume'] + date_columns)

            if not df_ahrefs_history.empty:
                row_ahrefs = {
                    'Keyword': keyword,
                    'Search Volume': ahrefs_overview_data.get('volume', ''),
                    'CPC': ahrefs_overview_data.get('cpc', ''),
                    'Global Volume': ahrefs_overview_data.get('global_volume', ''),
                    'Parent Volume': ahrefs_overview_data.get('parent_volume', ''),
                    'Datasource': 'Ahrefs'
                }
                for _, ahrefs_row in df_ahrefs_history.iterrows():
                    if ahrefs_row['Month-Year'] in date_columns:
                        row_ahrefs[ahrefs_row['Month-Year']] = ahrefs_row['Volume']
                combined_df = pd.concat([combined_df, pd.DataFrame([row_ahrefs])], ignore_index=True)
            else:
                row_ahrefs = {
                    'Keyword': keyword,
                    'Search Volume': ahrefs_overview_data.get('volume', ''),
                    'CPC': ahrefs_overview_data.get('cpc', ''),
                    'Global Volume': ahrefs_overview_data.get('global_volume', ''),
                    'Parent Volume': ahrefs_overview_data.get('parent_volume', ''),
                    'Datasource': 'Ahrefs'
                }
                for date_col in date_columns:
                    row_ahrefs[date_col] = 0  # Or some default value if no historical data available
                combined_df = pd.concat([combined_df, pd.DataFrame([row_ahrefs])], ignore_index=True)
            
            dataframes.append(combined_df)

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

