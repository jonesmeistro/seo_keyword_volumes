import streamlit as st
import requests
import pandas as pd
from dateutil import tz
import os
import io
from datetime import datetime
from country_codes import country_code_dict

AHREFS_API_KEY = st.secrets["AHREFS_API_KEY"]
SEMRUSH_API_KEY = st.secrets["SEMRUSH_API_KEY"]

def fetch_ahrefs_volume_data(keyword: str, country: str = "us", year: int = None):
    url = "https://api.ahrefs.com/v3/keywords-explorer/overview"
    headers = {"Authorization": f"Bearer {AHREFS_API_KEY}"}
    select_fields = "volume,cpc,global_volume,difficulty"  # Add other fields as needed
    params = {
        "output": "json",
        "country": country_code.lower(),
        "keywords": keyword,
        "select": select_fields
    }

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        print(data)
        if 'keywords' in data and data['keywords']:
            keyword_data = data['keywords'][0]
            return {
                "volume": keyword_data.get('volume', 'N/A'),
                "cpc": keyword_data.get('cpc', 'N/A'),
                "global_volume": keyword_data.get('global_volume', 'N/A'),
                "cps": keyword_data.get('cps', 'N/A'),
                "difficulty": keyword_data.get('difficulty', 'N/A')
            }
        else:
            return {"volume": 'Not found'}
    else:
        return {"volume": f'API request failed: {response.status_code}'}

def fetch_semrush_volume_data(keyword: str, country: str = "us"):

    SEMRUSH_API_URL = 'https://api.semrush.com'


    params = {
        'type': 'phrase_all',
        'key': SEMRUSH_API_KEY,
        'phrase': keyword,
        'export_columns': 'Ph,Nq,Cp,Co,Nr',
        'database': semrush_country_code.lower(),
    }

    response = requests.get(SEMRUSH_API_URL, params=params)
    if response.status_code == 200:
        lines = response.text.strip().split('\n')
        headers = lines[0].split(';')
        data = [
            dict(zip(headers, line.split(';')))
            for line in lines[1:]
        ]
        return {"metrics": data}
    else:
        return {"volume": f'API request failed: {response.status_code}'}

def fetch_ahrefs_volume_history(keyword: str, country: str = "us", start_date=None, end_date=None):
    if start_date is not None:
        start_date = pd.to_datetime(start_date)
        start_date = start_date.replace(tzinfo=tz.tzutc())
    if end_date is not None:
        end_date = pd.to_datetime(end_date)
        end_date = end_date.replace(tzinfo=tz.tzutc())

    url = "https://api.ahrefs.com/v3/keywords-explorer/volume-history"
    headers = {"Authorization": f"Bearer {AHREFS_API_KEY}"}
    params = {"output": "json", "country": country_code.lower(), "keyword": keyword}

    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        if 'metrics' in data and data['metrics']:
            volumes = []
            filtered_data = {}
            for metric in data['metrics']:
                metric_date = pd.to_datetime(metric['date']).tz_convert(tz.tzutc())  # Convert to UTC
                if start_date is None or end_date is None or (start_date <= metric_date <= end_date):
                    date_str = metric_date.strftime('%b %Y')
                    volume = metric['volume']
                    filtered_data[f"Ahrefs Search Volume: {date_str}"] = volume
                    volumes.append(volume)
            average_volume = sum(volumes) / len(volumes) if volumes else 'N/A'
            return {"average_volume": average_volume, "monthly_volumes": filtered_data}
        else:
            return {"average_volume": 'N/A', "monthly_volumes": {}}
    else:
        return {"volume": f'API request failed: {response.status_code}', "monthly_volumes": {}}

def calculate_monthly_percentage_change(volumes):
    changes = []
    months = sorted(volumes.keys())  # Ensure the months are in chronological order

    for i in range(1, len(months)):
        if volumes[months[i-1]] and volumes[months[i]] and volumes[months[i-1]] != 0:
            # Calculate percentage change
            change = (volumes[months[i]] - volumes[months[i-1]]) / volumes[months[i-1]] * 100
            changes.append(change)

    # Calculate average change if there are any changes calculated
    return sum(changes) / len(changes) if changes else 'N/A'

def calculate_monthly_percentage_change(volumes):
    changes = []
    months = sorted(volumes.keys())  # Ensure the months are in chronological order

    for i in range(1, len(months)):
        if volumes[months[i-1]] and volumes[months[i]] and volumes[months[i-1]] != 0:
            # Calculate percentage change
            change = (volumes[months[i]] - volumes[months[i-1]]) / volumes[months[i-1]] * 100
            changes.append(change)

    # Calculate average change if there are any changes calculated
    return sum(changes) / len(changes) if changes else 'N/A'


def enhance_keywords(keywords, data_sources, start_date=None, end_date=None, country_code="US"):
    combined_data = {}
    for keyword in keywords:
        data_for_keyword = {}

        if "Ahrefs" in data_sources:
            ahrefs_data = fetch_ahrefs_volume_data(keyword, country_code)
            data_for_keyword["ahrefs"] = ahrefs_data

        if "SEMrush" in data_sources:
            semrush_data = fetch_semrush_volume_data(keyword, country_code)
            data_for_keyword["semrush"] = semrush_data

        if "Ahrefs History" in data_sources and start_date is not None and end_date is not None:
            ahrefs_history_data = fetch_ahrefs_volume_history(keyword, country_code, start_date, end_date)
            data_for_keyword["ahrefs_history"] = ahrefs_history_data

        combined_data[keyword] = data_for_keyword

    return combined_data


def enhance_csv_with_detailed_volume(keywords, combined_data, data_sources, output_filename="enhanced_keywords.csv"):
    rows_list = []

    for keyword in keywords:
        keyword_data = {'Keyword': keyword}

        if "SEMrush" in data_sources and 'semrush' in combined_data[keyword]:
            semrush_metrics = combined_data[keyword]['semrush'].get("metrics", [])
            if semrush_metrics:
                semrush_data = semrush_metrics[0]
                keyword_data.update({
                    'SEMrush Search Volume': semrush_data.get('Search Volume', 'N/A'),
                    'SEMrush CPC': semrush_data.get('CPC', 'N/A'),
                    'SEMrush Competition': semrush_data.get('Competition', 'N/A'),
                    'SEMrush Number of Results': semrush_data.get('Number of Results', 'N/A')
                })

        if "Ahrefs" in data_sources and 'ahrefs' in combined_data[keyword]:
            ahrefs_data = combined_data[keyword]['ahrefs']
            keyword_data.update({
                "Ahrefs Volume": ahrefs_data.get('volume', 'N/A'),
                "Ahrefs CPC": ahrefs_data.get('cpc', 'N/A'),
                "Ahrefs Global Volume": ahrefs_data.get('global_volume', 'N/A'),
                "Ahrefs CPS": ahrefs_data.get('cps', 'N/A'),
                "Ahrefs Difficulty": ahrefs_data.get('difficulty', 'N/A')
            })

        if "Ahrefs History" in data_sources and 'ahrefs_history' in combined_data[keyword]:
            ahrefs_history_data = combined_data[keyword]['ahrefs_history']
            monthly_volumes = ahrefs_history_data.get('monthly_volumes', {})
            keyword_data.update(monthly_volumes)
            
            # Calculate keyword-specific average monthly search rounded to the nearest whole number
            volumes = [v for v in monthly_volumes.values() if isinstance(v, int)]
            average_monthly_search = round(sum(volumes) / len(volumes)) if volumes else 0
            keyword_data['Average Monthly Search in Date Range'] = average_monthly_search

            # Calculate the average monthly % change rounded to the nearest whole number and formatted as percentage
            avg_monthly_pct_change = calculate_monthly_percentage_change(monthly_volumes)
            keyword_data['Average Monthly % Change'] = f"{avg_monthly_pct_change}%"

        rows_list.append(keyword_data)

    final_df = pd.DataFrame(rows_list)
    final_df.fillna('N/A', inplace=True)
    cols = ['Keyword'] + [col for col in final_df.columns if col != 'Keyword']
    final_df = final_df[cols]

    return final_df.to_csv(index=False)  # Return CSV content instead of saving to a file

st.title('Keyword Volume Analysis Tool')

# Country selection
country = st.selectbox('Select Country', sorted(country_code_dict.values()))
country_code = next((code for code, name in country_code_dict.items() if name == country), None)
semrush_country_code = country_code.lower()

if country == "United Kingdom":  # Handle GB as UK in API requests
    semrush_country_code = "uk"

# Keyword input either via text input or file upload
keywords_input = st.text_input('Enter keywords separated by commas')
uploaded_file = st.file_uploader("Or upload a CSV file with keywords in the first column", type=["csv"])

if uploaded_file is not None:
    data = pd.read_csv(uploaded_file)
    data.rename(columns={data.columns[0]: 'Keyword'}, inplace=True)
    keywords = data['Keyword'].tolist()
elif keywords_input:
    keywords = keywords_input.split(',')

# Data sources and date range selection
data_sources = ["SEMrush", "Ahrefs", "Ahrefs History"]  # For example
start_date = st.date_input("Start Date", datetime.now())
end_date = st.date_input("End Date", datetime.now())

# Enhancement process
if st.button('Enhance Keywords'):
    if not keywords or keywords == ['']:
        st.error("Please enter or upload at least one keyword.")
    else:
        result = enhance_keywords(keywords, data_sources, start_date, end_date, semrush_country_code)
        if result:
            # Generate CSV content from the result
            csv_content = enhance_csv_with_detailed_volume(keywords, result, data_sources)
            st.download_button(
                label="Download CSV",
                data=csv_content,
                file_name='enhanced_keywords.csv',
                mime='text/csv'
            )
            # Optionally display the DataFrame in the app as well
            df = pd.read_csv(io.StringIO(csv_content))
            st.dataframe(df)
        else:
            st.error("No data to display.")

