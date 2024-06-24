#!/usr/bin/env python3
# pip install requests requests-oauthlib 


# FOR THIS TO RUN MUST HAVE API ACCESS TO NETSUITE SUITETALK API INSTANCE


import os
import json
import logging
import argparse
import requests
from requests_oauthlib import OAuth1
from singer import utils, Catalog, CatalogEntry, Schema, SchemaMessage, RecordMessage, write_message, write_bookmark, write_state
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv



load_dotenv()

REQUIRED_CONFIG_KEYS = [
    "start_date", "username", "password",
    "consumer_key", "consumer_secret",
    "token", "token_secret"
]
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def get_netsuite_data(config):
    base_url = 'https://your_account_id.suitetalk.api.netsuite.com/services/rest/record/v1/'
    endpoints = {
        "Customer": "customer",
        "SalesOrder": "salesOrder"
    }
    
    auth = OAuth1(
        config['consumer_key'],
        config['consumer_secret'],
        config['token'],
        config['token_secret']
    )
    
    data = {}
    for stream, endpoint in endpoints.items():
        url = base_url + endpoint
        response = requests.get(url, auth=auth)
        if response.status_code == 200:
            data[stream] = response.json()['items']
        else:
            LOGGER.error(f"Failed to fetch data from {endpoint}: {response.text}")
            data[stream] = []
    
    return data

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schemas():
    """Load schemas from schemas folder"""
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        try:
            with open(path) as file:
                schemas[file_raw] = Schema.from_dict(json.load(file))
        except json.JSONDecodeError as e:
            LOGGER.error(f"Error decoding JSON from file {path}: {e}")
            raise
    return schemas

def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        stream_metadata = []
        key_properties = ["internalId"]  # Example key property
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None
            )
        )
    return Catalog(streams)

def transform_data(data, stream_name):
    transformed_data = []
    for record in data:
        transformed_record = {}
        for key, value in record.items():
            if isinstance(value, datetime):
                transformed_record[key] = value.isoformat()
            elif isinstance(value, list):
                # Flatten the list into a string representation
                flattened_value = ', '.join([str(v) for v in value])
                transformed_record[key] = flattened_value
            else:
                transformed_record[key] = value
        transformed_data.append(transformed_record)
    return transformed_data

class CustomCatalog(Catalog):
    def get_selected_streams(self, state):
        selected_stream_ids = state.get('selected_streams', [])
        return [stream for stream in self.streams if stream.tap_stream_id in selected_stream_ids]

def sync(config, state, catalog):
    netsuite_data = get_netsuite_data(config)
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True 

        schema_message = SchemaMessage(
            stream=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties
        )
        write_message(schema_message)

        tap_data = netsuite_data.get(stream.tap_stream_id, [])

        transformed_data = transform_data(tap_data, stream.tap_stream_id)
        
        max_bookmark = None
        for row in transformed_data:
            record_message = RecordMessage(
                stream=stream.tap_stream_id,
                record=row,
                version=None,
                time_extracted=None
            )
            write_message(record_message)

            if bookmark_column:
                if is_sorted:
                    state = write_bookmark(state, stream.tap_stream_id, bookmark_column, row[bookmark_column])
                    write_state(state)
                else:
                    max_bookmark = max(max_bookmark, row[bookmark_column]) if max_bookmark else row[bookmark_column]
        if bookmark_column and not is_sorted:
            state = write_bookmark(state, stream.tap_stream_id, bookmark_column, max_bookmark)
            write_state(state)
    return



def load_to_google_sheets(data, spreadsheet_id, range_name, credentials_file):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    
    body = {
        'values': [list(data[0].keys())] + [list(row.values()) for row in data]
    }
    result = sheet.values().update(spreadsheetId=spreadsheet_id, range=range_name, valueInputOption="RAW", body=body).execute()

def load_to_bigquery(data, dataset_id, table_id, credentials_file):
    credentials = service_account.Credentials.from_service_account_file(credentials_file)
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    
    table_ref = client.dataset(dataset_id).table(table_id)
    job = client.load_table_from_json(data, table_ref)
    job.result()


## for Extensions of target examples
'''def load_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

def load_to_postgres(data, connection_string, table_name, max_retries=5, wait_time=5):
    engine = create_engine(connection_string)
    df = pd.DataFrame(data)
    for attempt in range(max_retries):
        try:
            df.to_sql(table_name, engine, if_exists='replace', index=False)
            LOGGER.info(f"Data successfully written to Postgres table {table_name}")
            return
        except OperationalError as e:
            LOGGER.error(f"Failed to connect to Postgres: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    LOGGER.error(f"Exceeded maximum retries ({max_retries}) to connect to Postgres.")'''

def parse_args(required_config_keys):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True, help="Config file")
    parser.add_argument("--state", required=False, help="State file")
    parser.add_argument("--catalog", required=False, help="Catalog file")
    parser.add_argument("--discover", action="store_true", help="Run discovery mode")
    args = parser.parse_args()

    with open(args.config) as input_file:
        config = json.load(input_file)
        for key in required_config_keys:
            if key not in config:
                raise Exception(f"Config file is missing required key: {key}")
    
    state = None
    if args.state:
        try:
            with open(args.state) as input_file:
                state = json.load(input_file)
        except FileNotFoundError:
            state = {}
    
    catalog = None
    if args.catalog:
        with open(args.catalog) as input_file:
            catalog = CustomCatalog.from_dict(json.load(input_file))
    
    return args, config, state, catalog

def main():
    try:
        args, config, state, catalog = parse_args(REQUIRED_CONFIG_KEYS)

        if args.discover:
            catalog = discover()
            catalog.dump()
        else:
            if not catalog:
                catalog = discover()
            
            netsuite_data = get_netsuite_data(config)
            transformed_data = {stream: transform_data(data, stream) for stream, data in netsuite_data.items()}
            
            for stream, data in transformed_data.items():
                # load_to_csv(data, f"{stream}.csv") 
                load_to_google_sheets(data, config['spreadsheet_id'], f'{stream}!A1', config['credentials_file'])
                load_to_bigquery(data, config['bigquery_dataset_id'], stream, config['credentials_file'])
            
            sync(config, state, catalog)
    except Exception as e:
        LOGGER.error(f"Error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()
