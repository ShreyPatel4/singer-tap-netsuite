#!/usr/bin/env python3
import os
import json
import singer
import logging
import pandas as pd
import argparse
import singer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from sqlalchemy import create_engine
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy.exc import OperationalError
import time
from singer.catalog import Catalog as SingerCatalog


load_dotenv()

REQUIRED_CONFIG_KEYS = ["start_date", "username", "password"]
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Mock NetSuite API response 
def mock_get_netsuite_data():
    return {
        "Customer": [
            {"taxable": True, "internalId": "123", "lastModifiedDate": "2024-01-01T00:00:00Z", "entityId": "Cust1", "entityStatus": "Active", "email": "cust1@example.com", "dateCreated": "2023-01-01T00:00:00Z", "subsidiary": "Subsidiary1"},
            {"taxable": False, "internalId": "124", "lastModifiedDate": "2024-02-01T00:00:00Z", "entityId": "Cust2", "entityStatus": "Inactive", "email": "cust2@example.com", "dateCreated": "2023-02-01T00:00:00Z", "subsidiary": "Subsidiary2"},
            {"taxable": True, "internalId": "125", "lastModifiedDate": "2024-03-01T00:00:00Z", "entityId": "Cust3", "entityStatus": "Active", "email": "cust3@example.com", "dateCreated": "2023-03-01T00:00:00Z", "subsidiary": "Subsidiary1"},
            {"taxable": False, "internalId": "126", "lastModifiedDate": "2024-04-01T00:00:00Z", "entityId": "Cust4", "entityStatus": "Inactive", "email": "cust4@example.com", "dateCreated": "2023-04-01T00:00:00Z", "subsidiary": "Subsidiary2"},
            {"taxable": True, "internalId": "127", "lastModifiedDate": "2024-05-01T00:00:00Z", "entityId": "Cust5", "entityStatus": "Active", "email": "cust5@example.com", "dateCreated": "2023-05-01T00:00:00Z", "subsidiary": "Subsidiary1"},
            {"taxable": False, "internalId": "128", "lastModifiedDate": "2024-06-01T00:00:00Z", "entityId": "Cust6", "entityStatus": "Inactive", "email": "cust6@example.com", "dateCreated": "2023-06-01T00:00:00Z", "subsidiary": "Subsidiary2"},
            {"taxable": True, "internalId": "129", "lastModifiedDate": "2024-07-01T00:00:00Z", "entityId": "Cust7", "entityStatus": "Active", "email": "cust7@example.com", "dateCreated": "2023-07-01T00:00:00Z", "subsidiary": "Subsidiary1"},
            {"taxable": False, "internalId": "130", "lastModifiedDate": "2024-08-01T00:00:00Z", "entityId": "Cust8", "entityStatus": "Inactive", "email": "cust8@example.com", "dateCreated": "2023-08-01T00:00:00Z", "subsidiary": "Subsidiary2"},
            {"taxable": True, "internalId": "131", "lastModifiedDate": "2024-09-01T00:00:00Z", "entityId": "Cust9", "entityStatus": "Active", "email": "cust9@example.com", "dateCreated": "2023-09-01T00:00:00Z", "subsidiary": "Subsidiary1"},
            {"taxable": False, "internalId": "132", "lastModifiedDate": "2024-10-01T00:00:00Z", "entityId": "Cust10", "entityStatus": "Inactive", "email": "cust10@example.com", "dateCreated": "2023-10-01T00:00:00Z", "subsidiary": "Subsidiary2"}
        ],
        "SalesOrder": [
            {"lastModifiedDate": "2024-01-01T00:00:00Z", "status": "Pending", "itemList": [{"items_line": "Item1"}, {"items_line": "Item2"}], "internalId": "SO123", "customFieldList": "Custom1", "createdDate": "2023-01-01T00:00:00Z", "customForm": "Form1", "entity": "Cust1", "currency": "USD", "tranDate": "2023-01-01T00:00:00Z", "tranId": "1001", "salesRep": "Rep1", "memo": "Memo1", "exchangeRate": 1.0, "taxItem": "Tax1", "taxRate": 0.05, "billingAddress": "Billing1", "shippingAddress": "Shipping1", "shipDate": "2023-01-05T00:00:00Z", "subTotal": 100.0, "discountTotal": 10.0, "total": 95.0, "balance": 50.0, "subsidiary": "Subsidiary1"},
            {"lastModifiedDate": "2024-02-01T00:00:00Z", "status": "Completed", "itemList": [{"items_line": "Item3"}, {"items_line": "Item4"}], "internalId": "SO124", "customFieldList": "Custom2", "createdDate": "2023-02-01T00:00:00Z", "customForm": "Form2", "entity": "Cust2", "currency": "EUR", "tranDate": "2023-02-01T00:00:00Z", "tranId": "1002", "salesRep": "Rep2", "memo": "Memo2", "exchangeRate": 0.9, "taxItem": "Tax2", "taxRate": 0.1, "billingAddress": "Billing2", "shippingAddress": "Shipping2", "shipDate": "2023-02-05T00:00:00Z", "subTotal": 200.0, "discountTotal": 20.0, "total": 180.0, "balance": 100.0, "subsidiary": "Subsidiary2"},
            {"lastModifiedDate": "2024-03-01T00:00:00Z", "status": "Pending", "itemList": [{"items_line": "Item5"}, {"items_line": "Item6"}], "internalId": "SO125", "customFieldList": "Custom3", "createdDate": "2023-03-01T00:00:00Z", "customForm": "Form3", "entity": "Cust3", "currency": "GBP", "tranDate": "2023-03-01T00:00:00Z", "tranId": "1003", "salesRep": "Rep3", "memo": "Memo3", "exchangeRate": 0.8, "taxItem": "Tax3", "taxRate": 0.15, "billingAddress": "Billing3", "shippingAddress": "Shipping3", "shipDate": "2023-03-05T00:00:00Z", "subTotal": 300.0, "discountTotal": 30.0, "total": 270.0, "balance": 150.0, "subsidiary": "Subsidiary1"},
            {"lastModifiedDate": "2024-04-01T00:00:00Z", "status": "Completed", "itemList": [{"items_line": "Item7"}, {"items_line": "Item8"}], "internalId": "SO126", "customFieldList": "Custom4", "createdDate": "2023-04-01T00:00:00Z", "customForm": "Form4", "entity": "Cust4", "currency": "JPY", "tranDate": "2023-04-01T00:00:00Z", "tranId": "1004", "salesRep": "Rep4", "memo": "Memo4", "exchangeRate": 110.0, "taxItem": "Tax4", "taxRate": 0.05, "billingAddress": "Billing4", "shippingAddress": "Shipping4", "shipDate": "2023-04-05T00:00:00Z", "subTotal": 400.0, "discountTotal": 40.0, "total": 360.0, "balance": 200.0, "subsidiary": "Subsidiary2"},
            {"lastModifiedDate": "2024-05-01T00:00:00Z", "status": "Pending", "itemList": [{"items_line": "Item9"}, {"items_line": "Item10"}], "internalId": "SO127", "customFieldList": "Custom5", "createdDate": "2023-05-01T00:00:00Z", "customForm": "Form5", "entity": "Cust5", "currency": "CAD", "tranDate": "2023-05-01T00:00:00Z", "tranId": "1005", "salesRep": "Rep5", "memo": "Memo5", "exchangeRate": 1.2, "taxItem": "Tax5", "taxRate": 0.1, "billingAddress": "Billing5", "shippingAddress": "Shipping5", "shipDate": "2023-05-05T00:00:00Z", "subTotal": 500.0, "discountTotal": 50.0, "total": 450.0, "balance": 250.0, "subsidiary": "Subsidiary1"},
            {"lastModifiedDate": "2024-06-01T00:00:00Z", "status": "Completed", "itemList": [{"items_line": "Item11"}, {"items_line": "Item12"}], "internalId": "SO128", "customFieldList": "Custom6", "createdDate": "2023-06-01T00:00:00Z", "customForm": "Form6", "entity": "Cust6", "currency": "AUD", "tranDate": "2023-06-01T00:00:00Z", "tranId": "1006", "salesRep": "Rep6", "memo": "Memo6", "exchangeRate": 1.3, "taxItem": "Tax6", "taxRate": 0.05, "billingAddress": "Billing6", "shippingAddress": "Shipping6", "shipDate": "2023-06-05T00:00:00Z", "subTotal": 600.0, "discountTotal": 60.0, "total": 540.0, "balance": 300.0, "subsidiary": "Subsidiary2"},
            {"lastModifiedDate": "2024-07-01T00:00:00Z", "status": "Pending", "itemList": [{"items_line": "Item13"}, {"items_line": "Item14"}], "internalId": "SO129", "customFieldList": "Custom7", "createdDate": "2023-07-01T00:00:00Z", "customForm": "Form7", "entity": "Cust7", "currency": "NZD", "tranDate": "2023-07-01T00:00:00Z", "tranId": "1007", "salesRep": "Rep7", "memo": "Memo7", "exchangeRate": 1.4, "taxItem": "Tax7", "taxRate": 0.05, "billingAddress": "Billing7", "shippingAddress": "Shipping7", "shipDate": "2023-07-05T00:00:00Z", "subTotal": 700.0, "discountTotal": 70.0, "total": 630.0, "balance": 350.0, "subsidiary": "Subsidiary1"},
            {"lastModifiedDate": "2024-08-01T00:00:00Z", "status": "Completed", "itemList": [{"items_line": "Item15"}, {"items_line": "Item16"}], "internalId": "SO130", "customFieldList": "Custom8", "createdDate": "2023-08-01T00:00:00Z", "customForm": "Form8", "entity": "Cust8", "currency": "SGD", "tranDate": "2023-08-01T00:00:00Z", "tranId": "1008", "salesRep": "Rep8", "memo": "Memo8", "exchangeRate": 1.5, "taxItem": "Tax8", "taxRate": 0.05, "billingAddress": "Billing8", "shippingAddress": "Shipping8", "shipDate": "2023-08-05T00:00:00Z", "subTotal": 800.0, "discountTotal": 80.0, "total": 720.0, "balance": 400.0, "subsidiary": "Subsidiary2"},
            {"lastModifiedDate": "2024-09-01T00:00:00Z", "status": "Pending", "itemList": [{"items_line": "Item17"}, {"items_line": "Item18"}], "internalId": "SO131", "customFieldList": "Custom9", "createdDate": "2023-09-01T00:00:00Z", "customForm": "Form9", "entity": "Cust9", "currency": "INR", "tranDate": "2023-09-01T00:00:00Z", "tranId": "1009", "salesRep": "Rep9", "memo": "Memo9", "exchangeRate": 74.0, "taxItem": "Tax9", "taxRate": 0.05, "billingAddress": "Billing9", "shippingAddress": "Shipping9", "shipDate": "2023-09-05T00:00:00Z", "subTotal": 900.0, "discountTotal": 90.0, "total": 810.0, "balance": 450.0, "subsidiary": "Subsidiary1"},
            {"lastModifiedDate": "2024-10-01T00:00:00Z", "status": "Completed", "itemList": [{"items_line": "Item19"}, {"items_line": "Item20"}], "internalId": "SO132", "customFieldList": "Custom10", "createdDate": "2023-10-01T00:00:00Z", "customForm": "Form10", "entity": "Cust10", "currency": "CNY", "tranDate": "2023-10-01T00:00:00Z", "tranId": "1010", "salesRep": "Rep10", "memo": "Memo10", "exchangeRate": 6.5, "taxItem": "Tax10", "taxRate": 0.05, "billingAddress": "Billing10", "shippingAddress": "Shipping10", "shipDate": "2023-10-05T00:00:00Z", "subTotal": 1000.0, "discountTotal": 100.0, "total": 900.0, "balance": 500.0, "subsidiary": "Subsidiary2"}
        ]
    }

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
    

def get_selected_streams(catalog, state):
    selected_stream_ids = state.get('selected_streams', [])  # Assumes state contains a list of selected stream IDs
    return [stream for stream in catalog.streams if stream.tap_stream_id in selected_stream_ids]

def sync(config, state, catalog):
    mock_data = mock_get_netsuite_data()
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream: " + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True 

        schema_message = singer.SchemaMessage(
            stream=stream.tap_stream_id,
            schema=stream.schema.to_dict(),
            key_properties=stream.key_properties
        )
        singer.write_message(schema_message)

        tap_data = mock_data.get(stream.tap_stream_id, [])

        transformed_data = transform_data(tap_data, stream.tap_stream_id)
        
        max_bookmark = None
        for row in transformed_data:
            record_message = singer.RecordMessage(
                stream=stream.tap_stream_id,
                record=row,
                version=None,
                time_extracted=None
            )
            singer.write_message(record_message)

            if bookmark_column:
                if is_sorted:
                    state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, row[bookmark_column])
                    singer.write_state(state)
                else:
                    max_bookmark = max(max_bookmark, row[bookmark_column]) if max_bookmark else row[bookmark_column]
        if bookmark_column and not is_sorted:
            state = singer.write_bookmark(state, stream.tap_stream_id, bookmark_column, max_bookmark)
            singer.write_state(state)
    return


def load_to_csv(data, filename):
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)

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
    LOGGER.error(f"Exceeded maximum retries ({max_retries}) to connect to Postgres.")


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
            catalog = Catalog.from_dict(json.load(input_file))
    
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
            
            mock_data = mock_get_netsuite_data()
            transformed_data = {stream: transform_data(data, stream) for stream, data in mock_data.items()}
            
            for stream, data in transformed_data.items():
                load_to_csv(data, f"{stream}.csv")
                load_to_google_sheets(data, config['spreadsheet_id'], f'{stream}!A1', config['credentials_file'])
                load_to_bigquery(data, config['bigquery_dataset_id'], stream, config['credentials_file'])
            
            sync(config, state, catalog)
    except Exception as e:
        LOGGER.error(f"Error occurred: {e}", exc_info=True)


class Catalog(SingerCatalog):
    def get_selected_streams(self, state):
        selected_stream_ids = state.get('selected_streams', []) 
        return [stream for stream in self.streams if stream.tap_stream_id in selected_stream_ids]

if __name__ == "__main__":
    main()
