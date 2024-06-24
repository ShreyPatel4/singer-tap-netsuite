from datetime import datetime

def transform_data(data, stream_name):
    transformed_data = []
    for record in data:
        if stream_name == 'Customer':
            # Example transformation for Customer stream
            if 'lastModifiedDate' in record:
                record['lastModifiedDate'] = datetime.strptime(record['lastModifiedDate'], "%Y-%m-%dT%H:%M:%SZ")
            if 'dateCreated' in record:
                record['dateCreated'] = datetime.strptime(record['dateCreated'], "%Y-%m-%dT%H:%M:%SZ")
            if 'email' in record:
                record['email'] = record['email'].lower()
        
        elif stream_name == 'SalesOrder':
            # Example transformation for SalesOrder stream
            if 'lastModifiedDate' in record:
                record['lastModifiedDate'] = datetime.strptime(record['lastModifiedDate'], "%Y-%m-%dT%H:%M:%SZ")
            if 'createdDate' in record:
                record['createdDate'] = datetime.strptime(record['createdDate'], "%Y-%m-%dT%H:%M:%SZ")
            if 'tranDate' in record:
                record['tranDate'] = datetime.strptime(record['tranDate'], "%Y-%m-%dT%H:%M:%SZ")
            if 'shipDate' in record:
                record['shipDate'] = datetime.strptime(record['shipDate'], "%Y-%m-%dT%H:%M:%SZ")
            if 'subTotal' in record and 'taxRate' in record:
                record['totalTax'] = record['subTotal'] * record['taxRate']
        
        transformed_data.append(record)
    return transformed_data
