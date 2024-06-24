import unittest
from unittest.mock import patch
from datetime import datetime
import etl_script 
class TestTransformData(unittest.TestCase):

    @patch('etl_script.fetch_customer_data')
    def test_transform_customer_data(self, mock_fetch):
        mock_fetch.return_value = [
            {"taxable": True, "internalId": "123", "lastModifiedDate": "2024-01-01T00:00:00Z", "entityId": "Cust1", "entityStatus": "Active", "email": "CUST1@EXAMPLE.COM", "dateCreated": "2023-01-01T00:00:00Z", "subsidiary": "Subsidiary1"}
        ]
        expected_data = [
            {"taxable": True, "internalId": "123", "lastModifiedDate": datetime(2024, 1, 1, 0, 0), "entityId": "Cust1", "entityStatus": "Active", "email": "cust1@example.com", "dateCreated": datetime(2023, 1, 1, 0, 0), "subsidiary": "Subsidiary1"}
        ]
        raw_data = mock_fetch()
        transformed_data = etl_script.transform_data(raw_data, 'Customer')
        self.assertEqual(transformed_data, expected_data)
    @patch('etl_script.fetch_sales_order_data')
    def test_transform_sales_order_data(self, mock_fetch):
        mock_fetch.return_value = [
            {"lastModifiedDate": "2024-01-01T00:00:00Z", "status": "Pending", "internalId": "SO123", "createdDate": "2023-01-01T00:00:00Z", "tranDate": "2023-01-01T00:00:00Z", "shipDate": "2023-01-05T00:00:00Z", "subTotal": 100.0, "taxRate": 0.05}
        ]
        expected_data = [
            {"lastModifiedDate": datetime(2024, 1, 1, 0, 0), "status": "Pending", "internalId": "SO123", "createdDate": datetime(2023, 1, 1, 0, 0), "tranDate": datetime(2023, 1, 1, 0, 0), "shipDate": datetime(2023, 1, 5, 0, 0), "subTotal": 100.0, "taxRate": 0.05, "totalTax": 5.0}
        ]
        raw_data = mock_fetch()
        transformed_data = etl_script.transform_data(raw_data, 'SalesOrder')
        self.assertEqual(transformed_data, expected_data)

if __name__ == '__main__':
    unittest.main()
