import io
from unittest.mock import patch
from pytest_mock import mocker
from datetime import datetime
import json
import pytest
from flask_api_csv_file_upload import app


@pytest.fixture
def client():
    with app.test_client() as client:
        yield client


def test_missing_schema_field(client):
    """
    This function checks if schema is missing in the request
    """
    data = {
        'create_usr_id': 'ashish',
        'files': (io.BytesIO(b'mock_data'), 'mock.csv')
    }
    response = client.post('/api/file-import', data=data)
    assert response.status_code == 400
    assert b'Missing schema field in the request' in response.data


def test_missing_files_field(client):
    """
    This function checks if files is missing in the request
    """
    data = {
        'create_usr_id': 'ashish',
        'schema': 'public'
    }
    response = client.post('/api/file-import', data=data)
    assert response.status_code == 400
    assert b'file_path is missing int the request' in response.data


def test_invalid_file_type(client):
    """
    This function checks filetype, if not csv then give error
    """
    data = {
        'create_usr_id': 'ashish',
        'schema': 'public',
        'files': (io.BytesIO(b'mock_data'), 'mock.txt')
    }
    response = client.post('/api/file-import', data=data)
    assert response.status_code == 400
    assert b'Invalid file type, only CSV files are allowed' in response.data


def test_error_reading_csv_file(client):
    """
    This function checks if there is any error while reading csv file
    """
    data = {
        'create_usr_id': 'ashish',
        'schema': 'public',
        'files': (io.BytesIO(b'mock_data'), 'mock.csv')
    }
    with patch('pandas.read_csv', side_effect=Exception('Error reading CSV file')):
        response = client.post('/api/file-import', data=data)
    assert response.status_code == 400
    assert b'Error reading CSV file' in response.data


def test_successful_import(client, mocker):
    """
    This function returns correct response when all the fields are provided
    """
    data = {
        'create_usr_id': 'ashish',
        'schema': 'public',
        'files': (io.BytesIO(b'mock_data'), 'mock.csv')
    }
    mock_db_connection = mocker.patch('flask_api_csv_file_upload.db_connection')
    response = client.post('/api/file-import', data=data)
    assert response.status_code == 200
    file_name = data['files'][1]
    file_name_without_ext = file_name.split('.')[0]
    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    table_name = f'{data["schema"]}.{file_name_without_ext}_{timestamp}'
    expected_response = {"message": f'{table_name} created'}
    assert json.loads(response.data) == expected_response
    mock_db_connection.assert_called_once()


