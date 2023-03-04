from datetime import datetime
from flask import Flask, request, jsonify, make_response
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

app = Flask(__name__)


@app.route('/api/file-import', methods=['POST'])
def upload_file_function():
    """
    This function accepts files, schema and create_usr_id. It reads csv file received from files and store it
    into new table created based on latest timestamp
    """
    if 'files' not in request.files:
        return make_response(jsonify({'error': 'file_path is missing int the request'}), 400)
    create_usr_id = request.form.get('create_usr_id')
    schema = request.form.get('schema')
    if not schema:
        return make_response(jsonify({'error': 'Missing schema field in the request'}), 400)

    file_path = request.files['files']
    print(file_path)
    file_name = file_path.filename
    file_name_without_ext = file_name.split('.')[0]
    if not file_name.endswith('.csv'):
        return make_response(jsonify({'error': 'Invalid file type, only CSV files are allowed'}), 400)

    timestamp = datetime.now().strftime('%Y_%m_%d_%H_%M_%S')
    table_name = f'{schema}.{file_name_without_ext}_{timestamp}'
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        return make_response(jsonify({'error': f'Error reading CSV file: {str(e)}'}), 400)

    db_connection(df, table_name)

    return make_response(jsonify({"message": f'{table_name} created'}), 200)


def db_connection(df, table_name):
    """
    This function creates db connection and save dataframes into the table
    """
    conn = psycopg2.connect(
        dbname='venzo_assignment_db',
        user='postgres',
        password='welcome123',
        host='localhost',
        port='5433'
    )

    engine = create_engine('postgresql+psycopg2://postgres:welcome123@localhost:5433/venzo_assignment_db')
    df.to_sql(name=table_name, con=engine, if_exists='fail', index=False)

    conn.close()


if __name__ == '__main__':
    app.run(debug=True)