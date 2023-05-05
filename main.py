import os
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)
data_file = 'partners_data.csv'


@app.route('/')
def home():
    return jsonify({"message": "Welcome to the Partners Data API!"})


@app.route('/api/upload', methods=['POST'])
def upload():
    file = request.files['file']
    if file:
        df = pd.read_csv(file)
        df.drop_duplicates(subset='transaction_id', inplace=True)
        df.to_csv(data_file, index=False)
        return jsonify({"message": "Data uploaded successfully."}), 200
    return jsonify({"message": "Invalid file."}), 400


def read_data():
    df = pd.read_csv(data_file)
    df.drop_duplicates(subset='transaction_id', inplace=True)
    return df


@app.route('/api/transaction-volumes', methods=['GET'])
def transaction_volumes():
    if os.path.exists(data_file):
        df = read_data()
        df_volume = df.groupby(['year', 'month']).size().reset_index(name='volume')
        return df_volume.to_json(orient='records')
    return jsonify({"message": "Data not found."}), 404


@app.route('/api/total-sales', methods=['GET'])
def total_sales():
    if os.path.exists(data_file):
        df = read_data()
        df_sales = df.groupby(['year', 'month'])['sale_value'].sum().reset_index(name='total_sales')
        return df_sales.to_json(orient='records')
    return jsonify({"message": "Data not found."}), 404


@app.route('/api/partners-table', methods=['GET'])
def partners_table():
    if os.path.exists(data_file):
        df = read_data()
        partners_table_data = df[['transaction_id', 'partner_name', 'sale_value', 'country', 'region']]
        return partners_table_data.to_json(orient='records')
    return jsonify({"message": "Data not found."}), 404


@app.route('/api/sales-per-region', methods=['GET'])
def sales_per_region():
    if os.path.exists(data_file):
        df = read_data()
        sales_region_data = df.groupby('region')['sale_value'].sum().reset_index(name='total_sales')
        return sales_region_data.to_json(orient='records')
    return jsonify({"message": "Data not found."}), 404


@app.route('/api/volume-per-region', methods=['GET'])
def volume_per_region():
    if os.path.exists(data_file):
        df = read_data()
        volume_region_data = df.groupby('region').size().reset_index(name='transaction_volume')
        return volume_region_data.to_json(orient='records')
    return jsonify({"message": "Data not found."}), 404


if __name__ == '__main__':
    app.run(debug=True)

