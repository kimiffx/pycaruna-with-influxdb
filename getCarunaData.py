import sys
import json
from influxdb import InfluxDBClient
import pycaruna


def send_to_influxdb(data_json):
    # Initialize InfluxDB client (legacy version)
    client = InfluxDBClient('localhost', '8086', 'grafana', 'grafana', 'home')

    # Parse the JSON data
    data = json.loads(data_json)

    # Prepare data in InfluxDB's legacy JSON format
    influx_data = []
    for record in data:
        measurement = record.get("measurement", "electricity")
        
        # Ensure data types are consistent
        def safe_float(value):
            try:
                return float(value)
            except (ValueError, TypeError):
                return 0.0

        def safe_string(value):
            return str(value) if value is not None else "unknown"

        point = {
            "measurement": measurement,
            "tags": {
                "status_totalConsumption": safe_string(record["statuses"].get("totalConsumption")),
                "status_invoicedConsumption": safe_string(record["statuses"].get("invoicedConsumption")),
            },
            "fields": {
                "totalConsumption": safe_float(record.get("totalConsumption")),
                "invoicedConsumption": safe_float(record.get("invoicedConsumption")),
                "totalFee": safe_float(record.get("totalFee")),
                "distributionFee": safe_float(record.get("distributionFee")),
                "distributionBaseFee": safe_float(record.get("distributionBaseFee")),
                "electricityTax": safe_float(record.get("electricityTax")),
                "valueAddedTax": safe_float(record.get("valueAddedTax")),
                "temperature": safe_float(record.get("temperature")),
            },
            "time": record["timestamp"],
        }
        influx_data.append(point)

    # Write data to InfluxDB
    try:
        client.write_points(influx_data)
    except Exception as e:
        print(f"Failed to write to InfluxDB: {e}")
    finally:
        # Close the client
        client.close()


if __name__ == "__main__":
    # Get input parameters
    username = sys.argv[1]
    password = sys.argv[2]
    ID = sys.argv[3]
    year = sys.argv[4]
    month = sys.argv[5]
    day = sys.argv[6]

    # Fetch consumption data from pycaruna
    try:
        (session, info) = pycaruna.login_caruna(username, password)
        customer = info['user']['ownCustomerNumbers'][0]
        token = info['token']
        metering_points = pycaruna.get_metering_points(session, token, ID)
        consumption = pycaruna.get_cons_hours(session, token, customer, metering_points[0][0], year, month, day)
        response = pycaruna.logout_caruna(session)

        # Convert consumption data to JSON and send to InfluxDB
        data_json = json.dumps(consumption)
        send_to_influxdb(data_json)

    except Exception as e:
        print(f"Error: {e}")

