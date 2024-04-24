import random
import json
from paho.mqtt import client as mqtt_client
from decimal import Decimal
import psycopg2
import threading
from datetime import datetime, timezone

def get_serial_number():
    try:
        with open('/proc/cpuinfo', 'r') as f:
            for line in f:
                if line.startswith('Serial'):
                    serial_number = line.split(':')[-1].strip()
                    return serial_number
    except FileNotFoundError:
        return "Error: /proc/cpuinfo not found"

serial_number = get_serial_number()
print("Serial Number:", serial_number)

broker = '146.190.195.87'
port = 1883
topic = f"emqx/{serial_number}"
# generate client ID with pub prefix randomly
client_id = f'python-mqtt-{random.randint(0, 1000)}'
username = 'admin'
password = 'admin'

dbpassword = "vigorvigor123"
dbusername = "tsdbadmin"
dbhost = "lzvccok4h7.nsp25iwq8b.tsdb.cloud.timescale.com"
dbport = 33417
dbname = "tsdb"

def create_table(device_code):
    try:
        table_name = f"invt_{device_code}"  # Use device code as part of table name
        CONNECTION = f"postgres://{dbusername}:{dbpassword}@{dbhost}:{dbport}/{dbname}"
        with psycopg2.connect(CONNECTION) as conn:
            cursor = conn.cursor()

            query_create_invtdata_table = f"""CREATE TABLE IF NOT EXISTS {table_name} (
                                                   device_code TEXT,
                                                   p_timestamp TEXT,
                                                   timest TIMESTAMPTZ NOT NULL,
                                                   it NUMERIC(4,1),
                                                   pv1v NUMERIC(4,1),
                                                   pv1a INTEGER,
                                                   dpy NUMERIC(6,1),
                                                   tpg NUMERIC(6,1),
                                                   dailyload NUMERIC(3,1),
                                                   tlc INTEGER,
                                                   trp NUMERIC(6,1),
                                                   ap NUMERIC(6,1),
                                                   cap INTEGER,
                                                   alarm TEXT,
                                                   inverterstate TEXT
                                               );
                                           """
            query_create_invtdata_hypertable = f"SELECT create_hypertable('{table_name}', by_range('timest'), if_not_exists => TRUE);"

            cursor.execute(query_create_invtdata_table)
            cursor.execute(query_create_invtdata_hypertable)

            current_timestamp = datetime.now(timezone.utc)
            conn.commit()
            cursor.close()

            print("Table created successfully:", table_name)
            return table_name

    except (Exception, psycopg2.Error) as error:
        print("Error while creating table:", error)
        return None
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to a topic or perform other actions here
    else:
        print(f"Failed to connect, return code {rc}")

def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        incoming_data = json.loads(msg.payload.decode())
        print("Received MQTT message:", incoming_data)  # Debugging statement

        if incoming_data:
            invt_data = incoming_data[0].get("INVT")
            if invt_data is not None:
                device_code = invt_data.get("device_code")
                if device_code is not None:
                    table_name = create_table(device_code)

                    mapped_data = []
                    for entry in incoming_data:
                        mapped_entry = {
                            "timest": datetime.now(timezone.utc),
                            "device_code": str(invt_data.get("device_code")),
                            "p_timestamp": str(invt_data.get("timestamp_ms")),
                            "it": Decimal(invt_data.get("InternalTemp_c", 0)),
                            "pv1v": Decimal(invt_data.get("pv1_input_voltage_V", 0)),
                            "pv1a": Decimal(invt_data.get("pv1_input_current_A", 0)),
                            "dpy": Decimal(invt_data.get("daily_generation_kWh", 0)),
                            "tpg": Decimal(invt_data.get("total_power_generation_kWh", 0)),
                            "dailyload": Decimal(invt_data.get("Daily_load_kWh", 0)),
                            "tlc": Decimal(invt_data.get("total_load_consumption_kWh", 0)),
                            "trp": Decimal(invt_data.get("TotalReactivePower_Var", 0)),
                            "ap": Decimal(invt_data.get("ActivePower_w", 0)),
                            "cap": Decimal(invt_data.get("Cap", 0)),
                            "alarm": invt_data.get("Alarm", 0),
                            "inverterstate": invt_data.get("InverterState", 0),
                        }
                        mapped_data.append(mapped_entry)

                    print(mapped_data, table_name)
                    insert_to_postgres(mapped_data, table_name)
                else:
                    print("device_code not found in the invt data.")
            else:
                print("invt data not found in the MQTT message.")
        else:
            print("Empty MQTT message received.")

    client.subscribe(topic)
    client.on_message = on_message

def insert_to_postgres(mapped_data, table_name):
    try:
        CONNECTION = f"postgres://{dbusername}:{dbpassword}@{dbhost}:{dbport}/{dbname}"
        with psycopg2.connect(CONNECTION) as conn:
            cursor = conn.cursor()

            insert_query = """INSERT INTO {} (
                                            device_code, p_timestamp, timest, it, pv1v, pv1a, dpy, tpg, dailyload, tlc, trp, ap,
                                            cap, alarm, inverterstate
                                          ) VALUES (
                                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                          )""".format(table_name)

            for entry in mapped_data:
                data_values = (
                    entry['device_code'],
                    entry['p_timestamp'],
                    entry['timest'],
                    entry['it'],
                    entry['pv1v'],
                    entry['pv1a'],
                    entry['dpy'],
                    entry['tpg'],
                    entry['dailyload'],
                    entry['tlc'],
                    entry['trp'],
                    entry['ap'],
                    entry['cap'],
                    entry['alarm'],
                    entry['inverterstate']
                )
                cursor.execute(insert_query, data_values)

            conn.commit()

            print("Data inserted successfully into PostgreSQL")

    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL:", error)

def connect_mqtt():
    client = mqtt_client.Client(client_id)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client

def mqtt_thread(client):
    try:
        client.loop_forever(timeout=1.0)
    except KeyboardInterrupt:
        print("Disconnected from MQTT Broker.")
        client.disconnect()

if __name__ == "__main__":
    mqtt_client = connect_mqtt()
    mqtt_thread = threading.Thread(target=mqtt_thread, args=(mqtt_client,))
    mqtt_thread.start()

    try:
        subscribe(mqtt_client)
        while True:
            # Keep the script running
            pass
    except KeyboardInterrupt:
        mqtt_client.disconnect()  # Disconnect on keyboard interrupt
        print("Disconnected from MQTT Broker.")
