import json
import os
import sys
from time import sleep

from dotenv import load_dotenv
load_dotenv()

from app.telemetry import agent
from app.utils import IIoT, connector

MQTT_HOSTNAME = os.getenv('MQTT_HOSTNAME', 'localhost')
MQTT_PORT = os.getenv('MQTT_PORT', '1883')
MQTT_CLIENT_ID = os.getenv('MQTT_CLIENT_ID', 'LOSANT')

MY_DEVICE_ID = os.getenv('MY_DEVICE_ID', '')
MY_APP_ACCESS_KEY = os.getenv('MY_APP_ACCESS_KEY', '')
MY_APP_ACCESS_SECRET = os.getenv('MY_APP_ACCESS_SECRET', '')


def command_from_agent_callback(command):
    # her i put the command to publish the data to the CONTROL mqtt channel

    print(command)


def run(config):
    topics = ['{}/#'.format(IIoT.MqttChannels.sensors)]
    mqtt_client = connector.MqttLocalClient(MQTT_CLIENT_ID, MQTT_HOSTNAME, int(MQTT_PORT), topics)
    mqtt_client.start()

    losant_client = agent.LosantAgent(
        my_device_id=MY_DEVICE_ID,
        my_app_access_key=MY_APP_ACCESS_KEY,
        my_app_access_secret=MY_APP_ACCESS_SECRET,
    )
    losant_client.set_callback(command_from_agent_callback)
    losant_client.name = 'Losant Thread'
    losant_client.start()
    timestamp_data = 0
    data  = {}

    while True:
        ## Waiting for messages from the subscribed mqtt channels
        message = mqtt_client.message_queue.get()
        try:
            _, mqtt_channel, module, sensor = message.topic.split('/')
        except Exception as e:
            print(e)
            return None

        json_payload = json.loads(message.payload)
        value = json_payload['value']
        timestamp = json_payload['timestamp']

        if timestamp == timestamp_data or timestamp_data == 0:
            print('add -  {} {}'.format(timestamp, sensor))
            data[sensor] = value
            timestamp_data = timestamp
            sleep(0.05)
        else:
            data = {}
            print('send')
            losant_client.send_state(data)
            data = {}
            timestamp_data = timestamp
            sleep(1)


if __name__ == '__main__':
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None

    run(config_file)
