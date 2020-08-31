import json
from datetime import datetime
from time import sleep

from libs.utils import connector, IIoT

from telemetry import agent


MY_DEVICE_ID         = ''
MY_APP_ACCESS_KEY    = ''
MY_APP_ACCESS_SECRET = ''

MY_DEVICE_ID = os.getenv('MY_DEVICE_ID', '')
MY_APP_ACCESS_KEY = os.getenv('MY_APP_ACCESS_KEY', '')
MY_APP_ACCESS_SECRET = os.getenv('MY_APP_ACCESS_SECRET', '')


class GenericValue:
    def __init__(self, key, value, timestamp):
        self.key       = key
        self.value     = value
        self.timestamp = timestamp

    def format(self):
        return json.dumps({
            "key"      : self.key,
            "value"    : self.value,
            "timestamp": self.timestamp
        })


def command_from_agent_callback(self, command):
    # her i put the command to publish the data to the CONTROL mqtt channel

    print(command)


if __name__ == "__main__":
    # Subscribed channels
    topics = [IIoT.MqttChannels.sensors]

    # Connect to MQTT clients
    mqtt_client = connector.MqttLocalClient('TELEMETRY-LOSANT', 'localhost', 1883, topics)
    mqtt_client.start()


    ## Init the module core object
    losant_client = agent.LosantAgent(
                                  my_device_id         = MY_DEVICE_ID,
                                  my_app_access_key    = MY_APP_ACCESS_KEY,
                                  my_app_access_secret = MY_APP_ACCESS_SECRET,
                                )
    losant_client.set_callback( command_from_agent_callback )
    losant_client.name = 'Losant Thread'
    losant_client.start()


    while True:
        ## Waiting for messages from the subscribed mqtt channels
        message = mqtt_client.message_queue.get()
        print(message.payload)

        json_payload = json.loads(message.payload)
        timestamp    = json_payload['timestamp']
        key          = json_payload['device'] + "-" +json_payload['sensor']
        value        = json_payload['value']

        losant_client.send_state(key, value)

        sleep(1)
