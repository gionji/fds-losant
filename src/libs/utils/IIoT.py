class MqttChannels:
    persist = "/persist"
    sensors = "/sensors"
    data = "/data"
    telemetry = "/telemetry"
    gui = "/gui"
    actuators = "/actuators"


MQTT_PORT = 1883
MQTT_HOST = '127.0.0.1'


def packOutputMessage(output_name, output_value):
    message = {
        "data": {
            "name": output_name,
            "value": output_value
        }
    }
    return message
