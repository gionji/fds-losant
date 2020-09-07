import queue
import threading
import time

import paho.mqtt.client as mqtt


class MqttLocalClient(threading.Thread):

    def __init__(self, client_id=None, host='localhost', port=1883, subscription_paths=None):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.client_id = client_id
        self.subscription_paths = subscription_paths
        self.message_queue = queue.Queue()
        self.client = mqtt.Client(client_id=self.client_id)
        self.callback = None

    def publish(self, topic, payload, ):
        print('[MQTT_CLIENT] publish to ' + topic + ' payload: ' + payload)
        self.client.publish(topic, payload)

    def publish_on_many_topics(self, topics, payload):
        for topic in topics:
            print('[MQTT_CLIENT] publish to ' + topic + ' payload: ' + payload)
            self.client.publish(topic, payload)

    def run(self):
        print('[MQTT_CLIENT] connecting to mqtt -> ' + self.host + ':' + str(self.port))
        self.client.on_message = self.on_message
        self.client.connect(self.host, self.port, 60)
        self.subscribe_all(self.subscription_paths)
        self.client.loop_start()

    def on_message(self, client, obj, msg):
        if msg is not None:
            if self.callback is None:
                self.message_queue.put(msg)
            else:
                self.callback(msg)

    def subscribe_all(self, subscription_paths=None, qos=1):
        if subscription_paths is None:
            subscription_paths = []
        for path in subscription_paths:
            print('[MQTT_CLIENT] subscribe to ' + path)
            self.client.subscribe(path, qos=qos)
            time.sleep(1)

    def set_callback(self, callback):
        self.callback = callback

    def stop(self):
        self.client.loop_stop()

