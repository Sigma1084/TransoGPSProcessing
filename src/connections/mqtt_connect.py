import paho.mqtt.client as mqtt
import os
from dotenv import load_dotenv
load_dotenv()

client = mqtt.Client(client_id="GPS Cleaner")


def on_connect(mqttc, obj, flags, rc):
    print("MQTT Connect Status: " + str(rc))
    if int(rc) == 0:
        print("Connection Success")


def on_message(mqttc, obj, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))


def on_publish(mqttc, obj, mid):
    print(f"Publishing {obj} {mid}")


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print(string)


def on_socket_open(mqttc, userdata, sock):
    print("MQTT Socket Opened")


def on_socket_close(mqttc, userdata, sock):
    print("MQTT Socket Closed")


MQTT_CONNECTION_DETAILS = {
    'host': os.getenv("MQTT_HOST"),
    'port': os.getenv("MQTT_PORT"),
}

# client.connect(**MQTT_CONNECTION_DETAILS)
# client.publish('h', '{}', qos=1)

__all__ = ['client', 'MQTT_CONNECTION_DETAILS']
