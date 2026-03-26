from flask import Flask, render_template_string
from flask_socketio import SocketIO, emit
import paho.mqtt.client as mqtt
import json
import os
import threading

app = Flask(__name__)

# Force standard threading mode. This guarantees Paho MQTT's native threads 
# can communicate natively with Flask-SocketIO without any async deadlocks.
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# MQTT settings
BROKER = '127.0.0.1'
PORT = 1883

try:
    mqtt_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1)
except AttributeError:
    mqtt_client = mqtt.Client()

def on_mqtt_connect(client, userdata, flags, rc):
    if rc == 0:
        print("\n✅ Server MQTT connected! Subscribing to topics...")
        client.subscribe("machine/#")
    else:
        print(f"Server MQTT connect failed: {rc}")

def on_mqtt_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        data['subscribed_topic'] = msg.topic
        # Direct emit is perfectly safe here because async_mode="threading"
        socketio.emit('mqtt_data', data)
        mid = (data.get('asrsId') or data.get('machineId') or
               data.get('robotId') or data.get('inspectionId') or '?')
        print(f"📡 [SocketIO] Emitted: {mid} | {msg.topic}")
    except Exception as e:
        print(f"❌ Error parsing MQTT message: {e}")

mqtt_client.on_connect = on_mqtt_connect
mqtt_client.on_message = on_mqtt_message

def start_mqtt():
    mqtt_client.connect(BROKER, PORT, 60)
    mqtt_client.loop_start()

@app.route('/')
def index():
    dashboard_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'dashboard .html')
    with open(dashboard_path, 'r') as f:
        html = f.read()
    return render_template_string(html)

def run_server(host='0.0.0.0', port=5001):
    start_mqtt()
    try:
        socketio.run(app, host=host, port=port, allow_unsafe_werkzeug=True)
    except OSError as e:
        print(f"\n[ERROR] Could not bind to port {port}: {e}")
        print(f"Port {port} is likely in use by a previous instance. Please terminate it and try again.\n")
        import sys
        sys.exit(1)

if __name__ == '__main__':
    run_server()