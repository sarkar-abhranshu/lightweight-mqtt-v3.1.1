import socket
import struct
import time
import threading
import random
import json
import argparse
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MQTT_PUBLISHER')

# MQTT Packet Types
CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
DISCONNECT = 14

# QoS Levels
QOS_0 = 0  # At most once
QOS_1 = 1  # At least once

class MQTTPublisher:
    def __init__(self, broker_host, broker_port=1883, client_id=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"publisher-{random.randint(1000, 9999)}"
        self.socket = None
        self.connected = False
        self.message_id = 0
        self.keep_alive = 60  # seconds
        self.unacknowledged_messages = {}  # message_id -> (topic, payload, qos)
        self.running = False
        self.lock = threading.Lock()
    
    def connect(self):
        """Connect to the MQTT broker"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.broker_host, self.broker_port))
            
            # Send CONNECT packet
            self._send_connect()
            
            # Wait for CONNACK
            packet_type, payload = self._read_packet()
            if packet_type == CONNACK:
                return_code = payload[1]
                if return_code == 0:
                    logger.info(f"Connected to broker at {self.broker_host}:{self.broker_port}")
                    self.connected = True
                    
                    # Start receive thread
                    self.running = True
                    self.receive_thread = threading.Thread(target=self._receive_loop)
                    self.receive_thread.daemon = True
                    self.receive_thread.start()
                    
                    return True
                else:
                    logger.error(f"Connection refused, return code: {return_code}")
            else:
                logger.error(f"Unexpected response: {packet_type}")
        
        except Exception as e:
            logger.error(f"Error connecting to broker: {e}")
        
        return False
    
    def disconnect(self):
        """Disconnect from the MQTT broker"""
        if self.connected:
            try:
                # Send DISCONNECT packet
                self._send_disconnect()
                
                # Close socket
                self.socket.close()
                
                # Stop threads
                self.running = False
                if hasattr(self, 'receive_thread') and self.receive_thread:
                    self.receive_thread.join(1.0)
                
                logger.info("Disconnected from broker")
            
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
            
            finally:
                self.connected = False
    
    def publish(self, topic, payload, qos=0):
        """Publish a message to a topic"""
        if not self.connected:
            logger.error("Not connected to broker")
            return False
        
        try:
            # Increment message ID for QoS > 0
            if qos > 0:
                with self.lock:
                    self.message_id = (self.message_id + 1) % 65536
                    message_id = self.message_id
                    self.unacknowledged_messages[message_id] = (topic, payload, qos)
            else:
                message_id = None
            
            # Send PUBLISH packet
            self._send_publish(topic, payload, qos, message_id)
            
            logger.info(f"Published message to {topic} with QoS {qos}")
            return True
        
        except Exception as e:
            logger.error(f"Error publishing message: {e}")
            return False
    
    def _send_connect(self):
        """Send CONNECT packet"""
        # Protocol name and version
        protocol_name = "MQTT"
        protocol_version = 4  # MQTT 3.1.1
        
        # Connect flags
        # bit 0: reserved
        # bit 1: clean session (1 = clean session)
        # bits 2-7: unused in this implementation
        connect_flags = 0x02  # Clean session
        
        # Keep alive (in seconds)
        keep_alive = self.keep_alive
        
        # Client ID
        client_id = self.client_id.encode('utf-8')
        client_id_len = len(client_id)
        
        # Variable header
        variable_header = struct.pack("!H", len(protocol_name)) + protocol_name.encode('utf-8')
        variable_header += bytes([protocol_version, connect_flags])
        variable_header += struct.pack("!H", keep_alive)
        
        # Payload
        payload = struct.pack("!H", client_id_len) + client_id
        
        # Calculate remaining length
        remaining_length = len(variable_header) + len(payload)
        
        # Encode remaining length
        encoded_length = bytearray()
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            if remaining_length > 0:
                byte |= 0x80
            encoded_length.append(byte)
            if remaining_length == 0:
                break
        
        # Fixed header
        fixed_header = bytes([CONNECT << 4]) + bytes(encoded_length)
        
        # Send packet
        self.socket.send(fixed_header + variable_header + payload)
    
    def _send_publish(self, topic, payload, qos, message_id=None):
        """Send PUBLISH packet"""
        if isinstance(payload, dict) or isinstance(payload, list):
            payload = json.dumps(payload).encode('utf-8')
        elif not isinstance(payload, bytes):
            payload = str(payload).encode('utf-8')
        
        # Calculate flags
        flags = qos << 1
        
        # Encode topic
        encoded_topic = topic.encode('utf-8')
        topic_len = len(encoded_topic)
        variable_header = struct.pack("!H", topic_len) + encoded_topic
        
        # Add message ID for QoS > 0
        if qos > 0 and message_id is not None:
            variable_header += struct.pack("!H", message_id)
        
        # Calculate remaining length
        remaining_length = len(variable_header) + len(payload)
        
        # Encode remaining length
        encoded_length = bytearray()
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            if remaining_length > 0:
                byte |= 0x80
            encoded_length.append(byte)
            if remaining_length == 0:
                break
        
        # Fixed header
        fixed_header = bytes([PUBLISH << 4 | flags]) + bytes(encoded_length)
        
        # Send packet
        self.socket.send(fixed_header + variable_header + payload)
    
    def _send_puback(self, message_id):
        """Send PUBACK packet"""
        # Fixed header
        fixed_header = bytes([PUBACK << 4, 2])  # 2 bytes in variable header
        
        # Variable header (message ID)
        variable_header = struct.pack("!H", message_id)
        
        # Send packet
        self.socket.send(fixed_header + variable_header)
    
    def _send_disconnect(self):
        """Send DISCONNECT packet"""
        # Fixed header (no variable header or payload)
        fixed_header = bytes([DISCONNECT << 4, 0])
        
        # Send packet
        self.socket.send(fixed_header)
    
    def _read_packet(self):
        """Read an MQTT packet from the socket"""
        # Read fixed header
        first_byte = self.socket.recv(1)
        if not first_byte:
            return None, None
        
        packet_type = (first_byte[0] & 0xF0) >> 4
        flags = first_byte[0] & 0x0F
        
        # Read remaining length using variable length encoding
        multiplier = 1
        remaining_length = 0
        while True:
            byte = self.socket.recv(1)[0]
            remaining_length += (byte & 127) * multiplier
            multiplier *= 128
            if not (byte & 128):
                break
        
        # Read the payload based on remaining length
        payload = b''
        if remaining_length > 0:
            payload = self.socket.recv(remaining_length)
            if len(payload) != remaining_length:
                logger.warning(f"Incomplete packet received")
                return None, None
        
        return packet_type, payload
    
    def _receive_loop(self):
        """Receive and process incoming packets"""
        while self.running and self.connected:
            try:
                packet_type, payload = self._read_packet()
                
                if packet_type is None:
                    logger.warning("Connection closed by broker")
                    self.connected = False
                    break
                
                if packet_type == PUBACK:
                    message_id = struct.unpack("!H", payload[0:2])[0]
                    logger.info(f"Received PUBACK for message {message_id}")
                    
                    # Remove from unacknowledged messages
                    with self.lock:
                        if message_id in self.unacknowledged_messages:
                            del self.unacknowledged_messages[message_id]
                
                elif packet_type == PUBLISH:
                    # Handle incoming PUBLISH (for completeness, though publishers typically don't receive PUBLISHes)
                    qos = (payload[0] & 0x06) >> 1
                    
                    # Parse topic
                    topic_len = struct.unpack("!H", payload[0:2])[0]
                    topic = payload[2:2+topic_len].decode('utf-8')
                    
                    # Parse message ID (only for QoS > 0)
                    message_id = None
                    payload_start = 2 + topic_len
                    
                    if qos > 0:
                        message_id = struct.unpack("!H", payload[payload_start:payload_start+2])[0]
                        payload_start += 2
                    
                    # Extract the actual message payload
                    message = payload[payload_start:]
                    
                    logger.info(f"Received PUBLISH: topic={topic}, qos={qos}, message_id={message_id}")
                    
                    # For QoS 1, send PUBACK
                    if qos == QOS_1 and message_id is not None:
                        self._send_puback(message_id)
            
            except Exception as e:
                logger.error(f"Error in receive loop: {e}")
                self.connected = False
                break

def generate_temperature_data():
    data = {
        "temperature": round(random.uniform(20.0, 30.0), 2),
        "timestamp": datetime.now().isoformat()
    }
    return data

def generate_humidity_data():
    data = {
        "humidity": round(random.uniform(30.0, 70.0), 2),
        "timestamp": datetime.now().isoformat()
    }
    return data

def generate_pressure_data():
    data = {
        "pressure": round(random.uniform(990.0, 1010.0), 2),
        "timestamp": datetime.now().isoformat()
    }
    return data

DATA_GENERATORS = {
    "sensor/temperature": generate_temperature_data,
    "sensor/humidity": generate_humidity_data,
    "sensor/pressure": generate_pressure_data,
}

def generate_data_for_topic(topic):
    return DATA_GENERATORS[topic]()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='MQTT Publisher')
    parser.add_argument('--topics', nargs='+', default=['sensor/temperature:0', 'sensor/humidity:0', 'sensor/pressure:0'],
    help='Topics to publish to in format topic:qos')
    parser.add_argument('--interval', type=float, default=1.0, help='Publish interval in seconds')
    args = parser.parse_args()

    topic_qos_pairs = []
    for topic_spec in args.topics:
        parts = topic_spec.split(':', 1)
        topic = parts[0]
        qos = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() and int(parts[1]) in [0, 1] else 0
        topic_qos_pairs.append((topic, qos))
    
    publisher = MQTTPublisher(broker_host='localhost', broker_port=1883)
    
    try:
        if publisher.connect():
            print(f"Publishing to {len(topic_qos_pairs)} topics every {args.interval} seconds:")
            for topic, qos in topic_qos_pairs:
                print(f" - {topic} (QoS {qos})")
            print("Press Ctrl+C to stop")
            
            while True:
                for topic, qos in topic_qos_pairs:
                    data = generate_data_for_topic(topic)
                    publisher.publish(topic, data, qos)
                time.sleep(args.interval)
        
    except KeyboardInterrupt:
        print("\nStopping...")
    
    finally:
        publisher.disconnect()