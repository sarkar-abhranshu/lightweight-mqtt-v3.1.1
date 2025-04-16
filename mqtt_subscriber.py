import socket
import struct
import time
import threading
import random
import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MQTT_SUBSCRIBER')

# MQTT Packet Types
CONNECT = 1
CONNACK = 2
PUBLISH = 3
PUBACK = 4
SUBSCRIBE = 8
SUBACK = 9
DISCONNECT = 14

# QoS Levels
QOS_0 = 0  # At most once
QOS_1 = 1  # At least once

class MQTTSubscriber:
    def __init__(self, broker_host, broker_port=1883, client_id=None):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"subscriber-{random.randint(1000, 9999)}"
        self.socket = None
        self.connected = False
        self.message_id = 0
        self.keep_alive = 60  # seconds
        self.subscriptions = {}  # topic -> callback
        self.running = False
        self.lock = threading.Lock()
        self.receive_thread = None

    def connect(self, clean_session=True):
        """Connect to the MQTT broker"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.broker_host, self.broker_port))
            self._send_connect(clean_session)
            packet_type, payload = self._read_packet()
            if packet_type == CONNACK:
                return_code = payload[1]
                if return_code == 0:
                    logger.info(f"Connected to broker at {self.broker_host}:{self.broker_port}")
                    self.connected = True
                    self.running = True
                    self.receive_thread = threading.Thread(target=self._receive_loop)
                    self.receive_thread.daemon = True
                    self.receive_thread.start()
                    logger.info(f"Started receive thread")
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
                self._send_disconnect()
                self.socket.close()
                self.running = False
                if self.receive_thread:
                    self.receive_thread.join(1.0)
                logger.info("Disconnected from broker")
            except Exception as e:
                logger.error(f"Error disconnecting: {e}")
            finally:
                self.connected = False

    def subscribe(self, topic, qos=0, callback=None):
        """Subscribe to a topic"""
        if not self.connected:
            logger.error("Not connected to broker")
            return False
        try:
            with self.lock:
                self.message_id = (self.message_id + 1) % 65536
                message_id = self.message_id
            logger.info(f"Sending SUBSCRIBE for topic {topic} with QoS {qos}, message_id {message_id}")
            self._send_subscribe(message_id, [(topic, qos)])
            self.subscriptions[topic] = callback
            start_time = time.time()
            while time.time() - start_time < 5:
                try:
                    import select
                    readable, _, _ = select.select([self.socket], [], [], 0.1)
                    if not readable:
                        continue
                    result = self._read_packet()
                    if result is None or result[0] is None:
                        logger.warning("Connection closed while waiting for SUBACK")
                        return False
                    if len(result) == 3:
                        packet_type, payload, _ = result
                    else:
                        packet_type, payload = result
                    logger.debug(f"Received packet type {packet_type} while waiting for SUBACK")
                    if packet_type == SUBACK:
                        if len(payload) >= 2:
                            received_message_id = struct.unpack("!H", payload[0:2])[0]
                            logger.info(f"Received SUBACK with message_id {received_message_id}, expected {message_id}")
                            if received_message_id == message_id:
                                if len(payload) >= 3:
                                    return_code = payload[2]
                                    if return_code <= 1:
                                        logger.info(f"Subscription confirmed for {topic} with QoS {return_code}")
                                        return True
                                    else:
                                        logger.error(f"Subscription failed, return code: {return_code}")
                                        if topic in self.subscriptions:
                                            del self.subscriptions[topic]
                                        return False
                                else:
                                    logger.warning("Received malformed SUBACK packet (too short)")
                            else:
                                logger.warning(f"Message ID mismatch: expected {message_id}, got {received_message_id}")
                        else:
                            logger.warning(f"Received malformed SUBACK packet (length: {len(payload)})")
                    elif packet_type == PUBLISH:
                        logger.debug("Received PUBLISH while waiting for SUBACK")
                    else:
                        logger.warning(f"Unexpected packet type while waiting for SUBACK: {packet_type}")
                except Exception as e:
                    logger.error(f"Error while waiting for SUBACK: {e}")
                    break
            logger.warning(f"Timed out waiting for SUBACK for topic {topic}")
            return True
        except Exception as e:
            logger.error(f"Error subscribing to topic: {e}")
            if topic in self.subscriptions:
                del self.subscriptions[topic]
            return False

    def _send_connect(self, clean_session=True):
        """Send CONNECT packet"""
        protocol_name = "MQTT"
        protocol_version = 4
        connect_flags = 0x02 if clean_session else 0x00
        keep_alive = self.keep_alive
        client_id = self.client_id.encode('utf-8')
        client_id_len = len(client_id)
        variable_header = struct.pack("!H", len(protocol_name)) + protocol_name.encode('utf-8')
        variable_header += bytes([protocol_version, connect_flags])
        variable_header += struct.pack("!H", keep_alive)
        payload = struct.pack("!H", client_id_len) + client_id
        remaining_length = len(variable_header) + len(payload)
        encoded_length = bytearray()
        while True:
            byte = remaining_length % 128
            remaining_length = remaining_length // 128
            if remaining_length > 0:
                byte |= 0x80
            encoded_length.append(byte)
            if remaining_length == 0:
                break
        fixed_header = bytes([CONNECT << 4]) + bytes(encoded_length)
        self.socket.send(fixed_header + variable_header + payload)

    def _send_subscribe(self, message_id, topics_qos):
        """Send SUBSCRIBE packet"""
        variable_header = struct.pack("!H", message_id)
        payload = bytearray()
        for topic, qos in topics_qos:
            topic_bytes = topic.encode('utf-8')
            payload.extend(struct.pack("!H", len(topic_bytes)))
            payload.extend(topic_bytes)
            payload.append(qos)
        remaining_length = len(variable_header) + len(payload)
        encoded_length = bytearray()
        length = remaining_length
        while True:
            byte = length % 128
            length = length // 128
            if length > 0:
                byte |= 0x80
            encoded_length.append(byte)
            if length == 0:
                break
        fixed_header = bytes([SUBSCRIBE << 4 | 0x02]) + bytes(encoded_length)
        packet = fixed_header + variable_header + payload
        logger.info(f"Sending SUBSCRIBE packet: message_id={message_id}, packet_length={len(packet)}")
        self.socket.send(packet)

    def _send_puback(self, message_id):
        """Send PUBACK packet"""
        fixed_header = bytes([PUBACK << 4, 2])
        variable_header = struct.pack("!H", message_id)
        self.socket.send(fixed_header + variable_header)

    def _send_disconnect(self):
        """Send DISCONNECT packet"""
        fixed_header = bytes([DISCONNECT << 4, 0])
        self.socket.send(fixed_header)

    def _read_packet(self):
        """Read an MQTT packet from the socket"""
        try:
            first_byte = self.socket.recv(1)
            if not first_byte or len(first_byte) == 0:
                return None, None
            packet_type = (first_byte[0] & 0xF0) >> 4
            flags = first_byte[0] & 0x0F
            if packet_type == 0:
                logger.warning("Received invalid packet type 0, skipping")
                try:
                    self.socket.recv(10)
                except:
                    pass
                return None, None
            multiplier = 1
            remaining_length = 0
            try:
                for i in range(4):
                    byte_data = self.socket.recv(1)
                    if not byte_data or len(byte_data) == 0:
                        return None, None
                    byte = byte_data[0]
                    remaining_length += (byte & 127) * multiplier
                    multiplier *= 128
                    if not (byte & 128):
                        break
                    if i == 3 and (byte & 128):
                        logger.warning("Malformed remaining length in packet")
                        return None, None
            except Exception as e:
                logger.error(f"Error reading remaining length: {e}")
                return None, None
            if remaining_length > 10000000:
                logger.warning(f"Unreasonable remaining length: {remaining_length}, skipping packet")
                return None, None
            payload = b''
            if remaining_length > 0:
                try:
                    bytes_received = 0
                    timeout_start = time.time()
                    while bytes_received < remaining_length:
                        if time.time() - timeout_start > 5:
                            logger.warning("Timeout reading packet payload")
                            return None, None
                        chunk = self.socket.recv(min(1024, remaining_length - bytes_received))
                        if not chunk:
                            logger.warning(f"Connection closed while reading payload")
                            return None, None
                        payload += chunk
                        bytes_received += len(chunk)
                except Exception as e:
                    logger.error(f"Error reading payload: {e}")
                    return None, None
            logger.debug(f"Successfully read packet type {packet_type}, length {remaining_length}")
            if packet_type == PUBLISH:
                return packet_type, payload, flags
            else:
                return packet_type, payload
        except Exception as e:
            logger.error(f"Error reading packet: {e}")
            return None, None

    def _receive_loop(self):
        """Receive and process incoming packets"""
        while self.running and self.connected:
            try:
                import select
                readable, _, _ = select.select([self.socket], [], [], 0.1)
                if not readable:
                    continue
                first_byte = self.socket.recv(1, socket.MSG_PEEK)
                if not first_byte or len(first_byte) == 0:
                    logger.warning("Connection closed by broker")
                    self.connected = False
                    break
                packet_type = (first_byte[0] & 0xF0) >> 4
                if packet_type == 0:
                    logger.warning("Detected invalid packet type 0, discarding bytes")
                    self.socket.recv(1)
                    try:
                        self.socket.recv(10)
                    except:
                        pass
                    continue
                result = self._read_packet()
                if result is None or result[0] is None:
                    logger.warning("Error reading packet, trying to continue")
                    time.sleep(0.1)
                    continue
                if len(result) == 3:
                    packet_type, payload, flags = result
                else:
                    packet_type, payload = result
                    flags = 0
                if packet_type == PUBACK:
                    if len(payload) >= 2:
                        message_id = struct.unpack("!H", payload[0:2])[0]
                        logger.info(f"Received PUBACK for message {message_id}")
                    else:
                        logger.warning("Received malformed PUBACK packet")
                elif packet_type == SUBACK:
                    if len(payload) >= 2:
                        message_id = struct.unpack("!H", payload[0:2])[0]
                        logger.info(f"Received SUBACK in receive loop with message_id {message_id}")
                        if len(payload) >= 3:
                            return_code = payload[2]
                            logger.info(f"SUBACK return code: {return_code}")
                    else:
                        logger.warning("Received malformed SUBACK packet")
                elif packet_type == PUBLISH:
                    try:
                        qos = (flags & 0x06) >> 1
                        if len(payload) < 2:
                            logger.warning("Received malformed PUBLISH packet (too short)")
                            continue
                        topic_len = struct.unpack("!H", payload[0:2])[0]
                        if topic_len > 1000 or topic_len < 0:
                            logger.warning(f"Received invalid topic length: {topic_len}, ignoring packet")
                            continue
                        if len(payload) < 2 + topic_len:
                            logger.warning(f"Received malformed PUBLISH packet (topic length: {topic_len}, payload length: {len(payload)})")
                            continue
                        topic = payload[2:2+topic_len].decode('utf-8')
                        message_id = None
                        payload_start = 2 + topic_len
                        if qos > 0 and payload_start + 2 <= len(payload):
                            message_id = struct.unpack("!H", payload[payload_start:payload_start+2])[0]
                            payload_start += 2
                        if payload_start < len(payload):
                            message = payload[payload_start:]
                        else:
                            message = b''
                        try:
                            message_str = message.decode('utf-8')
                            try:
                                message_data = json.loads(message_str)
                            except:
                                message_data = message_str
                        except:
                            message_data = message
                        logger.info(f"Received PUBLISH: topic={topic}, qos={qos}, message_id={message_id}")
                        for sub_topic, callback in list(self.subscriptions.items()):
                            if self._topic_matches(sub_topic, topic) and callback:
                                try:
                                    callback(topic, message_data)
                                except Exception as e:
                                    logger.error(f"Error in callback for topic {topic}: {e}")
                        if qos == QOS_1 and message_id is not None:
                            self._send_puback(message_id)
                    except Exception as e:
                        logger.error(f"Error processing PUBLISH packet: {e}")
                else:
                    logger.warning(f"Received unhandled packet type: {packet_type}")
            except ConnectionError as e:
                logger.error(f"Connection error in receive loop: {e}")
                self.connected = False
                break
            except Exception as e:
                logger.error(f"Error in receive loop: {e}")
                time.sleep(0.1)

    def _topic_matches(self, subscription_topic, published_topic):
        """Check if a subscription topic matches a published topic"""
        # Direct match
        if subscription_topic == published_topic:
            return True
        sub_parts = subscription_topic.split('/')
        pub_parts = published_topic.split('/')
        if len(sub_parts) > 0 and sub_parts[-1] == '#':
            for i in range(len(sub_parts) - 1):
                if i >= len(pub_parts):
                    return False
                if sub_parts[i] != '+' and sub_parts[i] != pub_parts[i]:
                    return False
            return True
        if len(sub_parts) != len(pub_parts):
            return False
        for i in range(len(sub_parts)):
            if sub_parts[i] != '+' and sub_parts[i] != pub_parts[i]:
                return False
        return True