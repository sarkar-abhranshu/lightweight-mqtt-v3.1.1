"""
MQTT Subscriber Implementation

This module provides a custom implementation of an MQTT client with subscription capabilities.
It implements a subset of the MQTT protocol (v3.1.1) focusing on subscription functionality.

The implementation handles:
- Connection and disconnection to MQTT brokers
- Topic subscription with QoS 0 and 1
- Message reception and callback execution
- MQTT packet parsing and creation
"""
import socket
import struct
import time
import threading
import random
import json
import logging
import ssl

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', force=True)
logger = logging.getLogger('MQTT_SUBSCRIBER')

# MQTT Packet Types
CONNECT = 1      # Client request to connect to Server
CONNACK = 2      # Connect acknowledgment
PUBLISH = 3      # Publish message
PUBACK = 4       # Publish acknowledgment
SUBSCRIBE = 8    # Client subscribe request
SUBACK = 9       # Subscribe acknowledgment
DISCONNECT = 14  # Client is disconnecting

# QoS Levels
QOS_0 = 0  # At most once delivery
QOS_1 = 1  # At least once delivery

class MQTTSubscriber:
    """
    MQTT Client implementation with subscription capabilities.
    
    This class provides functionality to connect to an MQTT broker, subscribe
    to topics, and receive published messages.
    
    Attributes:
        broker_host (str): Hostname or IP address of the MQTT broker
        broker_port (int): Port number of the MQTT broker (default: 1883)
        client_id (str): Unique identifier for this client
        socket: TCP socket connection to the broker
        connected (bool): Whether the client is connected to the broker
        message_id (int): Counter for message IDs
        keep_alive (int): Keep-alive interval in seconds
        subscriptions (dict): Dictionary mapping topics to callback functions
        running (bool): Flag to control the receive thread
        lock (threading.Lock): Thread synchronization lock
        receive_thread: Thread that handles incoming messages
        ssl_enabled (bool): Whether SSL/TLS is enabled
        cafile (str): Path to CA certificate file for SSL/TLS
    """
    def __init__(self, broker_host, broker_port=1883, client_id=None, ssl_enabled=True, cafile='certs/certificate.pem'):
        """
        Initialize the MQTT Subscriber.
        
        Args:
            broker_host (str): Hostname or IP address of the MQTT broker
            broker_port (int, optional): Port number of the MQTT broker. Defaults to 1883.
            client_id (str, optional): Unique identifier for this client. If None, a random ID is generated.
            ssl_enabled (bool, optional): Whether SSL/TLS is enabled. Defaults to True.
            cafile (str, optional): Path to CA certificate file for SSL/TLS. Defaults to 'certs/certificate.pem'.
        """
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.client_id = client_id or f"subscriber-{random.randint(1000, 9999)}"
        self.ssl_enabled = ssl_enabled
        self.cafile = cafile
        if self.ssl_enabled:
            self.context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            if self.cafile:
                self.context.load_verify_locations(cafile=self.cafile)
            self.context.check_hostname = False
            self.context.verify_mode = ssl.CERT_REQUIRED
        self.socket = None
        self.connected = False
        self.message_id = 0
        self.keep_alive = 60  # seconds
        self.subscriptions = {}  # topic -> callback
        self.running = False
        self.lock = threading.Lock()
        self.receive_thread = None
        # Reentrant lock to coordinate reads between subscribe() and receive_loop()
        self.read_lock = threading.RLock()

    def connect(self, clean_session=True):
        """
        Connect to the MQTT broker.

        Opens a SSL-wrapped TCP socket connection to broker.
        
        Sends a CONNECT packet, waits for CONNACK and starts background thread for incoming messages if successful.
        
        Args:
            clean_session (bool, optional): Whether to start a clean session. Defaults to True.
                If True, the broker will not store any subscription information or undelivered messages.
                
        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            # Create and connect socket (wrapped for SSL before connecting)
            if self.ssl_enabled:
                sock = self.context.wrap_socket(
                    socket.socket(socket.AF_INET, socket.SOCK_STREAM),
                    server_hostname=self.broker_host
                )
                sock.connect((self.broker_host, self.broker_port))
                self.socket = sock
            else:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((self.broker_host, self.broker_port))
                self.socket = sock
            # Send CONNECT packet
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
        """
        Disconnect from the MQTT broker.
        
        Sends a DISCONNECT packet if connected and cleans up resources.
        """
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
        """
        Subscribe to an MQTT topic.

        Acquires a lock, increments the message ID, sends a SUBSCRIBE packet for the topic, and registers the callback for incoming messages.
        
        Args:
            topic (str): The topic to subscribe to. May contain wildcards (+ and #).
            qos (int, optional): Quality of Service level (0 or 1). Defaults to 0.
            callback (function, optional): Function to call when a message is received on this topic.
                The callback should accept two parameters: topic (str) and message (any).
                
        Returns:
            bool: True if subscription request was sent successfully, False otherwise.
        """
        # Ensure exclusive read access during subscribe
        with self.read_lock:
            if not self.connected:
                logger.error("Not connected to broker")
                return False
            try:
                with self.lock:
                    self.message_id = (self.message_id + 1) % 65536
                    message_id = self.message_id
                logger.info(f"Sending SUBSCRIBE for topic {topic} with QoS {qos}, message_id {message_id}")
                self._send_subscribe(message_id, [(topic, qos)])
                # Subscription request sent; confirmation will be handled asynchronously
                self.subscriptions[topic] = callback
                return True
            except Exception as e:
                logger.error(f"Error subscribing to topic: {e}")
                if topic in self.subscriptions:
                    del self.subscriptions[topic]
                return False

    def _send_connect(self, clean_session=True):
        """
        Send MQTT CONNECT packet to broker.
        
        Args:
            clean_session (bool): Whether to start a clean session
        """
        protocol_name = "MQTT"
        protocol_version = 4  # MQTT v3.1.1
        connect_flags = 0x02 if clean_session else 0x00  # Clean Session flag
        keep_alive = self.keep_alive
        client_id = self.client_id.encode('utf-8')
        client_id_len = len(client_id)
        
        # Construct the variable header
        variable_header = struct.pack("!H", len(protocol_name)) + protocol_name.encode('utf-8')
        variable_header += bytes([protocol_version, connect_flags])
        variable_header += struct.pack("!H", keep_alive)
        
        # Construct the payload
        payload = struct.pack("!H", client_id_len) + client_id
        
        # Calculate remaining length (variable header + payload)
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
                
        # Assemble the fixed header
        fixed_header = bytes([CONNECT << 4]) + bytes(encoded_length)
        
        # Send the packet
        self.socket.send(fixed_header + variable_header + payload)

    def _send_subscribe(self, message_id, topics_qos):
        """
        Send MQTT SUBSCRIBE packet to broker.
        
        Args:
            message_id (int): Message identifier
            topics_qos (list): List of (topic, qos) pairs to subscribe to
        """
        variable_header = struct.pack("!H", message_id)
        payload = bytearray()
        
        # Add each topic-QoS pair to the payload
        for topic, qos in topics_qos:
            topic_bytes = topic.encode('utf-8')
            payload.extend(struct.pack("!H", len(topic_bytes)))
            payload.extend(topic_bytes)
            payload.append(qos)
            
        # Calculate remaining length
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
                
        # Assemble the fixed header (type = SUBSCRIBE, flags = 0x02)
        fixed_header = bytes([SUBSCRIBE << 4 | 0x02]) + bytes(encoded_length)
        packet = fixed_header + variable_header + payload
        
        logger.info(f"Sending SUBSCRIBE packet: message_id={message_id}, packet_length={len(packet)}")
        self.socket.send(packet)

    def _send_puback(self, message_id):
        """
        Send MQTT PUBACK packet to acknowledge receipt of a PUBLISH packet.
        
        Args:
            message_id (int): Message identifier of the PUBLISH packet
        """
        fixed_header = bytes([PUBACK << 4, 2])  # Length is always 2 for PUBACK
        variable_header = struct.pack("!H", message_id)
        self.socket.send(fixed_header + variable_header)

    def _send_disconnect(self):
        """
        Send MQTT DISCONNECT packet to broker.
        
        This tells the broker that the client is disconnecting cleanly.
        """
        fixed_header = bytes([DISCONNECT << 4, 0])  # Length is always 0 for DISCONNECT
        self.socket.send(fixed_header)

    def _read_packet(self):
        """
        Read and parse an MQTT packet from the socket.
        
        Returns:
            tuple: (packet_type, payload) or (packet_type, payload, flags) for PUBLISH packets
        """
        try:
            # Read the first byte (packet type and flags)
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
                
            # Read the remaining length
            multiplier = 1
            remaining_length = 0
            try:
                for i in range(4):  # Maximum 4 bytes for remaining length
                    byte_data = self.socket.recv(1)
                    if not byte_data or len(byte_data) == 0:
                        return None, None
                    byte = byte_data[0]
                    remaining_length += (byte & 127) * multiplier
                    multiplier *= 128
                    if not (byte & 128):  # If the continuation bit is not set, we're done
                        break
                    if i == 3 and (byte & 128):  # 4th byte must not have continuation bit
                        logger.warning("Malformed remaining length in packet")
                        return None, None
            except Exception as e:
                logger.error(f"Error reading remaining length: {e}")
                return None, None
                
            # Sanity check for length (prevent huge allocations)
            if remaining_length > 10000000:
                logger.warning(f"Unreasonable remaining length: {remaining_length}, skipping packet")
                return None, None
                
            # Read the payload
            payload = b''
            if remaining_length > 0:
                try:
                    bytes_received = 0
                    timeout_start = time.time()
                    while bytes_received < remaining_length:
                        if time.time() - timeout_start > 5:  # 5-second timeout
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
            
            # For PUBLISH packets, also return the flags
            if packet_type == PUBLISH:
                return packet_type, payload, flags
            else:
                return packet_type, payload
        except Exception as e:
            logger.error(f"Error reading packet: {e}")
            return None, None

    def _receive_loop(self):
        """
        Main receiving loop that processes incoming packets.
        
        This method runs in a separate thread and handles all incoming MQTT packets.
        It processes PUBLISH, PUBACK, and SUBACK packets and dispatches messages to callbacks.
        """
        while self.running and self.connected:
            try:
                # Use select to wait for data with a timeout
                import select
                readable, _, _ = select.select([self.socket], [], [], 0.1)
                if not readable:
                    continue
                    
                # Read and process the packet under lock
                with self.read_lock:
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
                    
                # Process packet based on type
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
                        # Extract QoS level from flags
                        qos = (flags & 0x06) >> 1
                        
                        # Check for malformed packet
                        if len(payload) < 2:
                            logger.warning("Received malformed PUBLISH packet (too short)")
                            continue
                            
                        # Extract topic
                        topic_len = struct.unpack("!H", payload[0:2])[0]
                        if topic_len > 1000 or topic_len < 0:
                            logger.warning(f"Received invalid topic length: {topic_len}, ignoring packet")
                            continue
                        if len(payload) < 2 + topic_len:
                            logger.warning(f"Received malformed PUBLISH packet (topic length: {topic_len}, payload length: {len(payload)})")
                            continue
                            
                        topic = payload[2:2+topic_len].decode('utf-8')
                        
                        # Extract message ID (only for QoS > 0)
                        message_id = None
                        payload_start = 2 + topic_len
                        if qos > 0 and payload_start + 2 <= len(payload):
                            message_id = struct.unpack("!H", payload[payload_start:payload_start+2])[0]
                            payload_start += 2
                            
                        # Extract message content
                        if payload_start < len(payload):
                            message = payload[payload_start:]
                        else:
                            message = b''
                            
                        # Try to convert message to JSON or string
                        try:
                            message_str = message.decode('utf-8')
                            try:
                                message_data = json.loads(message_str)
                            except:
                                message_data = message_str
                        except:
                            message_data = message
                            
                        logger.info(f"Received PUBLISH: topic={topic}, qos={qos}, message_id={message_id}")
                        
                        # Call appropriate callbacks for matching subscriptions
                        for sub_topic, callback in list(self.subscriptions.items()):
                            if self._topic_matches(sub_topic, topic) and callback:
                                try:
                                    callback(topic, message_data)
                                except Exception as e:
                                    logger.error(f"Error in callback for topic {topic}: {e}")
                                    
                        # Send PUBACK for QoS 1 messages
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
        """
        Check if a subscription topic matches a published topic considering MQTT wildcards.
        
        Args:
            subscription_topic (str): The topic filter with possible wildcards
            published_topic (str): The actual topic of the published message
            
        Returns:
            bool: True if the subscription topic matches the published topic
        """
        # Direct match
        if subscription_topic == published_topic:
            return True
            
        sub_parts = subscription_topic.split('/')
        pub_parts = published_topic.split('/')
        
        # Handle multi-level wildcard '#' (must be at the end)
        if len(sub_parts) > 0 and sub_parts[-1] == '#':
            for i in range(len(sub_parts) - 1):
                if i >= len(pub_parts):
                    return False
                if sub_parts[i] != '+' and sub_parts[i] != pub_parts[i]:
                    return False
            return True
            
        # Without '#', levels must match exactly
        if len(sub_parts) != len(pub_parts):
            return False
            
        # Check each level with single-level wildcard '+' support
        for i in range(len(sub_parts)):
            if sub_parts[i] != '+' and sub_parts[i] != pub_parts[i]:
                return False
                
        return True
