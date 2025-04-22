"""
MQTT Broker Implementation

This module implements a simplified MQTT (Message Queuing Telemetry Transport) broker that supports basic MQTT operations including connection management, publish/subscribe functionality, and QoS levels 0 and 1.

The broker handles:
- Client connections and disconnections
- Topic subscriptions 
- Message publishing and delivery
- QoS (Quality of Service) levels 0 (at most once) and 1 (at least once)
- Session persistence for non-clean sessions

This implementation follows a subset of the MQTT 3.1.1 specification.
"""

import socket
import threading
import struct
import time
import json
import logging
import ssl

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('MQTT_BROKER')

# MQTT Packet Types
CONNECT = 1     # Client request to connect to broker
CONNACK = 2     # Connect acknowledgment
PUBLISH = 3     # Publish message
PUBACK = 4      # Publish acknowledgment
SUBSCRIBE = 8   # Client subscribe request
SUBACK = 9      # Subscribe acknowledgment
DISCONNECT = 14 # Client disconnection request

# QoS Levels
QOS_0 = 0  # At most once delivery (fire and forget)
QOS_1 = 1  # At least once delivery (acknowledged delivery)

class MQTTBroker:
    """
    MQTT Broker implementation that handles client connections and message routing.
    
    The broker manages client connections, subscriptions to topics, and message delivery between publishers and subscribers according to MQTT protocol rules.
    """
    def __init__(self, host='0.0.0.0', port=1883, ssl_enabled=False, certfile=None, keyfile=None):
        """
        Initialize the MQTT broker with host, port, SSL options, and dictionaries to track clients, session data, subscriptions, and unacknowledged messages.
        
        Args:
            host (str): The host address to bind the broker to. Default is '0.0.0.0' (all interfaces).
            port (int): The port to listen on. Default is 1883 (standard MQTT port).
            ssl_enabled (bool): Whether SSL/TLS is enabled. Default is False.
            certfile (str): Path to the SSL certificate file. Required if SSL is enabled.
            keyfile (str): Path to the SSL private key file. Required if SSL is enabled.
        """
        self.host = host
        self.port = port
        self.server_socket = None
        self.ssl_enabled = ssl_enabled
        self.certfile = certfile
        self.keyfile = keyfile
        if self.ssl_enabled:
            self.context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            self.context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
        self.clients = {}  # client_id -> socket
        self.sessions = {}  # client_id -> session data
        self.subscriptions = {}  # topic -> list of client_ids
        self.running = False
        self.message_id_counter = 1
        self.unacknowledged_messages = {}  # client_id -> list of (message_id, topic, payload, qos)
        
    def start(self):
        """
        Start the MQTT broker server.
        
        Creates a socket, binds it to the specified host and port, and begins accepting client connections. For each client, it wraps the socket in SSL if SSL is enabled and logs the connection. Each client connection is handled in a separate thread (to avoid race conditions).
        
        Raises:
            Exception: Any error during server operation
        """
        try:
            raw_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            raw_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            raw_sock.bind((self.host, self.port))
            raw_sock.listen(5)
            # Use raw listening socket; wrap each client connection
            self.server_socket = raw_sock
            self.running = True
            
            logger.info(f"MQTT Broker started on {self.host}:{self.port}")
            
            while self.running:
                client_socket, client_address = self.server_socket.accept()
                # Wrap per-connection for SSL
                if self.ssl_enabled:
                    try:
                        client_conn = self.context.wrap_socket(client_socket, server_side=True)
                    except Exception as e:
                        logger.error(f"SSL handshake failed for {client_address}: {e}")
                        client_socket.close()
                        continue
                else:
                    client_conn = client_socket
                logger.info(f"New connection from {client_address}")
                # Start client handler thread
                client_thread = threading.Thread(target=self._handle_client, args=(client_conn, client_address))
                client_thread.daemon = True
                client_thread.start()
                
        except Exception as e:
            logger.error(f"Error in broker: {e}")
        finally:
            if self.server_socket:
                self.server_socket.close()
            self.running = False
            logger.info("MQTT Broker stopped")
    
    def stop(self):
        """
        Stop the MQTT broker server.
        
        Closes the server socket and terminates all active connections.
        """
        self.running = False
        if self.server_socket:
            self.server_socket.close()
    
    def _disconnect_client(self, client_id):
        """
        Disconnect a client due to timeout or explicit disconnect request.
        
        Closes and removes the client socket.
        
        If the client's session is configured as 'clean_session', all subscriptions, session data and unacknowledged messages are removed. Otherwise, the session state is preserved for future reconnection.
        
        Args:
            client_id (str): The ID of the client to disconnect
        """
        if client_id in self.clients:
            try:
                self.clients[client_id].close()
            except:
                pass
            del self.clients[client_id]
            
            # If clean session, remove all subscriptions and session data
            if client_id in self.sessions:
                if self.sessions[client_id].get('clean_session', True):
                    # Remove subscriptions
                    for topic, subscribers in list(self.subscriptions.items()):
                        if client_id in subscribers:
                            subscribers.remove(client_id)
                            if not subscribers:
                                del self.subscriptions[topic]
                    
                    # Remove session data
                    del self.sessions[client_id]
                    
                    # Remove unacknowledged messages
                    if client_id in self.unacknowledged_messages:
                        del self.unacknowledged_messages[client_id]
    
    def _handle_client(self, client_socket, client_address):
        """
        Handle communication with a connected client.
        
        Reads incoming MQTT packets, processes them according to their type, and generates appropriate responses. This method runs in a separate thread for each connected client. On disconnect or error, ensures resources are cleaned up.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            client_address (tuple): The client's address (host, port)
        """
        client_id = None
        
        try:
            while self.running:
                try:
                    # Read fixed header
                    first_byte_data = client_socket.recv(1)
                    if not first_byte_data or len(first_byte_data) == 0:
                        logger.info(f"Client {client_address} disconnected")
                        break
                    
                    first_byte = first_byte_data[0]
                    packet_type = (first_byte & 0xF0) >> 4
                    flags = first_byte & 0x0F
                    
                    # Read remaining length using variable length encoding
                    multiplier = 1
                    remaining_length = 0
                    while True:
                        byte_data = client_socket.recv(1)
                        if not byte_data or len(byte_data) == 0:
                            logger.info(f"Client {client_address} disconnected while reading remaining length")
                            return
                            
                        byte = byte_data[0]
                        remaining_length += (byte & 127) * multiplier
                        multiplier *= 128
                        if not (byte & 128):
                            break
                    
                    # Read the payload based on remaining length
                    payload = b''
                    if remaining_length > 0:
                        bytes_received = 0
                        while bytes_received < remaining_length:
                            chunk = client_socket.recv(remaining_length - bytes_received)
                            if not chunk:
                                logger.warning(f"Client {client_address} disconnected while reading payload")
                                return
                            payload += chunk
                            bytes_received += len(chunk)
                    
                    # Process the packet based on its type
                    if packet_type == CONNECT:
                        client_id = self._handle_connect(client_socket, payload, client_address)
                        if not client_id:
                            logger.warning(f"Failed to establish connection with {client_address}")
                            break
                    elif packet_type == PUBLISH:
                        self._handle_publish(client_socket, payload, flags)
                    elif packet_type == SUBSCRIBE:
                        self._handle_subscribe(client_socket, payload)
                    elif packet_type == DISCONNECT:
                        self._handle_disconnect(client_id)
                        break
                    elif packet_type == PUBACK:
                        self._handle_puback(payload, client_id)
                    else:
                        logger.warning(f"Unsupported packet type: {packet_type} from {client_address}")
                        
                except ConnectionError as e:
                    logger.error(f"Connection error with client {client_address}: {e}")
                    break
                except Exception as e:
                    logger.error(f"Error processing packet from {client_address}: {e}")
                    # Continue processing other packets
        
        except Exception as e:
            logger.error(f"Error handling client {client_address}: {e}")
        finally:
            if client_id:
                self._disconnect_client(client_id)
            else:
                try:
                    client_socket.close()
                except:
                    pass
            logger.info(f"Client {client_address} handler terminated")
    
    def _handle_connect(self, client_socket, payload, client_address):
        """
        Handle CONNECT packet from a client.
        
        Processes a connection request, extracts client information, and establishes a session. Sends a CONNACK response.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            payload (bytes): The packet payload containing connection details
            client_address (tuple): The client's address (host, port)
            
        Returns:
            str or None: The client ID if connection was successful, None otherwise
        """
        try:
            # Parse protocol name
            protocol_name_len = struct.unpack("!H", payload[0:2])[0]
            protocol_name = payload[2:2+protocol_name_len].decode('utf-8')
            
            # Parse protocol version
            protocol_version = payload[2+protocol_name_len]
            
            # Parse connect flags
            connect_flags = payload[3+protocol_name_len]
            clean_session = bool(connect_flags & 0x02)
            
            # Parse keep alive
            keep_alive = struct.unpack("!H", payload[4+protocol_name_len:6+protocol_name_len])[0]
            
            # Parse client ID
            client_id_len = struct.unpack("!H", payload[6+protocol_name_len:8+protocol_name_len])[0]
            client_id = payload[8+protocol_name_len:8+protocol_name_len+client_id_len].decode('utf-8')
            
            logger.info(f"CONNECT: client_id={client_id}, clean_session={clean_session}, keep_alive={keep_alive}")
            
            # Check if client ID already exists
            if client_id in self.clients:
                # If the client is already connected, disconnect the old connection
                old_socket = self.clients[client_id]
                try:
                    old_socket.close()
                except:
                    pass
            
            # Store client information
            self.clients[client_id] = client_socket
            
            # Create or update session
            if client_id not in self.sessions or clean_session:
                self.sessions[client_id] = {
                    'clean_session': clean_session,
                    'keep_alive': keep_alive
                }
            else:
                self.sessions[client_id]['keep_alive'] = keep_alive
            
            # Send CONNACK
            self._send_connack(client_socket, 0)  # 0 = connection accepted
            
            return client_id
            
        except Exception as e:
            logger.error(f"Error handling CONNECT from {client_address}: {e}")
            # Send CONNACK with error
            self._send_connack(client_socket, 1)  # 1 = connection refused
            return None
    
    def _send_connack(self, client_socket, return_code):
        """
        Send CONNACK packet to a client, indicating connection acceptance or refusal.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            return_code (int): The connection return code (0 for success)
        """
        # Fixed header
        fixed_header = bytes([CONNACK << 4, 2])  # 2 bytes in variable header
        
        # Variable header
        # First byte is reserved, second is return code
        variable_header = bytes([0, return_code])
        
        # Send packet
        client_socket.send(fixed_header + variable_header)
    
    def _handle_publish(self, client_socket, payload, flags):
        """
        Handle PUBLISH packet from a client.
        
        Extracts published message information and forwards it to subscribers.
        For QoS 1, sends a PUBACK response.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            payload (bytes): The packet payload containing topic and message
            flags (int): Packet flags including QoS level
        """
        try:
            qos = (flags & 0x06) >> 1
            retain = flags & 0x01
            dup = (flags & 0x08) >> 3
            
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
            
            logger.info(f"PUBLISH: topic={topic}, qos={qos}, message_id={message_id}, payload_length={len(message)}")
            
            # Forward the message to all subscribers of this topic
            self._forward_message(topic, message, qos, message_id)
            
            # For QoS 1, send PUBACK
            if qos == QOS_1 and message_id is not None:
                self._send_puback(client_socket, message_id)
                
        except Exception as e:
            logger.error(f"Error handling PUBLISH: {e}")
    
    def _forward_message(self, topic, message, qos, message_id):
        """
        Forward a published message to all subscribers of the topic.
        
        For QoS 1 messages, tracks delivery until acknowledgment.
        
        Args:
            topic (str): The topic the message was published to
            message (bytes): The message payload
            qos (int): Quality of Service level
            message_id (int or None): Message identifier for QoS > 0
        """
        # Find all subscribers for the exact topic
        matching_subscribers = set()
        
        # Direct match only
        if topic in self.subscriptions:
            matching_subscribers.update(self.subscriptions[topic])
        
        # Forward the message to each subscriber
        for client_id in matching_subscribers:
            if client_id in self.clients:
                try:
                    client_socket = self.clients[client_id]
                    
                    # Generate a new message ID for this subscriber
                    subscriber_message_id = self.message_id_counter
                    self.message_id_counter = (self.message_id_counter + 1) % 65536
                    
                    # Send the message
                    self._send_publish(client_socket, topic, message, qos, subscriber_message_id)
                    
                    # For QoS 1, store the message until PUBACK is received
                    if qos == QOS_1:
                        if client_id not in self.unacknowledged_messages:
                            self.unacknowledged_messages[client_id] = []
                        self.unacknowledged_messages[client_id].append((subscriber_message_id, topic, message, qos))
                        
                except Exception as e:
                    logger.error(f"Error forwarding message to {client_id}: {e}")
    
    def _send_publish(self, client_socket, topic, payload, qos, message_id=None):
        """
        Send PUBLISH packet to a client.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            topic (str): The topic to publish to
            payload (bytes): The message payload
            qos (int): Quality of Service level
            message_id (int, optional): Message identifier for QoS > 0
        """
        # Calculate flags
        flags = qos << 1
        
        # Encode topic
        encoded_topic = topic.encode('utf-8')
        topic_len = len(encoded_topic)
        variable_header = struct.pack("!H", topic_len) + encoded_topic
        
        # Add message ID for QoS > 0
        if qos > 0:
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
        client_socket.send(fixed_header + variable_header + payload)
    
    def _send_puback(self, client_socket, message_id):
        """
        Send PUBACK packet to acknowledge message receipt (QoS 1).
        
        Args:
            client_socket (socket.socket): The client's socket connection
            message_id (int): The message identifier being acknowledged
        """
        # Fixed header
        fixed_header = bytes([PUBACK << 4, 2])  # 2 bytes in variable header
        
        # Variable header (message ID)
        variable_header = struct.pack("!H", message_id)
        
        # Send packet
        client_socket.send(fixed_header + variable_header)
    
    def _handle_puback(self, payload, client_id):
        """
        Handle PUBACK packet from a client.
        
        Removes the acknowledged message from the tracking list of unacknowledged messages.
        
        Args:
            payload (bytes): The packet payload containing the message ID
            client_id (str): The ID of the client sending the acknowledgment
        """
        try:
            message_id = struct.unpack("!H", payload[0:2])[0]
            logger.info(f"PUBACK: client_id={client_id}, message_id={message_id}")
            
            # Remove the message from unacknowledged messages
            if client_id in self.unacknowledged_messages:
                self.unacknowledged_messages[client_id] = [
                    msg for msg in self.unacknowledged_messages[client_id] 
                    if msg[0] != message_id
                ]
                
        except Exception as e:
            logger.error(f"Error handling PUBACK: {e}")
    
    def _handle_subscribe(self, client_socket, payload):
        """
        Handle SUBSCRIBE packet from a client.
        
        Processes subscription requests to one or more topics and
        sends a SUBACK response.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            payload (bytes): The packet payload containing topics and QoS
        """
        try:
            # Parse message ID
            if len(payload) < 2:
                logger.warning("Received malformed SUBSCRIBE packet (too short)")
                return
                
            message_id = struct.unpack("!H", payload[0:2])[0]
            
            # Find client ID from socket
            client_id = None
            for cid, sock in self.clients.items():
                if sock == client_socket:
                    client_id = cid
                    break
            
            if not client_id:
                logger.warning("Received SUBSCRIBE from unknown client")
                return
            
            # Parse topics and QoS
            topics_qos = []
            pos = 2
            
            while pos < len(payload):
                try:
                    # Parse topic
                    if pos + 2 > len(payload):
                        logger.warning("Malformed SUBSCRIBE packet: incomplete topic length")
                        break
                        
                    topic_len = struct.unpack("!H", payload[pos:pos+2])[0]
                    pos += 2
                    
                    if pos + topic_len > len(payload):
                        logger.warning("Malformed SUBSCRIBE packet: incomplete topic")
                        break
                        
                    topic = payload[pos:pos+topic_len].decode('utf-8')
                    pos += topic_len
                    
                    # Parse QoS
                    if pos >= len(payload):
                        logger.warning("Malformed SUBSCRIBE packet: missing QoS")
                        break
                        
                    qos = payload[pos]
                    pos += 1
                    
                    topics_qos.append((topic, qos))
                except Exception as e:
                    logger.error(f"Error parsing SUBSCRIBE topic: {e}")
                    break
            
            logger.info(f"SUBSCRIBE: client_id={client_id}, message_id={message_id}, topics={topics_qos}")
            
            # Add subscriptions
            return_codes = []
            for topic, qos in topics_qos:
                # Add to subscriptions
                if topic not in self.subscriptions:
                    self.subscriptions[topic] = []
                if client_id not in self.subscriptions[topic]:
                    self.subscriptions[topic].append(client_id)
                
                # Store QoS in session
                if client_id in self.sessions:
                    if 'subscriptions' not in self.sessions[client_id]:
                        self.sessions[client_id]['subscriptions'] = {}
                    self.sessions[client_id]['subscriptions'][topic] = qos
                
                # Add return code
                return_codes.append(min(qos, 1))  # QoS limited to 0 or 1
            
            # Send SUBACK
            self._send_suback(client_socket, message_id, return_codes)
            
        except Exception as e:
            logger.error(f"Error handling SUBSCRIBE: {e}")
    
    def _send_suback(self, client_socket, message_id, return_codes):
        """
        Send SUBACK packet to confirm subscriptions.
        
        Args:
            client_socket (socket.socket): The client's socket connection
            message_id (int): The message identifier from the SUBSCRIBE packet
            return_codes (list): List of QoS levels granted for each subscription
        """
        try:
            # Fixed header
            remaining_length = 2 + len(return_codes)  # 2 for message ID, 1 for each return code
            
            # Encode remaining length
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
                    
            fixed_header = bytes([SUBACK << 4]) + bytes(encoded_length)
            
            # Variable header (message ID)
            variable_header = struct.pack("!H", message_id)
            
            # Payload (return codes)
            payload = bytes(return_codes)
            
            # Send packet
            packet = fixed_header + variable_header + payload
            logger.info(f"Sending SUBACK: message_id={message_id}, return_codes={return_codes}, packet_length={len(packet)}")
            client_socket.send(packet)
        except Exception as e:
            logger.error(f"Error sending SUBACK: {e}")
    
    def _handle_disconnect(self, client_id):
        """
        Handle DISCONNECT packet from a client.
        
        Closes the connection gracefully and updates client state.
        
        Args:
            client_id (str): The ID of the client being disconnected
        """
        logger.info(f"DISCONNECT: client_id={client_id}")
        
        # Disconnect the client
        self._disconnect_client(client_id)

if __name__ == "__main__":
    """
    Main entry point to start the MQTT broker when the script is run directly.
    
    The broker will listen on all interfaces (0.0.0.0) on the standard MQTT port (1883).
    """
    # Hardcoded SSL/TLS broker configuration
    host = '0.0.0.0'
    port = 1883
    certfile = 'certs/certificate.pem'
    keyfile = 'certs/private_key.pem'
    broker = MQTTBroker(host, port, ssl_enabled=True, certfile=certfile, keyfile=keyfile)
    try:
        broker.start()
    except KeyboardInterrupt:
        broker.stop()