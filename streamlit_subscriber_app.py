import streamlit as st
import pandas as pd
import time
import json
from mqtt_subscriber import MQTTSubscriber
import threading
import socket
import logging
import queue
import os

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('STREAMLIT_APP')

# Create a message queue for thread-safe communication
message_queue = queue.Queue()

# Create a file-based storage for messages to persist between Streamlit reruns
MESSAGES_FILE = "mqtt_messages.json"

# Initialize session state
if 'initialized' not in st.session_state:
    st.session_state.initialized = True
    st.session_state.messages = []
    st.session_state.connected = False
    st.session_state.subscriber = None
    st.session_state.topics = set()
    
    # Load any saved messages
    try:
        if os.path.exists(MESSAGES_FILE) and os.path.getsize(MESSAGES_FILE) > 0:
            with open(MESSAGES_FILE, 'r') as f:
                try:
                    st.session_state.messages = json.load(f)
                    for msg in st.session_state.messages:
                        if 'topic' in msg:
                            st.session_state.topics.add(msg['topic'])
                    logger.info(f"Loaded {len(st.session_state.messages)} messages from file")
                except json.JSONDecodeError as e:
                    logger.error(f"Error parsing JSON file: {e}")
                    # Create a new empty file
                    with open(MESSAGES_FILE, 'w') as f:
                        json.dump([], f)
    except Exception as e:
        logger.error(f"Error loading messages: {e}")
    
    logger.info("Session state initialized")

def on_message(topic, message):
    """Callback function for when a message is received"""
    try:
        timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
        
        # Convert message to string if it's bytes
        if isinstance(message, bytes):
            try:
                message = message.decode('utf-8')
            except:
                message = str(message)
        
        # Convert message to string if it's a dict or other object
        if isinstance(message, dict):
            try:
                message = json.dumps(message, indent=2)
            except:
                message = str(message)
        elif not isinstance(message, str):
            message = str(message)
        
        logger.info(f"Received message on topic {topic}: {message[:100]}...")
        
        # Create the message object
        msg = {
            'topic': topic,
            'message': message,
            'timestamp': timestamp
        }
        
        # Add to queue
        message_queue.put(msg)
        
        # Also save directly to file for persistence
        try:
            # Load existing messages
            messages = []
            if os.path.exists(MESSAGES_FILE) and os.path.getsize(MESSAGES_FILE) > 0:
                try:
                    with open(MESSAGES_FILE, 'r') as f:
                        messages = json.load(f)
                except json.JSONDecodeError:
                    # If file is corrupted, start with empty list
                    messages = []
            
            # Add new message
            messages.append(msg)
            
            # Save back to file
            with open(MESSAGES_FILE, 'w') as f:
                json.dump(messages, f)
        except Exception as e:
            logger.error(f"Error saving message to file: {e}")
            
    except Exception as e:
        logger.error(f"Error in on_message callback: {e}")

def connect_to_broker():
    """Connect to the MQTT broker"""
    broker_host = st.session_state.broker_host
    broker_port = st.session_state.broker_port
    client_id = st.session_state.client_id
    clean_session = st.session_state.clean_session
    
    # Create subscriber
    subscriber = MQTTSubscriber(broker_host, broker_port, client_id)
    
    # Connect
    if subscriber.connect(clean_session):
        st.session_state.subscriber = subscriber
        st.session_state.connected = True
        return True
    else:
        return False

def disconnect_from_broker():
    """Disconnect from the MQTT broker"""
    if st.session_state.subscriber:
        st.session_state.subscriber.disconnect()
        st.session_state.connected = False
        st.session_state.subscriber = None

def subscribe_to_topic():
    """Subscribe to a topic"""
    topic = st.session_state.topic
    qos = st.session_state.qos
    
    if st.session_state.subscriber and st.session_state.connected:
        try:
            logger.info(f"Attempting to subscribe to topic: {topic} with QoS: {qos}")
            if st.session_state.subscriber.subscribe(topic, qos, on_message):
                st.success(f"Subscribed to {topic} with QoS {qos}")
                logger.info(f"Successfully subscribed to topic: {topic}")
            else:
                st.error(f"Failed to subscribe to {topic}")
                logger.error(f"Failed to subscribe to topic: {topic}")
        except Exception as e:
            st.error(f"Error subscribing to topic: {str(e)}")
            logger.error(f"Exception during subscription: {str(e)}")
    else:
        st.error("Not connected to broker")
        logger.error("Attempted to subscribe while not connected")

def get_local_ip():
    """Get the local IP address"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except:
        return "127.0.0.1"

# App title
st.title("MQTT Subscriber")
st.write("This app allows you to subscribe to MQTT topics and view messages published to those topics.")

# Sidebar for connection settings
st.sidebar.header("Connection Settings")

# Broker host and port
st.sidebar.text_input("Broker Host", "localhost", key="broker_host")
st.sidebar.number_input("Broker Port", min_value=1, max_value=65535, value=1883, key="broker_port")

# Client ID
client_id = f"streamlit-subscriber-{get_local_ip().replace('.', '-')}"
st.sidebar.text_input("Client ID", client_id, key="client_id")

# Clean session
st.sidebar.checkbox("Clean Session", value=True, key="clean_session")

# Connect/Disconnect button
if not st.session_state.connected:
    if st.sidebar.button("Connect"):
        with st.spinner("Connecting to broker..."):
            if connect_to_broker():
                st.sidebar.success("Connected to broker")
            else:
                st.sidebar.error("Failed to connect to broker")
else:
    if st.sidebar.button("Disconnect"):
        with st.spinner("Disconnecting from broker..."):
            disconnect_from_broker()
            st.sidebar.success("Disconnected from broker")

# Topic subscription
if st.session_state.connected:
    st.sidebar.header("Subscribe to Topic")
    
    # Topic
    st.sidebar.text_input("Topic", "sensor/#", key="topic")
    
    # QoS
    st.sidebar.selectbox("QoS", [0, 1], key="qos")
    
    # Subscribe button
    if st.sidebar.button("Subscribe"):
        subscribe_to_topic()

# Main content
st.header("Received Messages")

# Display connection status
if st.session_state.connected:
    st.success(f"Connected to broker at {st.session_state.broker_host}:{st.session_state.broker_port}")
else:
    st.warning("Not connected to broker")

# Pull messages from the queue and update session state
messages_added = False
while not message_queue.empty():
    try:
        msg = message_queue.get(block=False)
        if msg not in st.session_state.messages:  # Avoid duplicates
            st.session_state.messages.append(msg)
            if 'topic' in msg:
                st.session_state.topics.add(msg['topic'])
            messages_added = True
    except queue.Empty:
        break

# Also check file for new messages (in case they were added by another Streamlit run)
try:
    if os.path.exists(MESSAGES_FILE) and os.path.getsize(MESSAGES_FILE) > 0:
        try:
            with open(MESSAGES_FILE, 'r') as f:
                file_messages = json.load(f)
                
            # Add any messages not already in session state
            for msg in file_messages:
                if msg not in st.session_state.messages:
                    st.session_state.messages.append(msg)
                    if 'topic' in msg:
                        st.session_state.topics.add(msg['topic'])
                    messages_added = True
        except json.JSONDecodeError:
            logger.error("Corrupted JSON file, skipping loading")
except Exception as e:
    logger.error(f"Error loading messages from file: {e}")

# Add a refresh button
if st.button("Refresh Messages"):
    st.rerun()

# Topic filter
if st.session_state.topics:
    selected_topic = st.selectbox("Filter by Topic", list(st.session_state.topics))
else:
    selected_topic = "All"

# Display messages
if st.session_state.messages:
    # Convert messages to DataFrame
    df = pd.DataFrame(st.session_state.messages)
    
    # Filter by topic if selected
    if selected_topic != "All":
        df = df[df['topic'] == selected_topic]
    
    # Display as table
    st.dataframe(df[['timestamp', 'topic', 'message']], use_container_width=True)
else:
    st.info("No messages received yet")

# Add a clear messages button
if st.session_state.messages and st.button("Clear All Messages"):
    st.session_state.messages = []
    st.session_state.topics = set()
    if os.path.exists(MESSAGES_FILE):
        os.remove(MESSAGES_FILE)
    st.rerun()