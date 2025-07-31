#!/usr/bin/env python3

import os
import json
import subprocess
import sys
import pulsar
import re
from pathlib import Path

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

PULSAR_URL = config['pulsar']['url']
CAPTURE_DIR = config['backup']['capture_dir']
DOCKER_CONTAINER = config['docker']['container']
MAX_MESSAGES_PER_TOPIC = config['backup']['max_messages_per_topic']
TIMEOUT_MS = config['pulsar']['timeout_ms']
RECEIVER_QUEUE_SIZE = config['pulsar']['receiver_queue_size']
SYSTEM_TENANTS = config['system_resources']['tenants']
SYSTEM_NAMESPACES = config['system_resources']['namespaces']

# Create backup directory structure
Path(f"{CAPTURE_DIR}/messages").mkdir(parents=True, exist_ok=True)

def run_command(cmd, capture_output=True):
    """Run a shell command and return its output"""
    result = subprocess.run(cmd, shell=True, text=True, capture_output=capture_output)
    if result.returncode != 0:
        print(f"Error executing command: {cmd}")
        print(f"Error output: {result.stderr}")
        return None
    return result.stdout.strip() if capture_output else None

def check_pulsar():
    """Check if Pulsar is running and accessible"""
    result = run_command("pulsarctl cluster list", capture_output=True)
    if result is None:
        print("Error: Pulsar is not running or not accessible.")
        sys.exit(1)
    return True

def is_partition_topic(topic):
    """Check if a topic is a partition topic"""
    return bool(re.search(r'-partition-\d+$', topic))

def capture_pulsar():
    """Capture tenants, namespaces, topics, and messages"""
    check_pulsar()
    
    # Capture tenants
    print("Capturing tenants...")
    tenant_output = run_command("pulsarctl tenants list")
    tenants = []
    for line in tenant_output.splitlines()[3:-1]:  # Skip header and footer lines
        tenants.append(line.split()[1])
    
    with open(f"{CAPTURE_DIR}/tenants.txt", "w") as f:
        f.write("\n".join(tenants))
    
    print("Captured tenants:")
    print("\n".join(tenants))
    
    # Capture namespaces
    print("Capturing namespaces...")
    namespaces = []
    for tenant in tenants:
        namespace_output = run_command(f"pulsarctl namespaces list {tenant}")
        if namespace_output:
            for line in namespace_output.splitlines()[3:-1]:
                namespaces.append(line.split()[1])
    
    with open(f"{CAPTURE_DIR}/namespaces.txt", "w") as f:
        f.write("\n".join(namespaces))
    
    # Capture topics
    print("Capturing topics...")
    all_topics = []
    for namespace in namespaces:
        topic_output = run_command(f"pulsarctl topics list {namespace}")
        if topic_output:
            for line in topic_output.splitlines()[3:-1]:
                all_topics.append(line.split()[1])
    
    # Filter out partition topics to avoid duplication
    filtered_topics = [topic for topic in all_topics if not is_partition_topic(topic)]
    
    print(f"Found {len(all_topics)} total topics, filtering to {len(filtered_topics)} non-partition topics")
    
    with open(f"{CAPTURE_DIR}/topics.txt", "w") as f:
        f.write("\n".join(filtered_topics))
    
    # Also save the full list for reference
    with open(f"{CAPTURE_DIR}/all_topics.txt", "w") as f:
        f.write("\n".join(all_topics))
    
    # Capture messages with metadata
    print("Capturing messages...")
    client = pulsar.Client(PULSAR_URL)
    
    for topic in filtered_topics:
        topic_safe = topic.replace("/", "_")
        print(f"Capturing messages from {topic}...")
        
        # Use a reader instead of a consumer to ensure messages aren't consumed
        reader = client.create_reader(
            topic,
            pulsar.MessageId.earliest,
            receiver_queue_size=RECEIVER_QUEUE_SIZE
        )
        
        messages = []
        # Attempt to read up to MAX_MESSAGES_PER_TOPIC messages
        for _ in range(MAX_MESSAGES_PER_TOPIC):
            try:
                msg = reader.read_next(timeout_millis=TIMEOUT_MS)
                try:
                    # Try to decode as UTF-8 but handle binary data
                    content = msg.data().decode('utf-8')
                except UnicodeDecodeError:
                    # If not valid UTF-8, use base64 encoding
                    import base64
                    content = base64.b64encode(msg.data()).decode('ascii')
                    
                message_data = {
                    "content": content,
                    "binary_encoded": not isinstance(content, str),
                    "properties": msg.properties(),
                    "publish_timestamp": msg.publish_timestamp(),
                    "event_timestamp": msg.event_timestamp(),
                    "partition_key": msg.partition_key()
                }
                messages.append(message_data)
            except Exception as e:
                print(f"  Finished reading messages: {str(e)}")
                break
        
        print(f"  Captured {len(messages)} messages from {topic}")
        
        # Save messages with metadata
        with open(f"{CAPTURE_DIR}/messages/{topic_safe}.json", "w") as f:
            json.dump(messages, f, indent=2)
        
        reader.close()
    
    client.close()
    print("Capture completed.")

def restore_pulsar():
    """Recreate tenants, namespaces, and topics"""
    check_pulsar()
    
    # Restore tenants
    print("Recreating tenants...")
    with open(f"{CAPTURE_DIR}/tenants.txt", "r") as f:
        tenants = f.read().splitlines()
    
    for tenant in tenants:
        print(f"Creating tenant: {tenant}")
        run_command(f"pulsarctl tenants create {tenant} --allowed-clusters standalone", capture_output=False)
    
    # Restore namespaces
    print("Recreating namespaces...")
    with open(f"{CAPTURE_DIR}/namespaces.txt", "r") as f:
        namespaces = f.read().splitlines()
    
    for namespace in namespaces:
        print(f"Creating namespace: {namespace}")
        run_command(f"pulsarctl namespaces create {namespace}", capture_output=False)
    
    # Restore topics
    print("Recreating topics...")
    with open(f"{CAPTURE_DIR}/topics.txt", "r") as f:
        topics = f.read().splitlines()
    
    for topic in topics:
        print(f"Creating topic: {topic}")
        run_command(f"pulsarctl topics create {topic}", capture_output=False)
    
    print("Restore completed.")

def replay_messages():
    """Replay captured messages with their original metadata"""
    check_pulsar()
    print("Replaying messages...")
    
    client = pulsar.Client(PULSAR_URL)
    
    message_files = Path(f"{CAPTURE_DIR}/messages").glob("*.json")
    for message_file in message_files:
        topic_name = str(message_file.stem).replace("_", "/")
        print(f"Replaying messages to {topic_name}")
        
        producer = client.create_producer(topic_name)
        
        with open(message_file, "r") as f:
            messages = json.load(f)
        
        print(f"  Found {len(messages)} messages to replay")
        
        for msg in messages:
            # Handle binary data if it was encoded
            if msg.get("binary_encoded", False):
                import base64
                content = base64.b64decode(msg["content"])
            else:
                content = msg["content"].encode('utf-8')
                
            # Create a message with the same properties as the original
            producer.send(
                content,
                properties=msg["properties"],
                event_timestamp=msg.get("event_timestamp", 0),
                partition_key=msg.get("partition_key", None)
            )
        
        print(f"  Replayed {len(messages)} messages to {topic_name}")
        producer.close()
    
    client.close()
    print("Replay completed.")

def delete_pulsar_resources():
    """Delete all topics, namespaces, and tenants from Pulsar"""
    check_pulsar()
    
    # Confirm deletion
    confirm = input("WARNING: This will delete ALL topics, namespaces, and tenants from Pulsar.\nType 'DELETE' to confirm: ")
    if confirm != "DELETE":
        print("Deletion cancelled.")
        return
    
    # Get all tenants
    print("Finding resources to delete...")
    tenant_output = run_command("pulsarctl tenants list")
    if not tenant_output:
        print("No tenants found or could not list tenants.")
        return
    
    tenants = []
    for line in tenant_output.splitlines()[3:-1]:
        tenants.append(line.split()[1])
    
    # Skip system tenants
    user_tenants = [t for t in tenants if t not in SYSTEM_TENANTS]
    
    print(f"Found {len(user_tenants)} non-system tenants: {', '.join(user_tenants)}")
    
    # For each tenant, get namespaces
    all_namespaces = []
    for tenant in user_tenants:
        namespace_output = run_command(f"pulsarctl namespaces list {tenant}")
        if namespace_output:
            for line in namespace_output.splitlines()[3:-1]:
                all_namespaces.append(line.split()[1])
    
    # Skip system namespaces
    user_namespaces = [ns for ns in all_namespaces if ns not in SYSTEM_NAMESPACES]
    
    print(f"Found {len(user_namespaces)} non-system namespaces")
    
    # For each namespace, get topics
    all_topics = []
    for namespace in user_namespaces:
        topic_output = run_command(f"pulsarctl topics list {namespace}")
        if topic_output:
            for line in topic_output.splitlines()[3:-1]:
                all_topics.append(line.split()[1])
    
    print(f"Found {len(all_topics)} topics to delete")
    
    # Delete topics first
    print("Deleting topics...")
    for topic in all_topics:
        print(f"  Deleting topic: {topic}")
        # Remove the -f flag as it's not supported
        run_command(f"pulsarctl topics delete {topic}", capture_output=False)
    
    # Delete namespaces
    print("Deleting namespaces...")
    for namespace in user_namespaces:
        print(f"  Deleting namespace: {namespace}")
        # Remove the -f flag as it's not supported
        run_command(f"pulsarctl namespaces delete {namespace}", capture_output=False)
    
    # Delete tenants
    print("Deleting tenants...")
    for tenant in user_tenants:
        print(f"  Deleting tenant: {tenant}")
        run_command(f"pulsarctl tenants delete {tenant}", capture_output=False)
    
    print("Deletion completed. System tenants and namespaces were preserved.")

def print_all_messages():
    """Print all messages from all topics"""
    check_pulsar()
    
    # Get all tenants
    print("Finding tenants...")
    tenant_output = run_command("pulsarctl tenants list")
    tenants = []
    for line in tenant_output.splitlines()[3:-1]:  # Skip header and footer lines
        tenants.append(line.split()[1])
    
    # Get namespaces
    print("Finding namespaces...")
    namespaces = []
    for tenant in tenants:
        namespace_output = run_command(f"pulsarctl namespaces list {tenant}")
        if namespace_output:
            for line in namespace_output.splitlines()[3:-1]:
                namespaces.append(line.split()[1])
    
    # Get topics
    print("Finding topics...")
    all_topics = []
    for namespace in namespaces:
        topic_output = run_command(f"pulsarctl topics list {namespace}")
        if topic_output:
            for line in topic_output.splitlines()[3:-1]:
                all_topics.append(line.split()[1])
    
    # Filter out partition topics to avoid duplication
    filtered_topics = [topic for topic in all_topics if not is_partition_topic(topic)]
    
    print(f"Found {len(all_topics)} total topics, filtering to {len(filtered_topics)} non-partition topics")
    
    # Print messages
    print("Reading and printing messages...")
    client = pulsar.Client(PULSAR_URL)
    
    for topic in filtered_topics:
        print(f"\n=== TOPIC: {topic} ===")
        
        # Use a reader instead of a consumer to ensure messages aren't consumed
        reader = client.create_reader(
            topic,
            pulsar.MessageId.earliest,
            receiver_queue_size=RECEIVER_QUEUE_SIZE
        )
        
        message_count = 0
        
        # Attempt to read up to MAX_MESSAGES_PER_TOPIC messages
        for _ in range(MAX_MESSAGES_PER_TOPIC):
            try:
                msg = reader.read_next(timeout_millis=TIMEOUT_MS)
                try:
                    # Try to decode as UTF-8 but handle binary data
                    content = msg.data().decode('utf-8')
                    binary_encoded = False
                except UnicodeDecodeError:
                    # If not valid UTF-8, use base64 encoding
                    import base64
                    content = base64.b64encode(msg.data()).decode('ascii')
                    binary_encoded = True
                
                message_count += 1
                print(f"\nMessage #{message_count}")
                print(f"Content: {content}")
                if binary_encoded:
                    print("(Content is base64-encoded binary data)")
                if msg.properties():
                    print(f"Properties: {json.dumps(msg.properties(), indent=2)}")
                print(f"Publish timestamp: {msg.publish_timestamp()}")
                if msg.event_timestamp():
                    print(f"Event timestamp: {msg.event_timestamp()}")
                if msg.partition_key():
                    print(f"Partition key: {msg.partition_key()}")
            except Exception as e:
                if message_count == 0:
                    print(f"  No messages found: {str(e)}")
                else:
                    print(f"  Finished reading messages: {str(e)}")
                break
        
        print(f"\nTotal messages read from {topic}: {message_count}")
        reader.close()
    
    client.close()
    print("\nPrinting completed.")

def main():
    """Main menu function"""
    print("Choose an option:")
    print("1. Capture Pulsar tenants, namespaces, topics, and messages")
    print("2. Restore Pulsar tenants, namespaces, and topics")
    print("3. Replay captured messages")
    print("4. Delete ALL Pulsar resources (topics, namespaces, tenants)")
    print("5. Print all messages in all topics")
    
    choice = input("Enter choice (1/2/3/4/5): ")
    
    if choice == "1":
        capture_pulsar()
    elif choice == "2":
        restore_pulsar()
    elif choice == "3":
        replay_messages()
    elif choice == "4":
        delete_pulsar_resources()
    elif choice == "5":
        print_all_messages()
    else:
        print("Invalid choice. Exiting.")

if __name__ == "__main__":
    main() 