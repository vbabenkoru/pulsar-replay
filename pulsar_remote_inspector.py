#!/usr/bin/env python3
"""
Pulsar Remote Inspector & Message Publisher - Connect to remote Pulsar server and publish messages
"""

import json
import os
import yaml
import requests
import uuid
import time
import random
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional
import pulsar


class PulsarRemoteInspector:
    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.expanduser("~/.config/pulsar/config")
        self.config = self._load_config()
        self.current_context = self.config.get('current-context')
        self.auth_token = None
        self.client = None
        self.producer = None
        
        # Message generation settings
        self.user_domains = ['@test.com', '@iterable.com', '@example.com']
        self.user_prefixes = ['user', 'test', 'john', 'jane', 'alex', 'sam', 'chris', 'taylor']
        self.campaign_ids = [1, 2, 3, 4, 5]  # Will be configurable
        self.template_ids = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
        
    def _load_config(self) -> Dict:
        """Load and parse Pulsar configuration"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            raise Exception(f"Failed to load config from {self.config_path}: {e}")

    def _get_oauth_token(self) -> str:
        """Get OAuth token using client credentials flow"""
        if not self.current_context:
            raise Exception("No current context set in config")
            
        auth_info = self.config['auth-info'].get(self.current_context, {})
        
        if not auth_info.get('issuer_endpoint'):
            print(f"No OAuth config for context '{self.current_context}', trying without auth...")
            return None
            
        # Read service account credentials
        key_file = auth_info.get('key_file')
        if key_file and os.path.exists(key_file):
            try:
                with open(key_file, 'r') as f:
                    service_account = json.loads(f.read().strip())
            except Exception as e:
                print(f"Failed to parse service account file: {e}")
                return None
        else:
            print(f"Warning: Key file not found at {key_file}")
            return None

        # OAuth token request using client_secret
        token_url = f"{auth_info['issuer_endpoint'].rstrip('/')}/oauth/token"
        
        payload = {
            'grant_type': 'client_credentials',
            'client_id': service_account.get('client_id', auth_info['client_id']),
            'client_secret': service_account.get('client_secret'),
            'audience': auth_info['audience'],
        }
        
        try:
            response = requests.post(token_url, data=payload)
            response.raise_for_status()
            token_data = response.json()
            return token_data.get('access_token')
        except Exception as e:
            print(f"Failed to get OAuth token: {e}")
            return None

    def _get_admin_url(self) -> str:
        """Get admin service URL for current context"""
        contexts = self.config.get('contexts', {})
        context_info = contexts.get(self.current_context, {})
        return context_info.get('admin-service-url')

    def _make_admin_request(self, endpoint: str) -> Optional[Dict]:
        """Make authenticated request to admin API"""
        admin_url = self._get_admin_url()
        if not admin_url:
            raise Exception(f"No admin URL found for context '{self.current_context}'")
            
        url = f"{admin_url.rstrip('/')}/{endpoint.lstrip('/')}"
        headers = {}
        
        # Add auth header if we have a token
        if self.auth_token:
            headers['Authorization'] = f'Bearer {self.auth_token}'
            
        try:
            response = requests.get(url, headers=headers, timeout=3)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if hasattr(e, 'response') and e.response.status_code == 404:
                pass  # Silently ignore 404s (empty namespaces)
            else:
                print(f"Request failed for {url}: {e}")
            return None

    def connect(self) -> bool:
        """Establish connection and authenticate"""
        print(f"Connecting to Pulsar context: {self.current_context}")
        print(f"Admin URL: {self._get_admin_url()}")
        
        # Get OAuth token if needed
        self.auth_token = self._get_oauth_token()
        if self.auth_token:
            print("✓ Authentication successful")
        else:
            print("⚠ No authentication token (proceeding without auth)")
        
        # Test connection
        try:
            result = self._make_admin_request('admin/v2/clusters')
            if result is not None:
                print("✓ Connection successful")
                return True
        except Exception as e:
            print(f"✗ Connection failed: {e}")
            
        return False

    def _get_pulsar_client_url(self) -> str:
        """Get Pulsar service URL for current context"""
        contexts = self.config.get('contexts', {})
        context_info = contexts.get(self.current_context, {})
        
        # Try broker-service-url first, then bookie-service-url, then fallback to admin URL
        broker_url = context_info.get('broker-service-url')
        if broker_url:
            return broker_url
            
        bookie_url = context_info.get('bookie-service-url')
        if bookie_url:
            return bookie_url
            
        # Fallback to admin URL with port change
        admin_url = context_info.get('admin-service-url', '')
        if admin_url.startswith('https://'):
            return admin_url.replace('https://', 'pulsar+ssl://').replace(':8080', ':6651') + ':6651'
        else:
            return admin_url.replace('8080', '6650')

    def connect_producer(self, topic: str) -> bool:
        """Connect Pulsar producer to topic"""
        try:
            service_url = self._get_pulsar_client_url()
            print(f"Connecting to Pulsar: {service_url}")
            
            # Create client with auth if available
            client_config = {'service_url': service_url}
            if self.auth_token:
                client_config['authentication'] = pulsar.AuthenticationToken(self.auth_token)
                
            self.client = pulsar.Client(**client_config)
            
            # Create producer
            self.producer = self.client.create_producer(
                topic=topic,
                batching_enabled=True,
                batching_max_messages=100,
                batching_max_publish_delay_ms=50
            )
            
            print(f"✓ Producer connected to topic: {topic}")
            return True
            
        except Exception as e:
            print(f"✗ Failed to connect producer: {e}")
            return False

    def extract_project_id_from_topic(self, topic: str) -> Optional[int]:
        """Extract project ID from topic name pattern like post-ingestion-495"""
        import re
        # Match patterns like "post-ingestion-123" or "ingestion-456" 
        match = re.search(r'(?:post-)?ingestion-(\d+)', topic)
        if match:
            return int(match.group(1))
        return None

    def generate_campaign_range(self, start: int, count: int) -> List[int]:
        """Generate range of campaign IDs"""
        return list(range(start, start + count))

    def generate_user_key(self) -> str:
        """Generate random user key/email"""
        prefix = random.choice(self.user_prefixes)
        suffix = random.randint(1, 9999)
        domain = random.choice(self.user_domains)
        return f"{prefix}+{suffix}{domain}"

    def generate_emailsend_message(self, project_id: int = 1) -> Dict:
        """Generate emailSend message based on prototype"""
        now = datetime.now(timezone.utc)
        event_id = str(uuid.uuid4())
        user_key = self.generate_user_key()
        message_id = uuid.uuid4().hex
        
        return {
            "eventId": event_id,
            "correlationId": event_id,
            "createdAt": now.isoformat().replace('+00:00', 'Z'),
            "payloadVersion": 1,
            "payloadType": "UpdateEvent",
            "payload": {
                "projectId": project_id,
                "userKey": user_key,
                "docType": "emailSend",
                "metadata": {
                    "telemetry": {
                        "ingestRequestTime": now.isoformat().replace('+00:00', 'Z'),
                        "ingestStartTime": now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "Z",
                        "ingestFinishTime": now.isoformat().replace('+00:00', 'Z')
                    },
                    "esContext": {
                        "documentId": uuid.uuid4().hex,
                        "unconvertedDocumentId": uuid.uuid4().hex,
                        "createdAt": now.strftime("%Y-%m-%d %H:%M:%S +00:00"),
                        "updatedAt": now.strftime("%Y-%m-%d %H:%M:%S +00:00")
                    },
                    "source": {
                        "action": "NoOp"
                    }
                },
                "data": {
                    "data": {},
                    "diff": {
                        "templateId": random.choice(self.template_ids),
                        "campaignId": random.choice(self.campaign_ids),
                        "email": user_key,
                        "messageId": message_id,
                        "itblInternal": {
                            "documentCreatedAt": now.strftime("%Y-%m-%d %H:%M:%S +00:00"),
                            "documentUpdatedAt": now.strftime("%Y-%m-%d %H:%M:%S +00:00")
                        },
                        "createdAt": now.strftime("%Y-%m-%d %H:%M:%S +00:00")
                    }
                }
            }
        }

    async def publish_messages(self, topic: str, count: int, rate_per_second: int = 1000, 
                              project_id: int = None, campaign_ids: List[int] = None,
                              campaign_start: int = None, campaign_count: int = None,
                              auto_detect_project: bool = True) -> None:
        """Publish messages at specified rate"""
        
        # Auto-detect project ID from topic if requested and not explicitly set
        if auto_detect_project and project_id is None:
            detected_id = self.extract_project_id_from_topic(topic)
            if detected_id:
                project_id = detected_id
                print(f"🎯 Auto-detected project ID: {project_id} from topic: {topic}")
            else:
                project_id = 1
                print(f"⚠️  Could not auto-detect project ID from topic, using default: {project_id}")
        elif project_id is None:
            project_id = 1
            
        # Set up campaign IDs
        if campaign_ids:
            self.campaign_ids = campaign_ids
        elif campaign_start is not None and campaign_count is not None:
            self.campaign_ids = self.generate_campaign_range(campaign_start, campaign_count)
            print(f"📊 Generated campaign range: {campaign_start} to {campaign_start + campaign_count - 1}")
            
        if not self.connect_producer(topic):
            return
            
        print(f"🚀 Publishing {count} messages at {rate_per_second}/sec to {topic}")
        print(f"📋 Project ID: {project_id}")
        print(f"🎯 Campaign IDs: {self.campaign_ids} ({len(self.campaign_ids)} campaigns)")
        print(f"📧 Template IDs: {self.template_ids} ({len(self.template_ids)} templates)")
        
        batch_size = min(100, rate_per_second // 10)  # 10 batches per second max
        delay_between_batches = batch_size / rate_per_second
        
        sent = 0
        start_time = time.time()
        
        try:
            while sent < count:
                batch_start = time.time()
                
                # Send batch
                for _ in range(min(batch_size, count - sent)):
                    message = self.generate_emailsend_message(project_id)
                    self.producer.send_async(
                        json.dumps(message).encode('utf-8'),
                        callback=None
                    )
                    sent += 1
                
                # Progress update
                if sent % 500 == 0 or sent == count:
                    elapsed = time.time() - start_time
                    rate = sent / elapsed if elapsed > 0 else 0
                    print(f"Sent {sent}/{count} messages (rate: {rate:.1f}/sec)")
                
                # Rate limiting
                batch_time = time.time() - batch_start
                if batch_time < delay_between_batches:
                    await asyncio.sleep(delay_between_batches - batch_time)
                    
        except KeyboardInterrupt:
            print(f"\nStopped. Sent {sent} messages.")
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.client:
                self.client.close()
                
        elapsed = time.time() - start_time
        final_rate = sent / elapsed if elapsed > 0 else 0
        print(f"Completed! Sent {sent} messages in {elapsed:.1f}s (avg rate: {final_rate:.1f}/sec)")

    def close(self) -> None:
        """Clean up connections"""
        if self.producer:
            self.producer.close()
        if self.client:
            self.client.close()

    def list_tenants(self) -> List[str]:
        """List all tenants"""
        print("\n=== TENANTS ===")
        result = self._make_admin_request('admin/v2/tenants')
        
        if result is None:
            print("Failed to fetch tenants")
            return []
            
        tenants = result if isinstance(result, list) else []
        
        for tenant in tenants:
            print(f"  • {tenant}")
            
        print(f"Total: {len(tenants)} tenants")
        return tenants

    def list_namespaces(self, tenant: str = None) -> List[str]:
        """List namespaces for a tenant or all namespaces"""
        print(f"\n=== NAMESPACES{f' ({tenant})' if tenant else ''} ===")
        
        if tenant:
            endpoint = f'admin/v2/namespaces/{tenant}'
        else:
            # Get all namespaces across all tenants
            tenants = self._make_admin_request('admin/v2/tenants')
            if not tenants:
                return []
                
            all_namespaces = []
            for t in tenants:
                ns_result = self._make_admin_request(f'admin/v2/namespaces/{t}')
                if ns_result:
                    all_namespaces.extend(ns_result)
            
            for ns in all_namespaces:
                print(f"  • {ns}")
            print(f"Total: {len(all_namespaces)} namespaces")
            return all_namespaces
            
        result = self._make_admin_request(endpoint)
        if result is None:
            print(f"Failed to fetch namespaces for tenant: {tenant}")
            return []
            
        namespaces = result if isinstance(result, list) else []
        
        for ns in namespaces:
            print(f"  • {ns}")
            
        print(f"Total: {len(namespaces)} namespaces")
        return namespaces

    def list_topics(self, namespace: str = None, tenant: str = None, limit: int = 50) -> List[str]:
        """List topics in a namespace or all topics"""
        scope = namespace or (f"tenant {tenant}" if tenant else f"first {limit}")
        print(f"\n=== TOPICS{f' ({scope})' if scope else ''} ===")
        
        if namespace:
            # Get comprehensive topic list using the essential Pulsar Admin REST APIs
            # Based on https://pulsar.apache.org/docs/next/reference-rest-api-overview/
            combined_topics = []
            
            # 1. Get basic topics
            base_result = self._make_admin_request(f'admin/v2/namespaces/{namespace}/topics')
            if base_result:
                combined_topics.extend(base_result)
            
            # 2. Get partitioned topics (this was the key missing piece that pulsarctl uses!)
            partitioned_result = self._make_admin_request(f'admin/v2/persistent/{namespace}/partitioned')
            if partitioned_result:
                for topic in partitioned_result:
                    if topic not in combined_topics:
                        combined_topics.append(topic)
            
            # 3. Include system topics (__change_events, __transaction_buffer_snapshot, etc.)
            system_result = self._make_admin_request(f'admin/v2/namespaces/{namespace}/topics?includeSystemTopic=true')
            if system_result:
                for topic in system_result:
                    if topic not in combined_topics:
                        combined_topics.append(topic)
            
            if not combined_topics:
                print(f"No topics found for namespace: {namespace}")
                return []
            
            individual_topics = combined_topics
            print(f"Found {len(individual_topics)} total topics")
            
            # Separate partitioned topics from individual topics
            partitioned_topics = set()
            non_partitioned_topics = []
            
            for topic in individual_topics:
                # Check if this is a partition (ends with -partition-N)
                if '-partition-' in topic:
                    # Extract the parent partitioned topic name
                    parent_topic = topic.rsplit('-partition-', 1)[0]
                    partitioned_topics.add(parent_topic)
                else:
                    # This is a non-partitioned topic
                    non_partitioned_topics.append(topic)
            
            # Convert set to sorted list
            partitioned_topics = sorted(list(partitioned_topics))
            non_partitioned_topics.sort()
            
            print(f"\n=== PARTITIONED TOPICS ===")
            for topic in partitioned_topics:
                print(f"  • {topic} (partitioned)")
            
            print(f"\n=== NON-PARTITIONED TOPICS ===")
            for topic in non_partitioned_topics:
                print(f"  • {topic}")
            
            all_topics = partitioned_topics + non_partitioned_topics
            print(f"\nTotal: {len(all_topics)} topics ({len(partitioned_topics)} partitioned, {len(non_partitioned_topics)} non-partitioned)")
            return all_topics
        else:
            # Get topics from specified tenant or all tenants
            if tenant:
                # Only scan the specified tenant
                tenants_to_scan = [tenant]
                print(f"Scanning tenant: {tenant}")
            else:
                # Get all tenants and prioritize
                tenants = self._make_admin_request('admin/v2/tenants')
                if not tenants:
                    return []
                tenants_to_scan = tenants
                print(f"Scanning priority namespaces first...")
                
            all_topics = []
            count = 0
            
            # Priority patterns - check these first as they're more likely to have topics
            priority_patterns = ['org-1', 'global', 'dlq']
            
            for tenant_name in tenants_to_scan:
                if count >= limit:
                    break
                ns_result = self._make_admin_request(f'admin/v2/namespaces/{tenant_name}')
                if not ns_result:
                    continue
                    
                # First pass: priority namespaces
                priority_namespaces = [ns for ns in ns_result if any(pattern in ns for pattern in priority_patterns)]
                
                for ns in priority_namespaces:
                    if count >= limit:
                        break
                    print(f"  Checking {ns}...")
                    # Use the same comprehensive API approach as single namespace
                    ns_topics = []
                    
                    # Get basic topics
                    base_result = self._make_admin_request(f'admin/v2/namespaces/{ns}/topics')
                    if base_result:
                        ns_topics.extend(base_result)
                    
                    # Get partitioned topics
                    partitioned_result = self._make_admin_request(f'admin/v2/persistent/{ns}/partitioned')
                    if partitioned_result:
                        for topic in partitioned_result:
                            if topic not in ns_topics:
                                ns_topics.append(topic)
                    
                    # Get system topics
                    system_result = self._make_admin_request(f'admin/v2/namespaces/{ns}/topics?includeSystemTopic=true')
                    if system_result:
                        for topic in system_result:
                            if topic not in ns_topics:
                                ns_topics.append(topic)
                    
                    if ns_topics:
                        # Parse partitioned topics from partitions
                        partitioned_topics_set = set()
                        non_partitioned_topics_list = []
                        
                        for topic in ns_topics:
                            if '-partition-' in topic:
                                parent_topic = topic.rsplit('-partition-', 1)[0]
                                partitioned_topics_set.add(parent_topic)
                            else:
                                non_partitioned_topics_list.append(topic)
                        
                        # Combine and add to results
                        namespace_topics = sorted(list(partitioned_topics_set)) + sorted(non_partitioned_topics_list)
                        added_count = 0
                        for topic in namespace_topics:
                            if count >= limit:
                                break
                            all_topics.append(topic)
                            count += 1
                            added_count += 1
                        if added_count > 0:
                            print(f"    Found {added_count} topics")
                        
            # If we still need more topics, scan remaining namespaces quickly
            if count < limit and not tenant:  # Only do this if scanning all tenants
                print(f"Scanning remaining namespaces... (found {count} so far)")
                for tenant_name in tenants_to_scan[:10]:  # Limit to first 10 tenants to avoid timeout
                    if count >= limit:
                        break
                    ns_result = self._make_admin_request(f'admin/v2/namespaces/{tenant_name}')
                    if not ns_result:
                        continue
                        
                    # Skip priority ones we already checked
                    remaining_namespaces = [ns for ns in ns_result if not any(pattern in ns for pattern in priority_patterns)]
                    
                    for ns in remaining_namespaces[:5]:  # Limit to 5 per tenant
                        if count >= limit:
                            break
                        
                        # Use same comprehensive API approach
                        ns_topics = []
                        base_result = self._make_admin_request(f'admin/v2/namespaces/{ns}/topics')
                        if base_result:
                            ns_topics.extend(base_result)
                        
                        partitioned_result = self._make_admin_request(f'admin/v2/persistent/{ns}/partitioned')
                        if partitioned_result:
                            for topic in partitioned_result:
                                if topic not in ns_topics:
                                    ns_topics.append(topic)
                        
                        system_result = self._make_admin_request(f'admin/v2/namespaces/{ns}/topics?includeSystemTopic=true')
                        if system_result:
                            for topic in system_result:
                                if topic not in ns_topics:
                                    ns_topics.append(topic)
                        
                        if ns_topics:
                            # Parse partitioned topics from partitions
                            partitioned_topics_set = set()
                            non_partitioned_topics_list = []
                            
                            for topic in ns_topics:
                                if '-partition-' in topic:
                                    parent_topic = topic.rsplit('-partition-', 1)[0]
                                    partitioned_topics_set.add(parent_topic)
                                else:
                                    non_partitioned_topics_list.append(topic)
                            
                            # Combine and add to results
                            namespace_topics = sorted(list(partitioned_topics_set)) + sorted(non_partitioned_topics_list)
                            for topic in namespace_topics:
                                if count >= limit:
                                    break
                                all_topics.append(topic)
                                count += 1
                        
            for topic in all_topics:
                print(f"  • {topic}")
            print(f"Total: {len(all_topics)} topics")
            return all_topics


    def inspect_all(self, sample_tenant: str = None, sample_namespace: str = None):
        """Run full inspection of the Pulsar cluster"""
        if not self.connect():
            return
            
        tenants = self.list_tenants()
        
        if sample_tenant and sample_tenant in tenants:
            self.list_namespaces(sample_tenant)
            namespaces = self._make_admin_request(f'admin/v2/namespaces/{sample_tenant}')
            if namespaces and sample_namespace:
                full_ns = f"{sample_tenant}/{sample_namespace}"
                if full_ns in namespaces:
                    self.list_topics(full_ns)
        else:
            self.list_namespaces()
            self.list_topics(tenant=sample_tenant)


def main():
    """Main CLI interface"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Inspect remote Pulsar cluster and publish messages')
    parser.add_argument('--config', help='Path to pulsar config file')
    parser.add_argument('--tenant', help='Specific tenant to inspect')
    parser.add_argument('--namespace', help='Specific namespace to inspect (requires --tenant)')
    parser.add_argument('--topics-limit', type=int, default=50, help='Limit for topics when listing all')
    
    subparsers = parser.add_subparsers(dest='command', help='Commands')
    
    # List commands
    subparsers.add_parser('tenants', help='List tenants')
    subparsers.add_parser('namespaces', help='List namespaces') 
    subparsers.add_parser('topics', help='List topics')
    subparsers.add_parser('all', help='List everything (default)')
    
    # Publish command
    publish_parser = subparsers.add_parser('publish', help='Publish emailSend messages')
    publish_parser.add_argument('topic', help='Topic to publish to')
    publish_parser.add_argument('--count', type=int, default=1000, help='Number of messages to send')
    publish_parser.add_argument('--rate', type=int, default=1000, help='Messages per second')
    publish_parser.add_argument('--project-id', type=int, help='Project ID for messages (auto-detected from topic if not set)')
    publish_parser.add_argument('--no-auto-detect', action='store_true', help='Disable auto-detection of project ID from topic')
    publish_parser.add_argument('--campaign-ids', nargs='+', type=int, 
                              help='Specific campaign IDs to randomly distribute')
    publish_parser.add_argument('--campaign-start', type=int, help='Starting campaign ID for range generation')
    publish_parser.add_argument('--campaign-count', type=int, help='Number of campaign IDs to generate (use with --campaign-start)')
    
    # Generate sample message command
    subparsers.add_parser('sample', help='Generate and print sample emailSend message')
    
    # Show ranges command
    ranges_parser = subparsers.add_parser('ranges', help='Show current ID ranges and test topic parsing')
    ranges_parser.add_argument('--test-topic', help='Test topic name for project ID extraction')
    
    args = parser.parse_args()
    
    inspector = PulsarRemoteInspector(args.config)
    
    if not args.command or args.command == 'all':
        inspector.inspect_all(args.tenant, args.namespace)
    elif args.command == 'tenants':
        if inspector.connect():
            inspector.list_tenants()
    elif args.command == 'namespaces':
        if inspector.connect():
            inspector.list_namespaces(args.tenant)
    elif args.command == 'topics':
        if inspector.connect():
            if args.tenant and args.namespace:
                inspector.list_topics(f"{args.tenant}/{args.namespace}")
            else:
                inspector.list_topics(tenant=args.tenant, limit=args.topics_limit)
    elif args.command == 'publish':
        # Validate campaign arguments
        if args.campaign_start and not args.campaign_count:
            parser.error("--campaign-start requires --campaign-count")
        if args.campaign_count and not args.campaign_start:
            parser.error("--campaign-count requires --campaign-start")
        if args.campaign_ids and (args.campaign_start or args.campaign_count):
            parser.error("Cannot use both --campaign-ids and --campaign-start/--campaign-count")
            
        # Get auth token first
        inspector.auth_token = inspector._get_oauth_token()
        
        # Run async publish
        asyncio.run(inspector.publish_messages(
            topic=args.topic,
            count=args.count,
            rate_per_second=args.rate,
            project_id=args.project_id,
            campaign_ids=args.campaign_ids,
            campaign_start=args.campaign_start,
            campaign_count=args.campaign_count,
            auto_detect_project=not args.no_auto_detect
        ))
    elif args.command == 'sample':
        # Generate and print sample message
        message = inspector.generate_emailsend_message()
        print(json.dumps(message, indent=2))
    elif args.command == 'ranges':
        # Show current ranges and test topic parsing
        print("📊 CURRENT RANGES:")
        print(f"   Campaign IDs: {inspector.campaign_ids} ({len(inspector.campaign_ids)} campaigns)")
        print(f"   Template IDs: {inspector.template_ids} ({len(inspector.template_ids)} templates)")
        print(f"   User domains: {inspector.user_domains}")
        print(f"   User prefixes: {inspector.user_prefixes}")
        
        if args.test_topic:
            print(f"\n🎯 TOPIC PARSING TEST:")
            print(f"   Topic: {args.test_topic}")
            project_id = inspector.extract_project_id_from_topic(args.test_topic)
            if project_id:
                print(f"   ✅ Extracted project ID: {project_id}")
            else:
                print(f"   ❌ Could not extract project ID")
        
        print(f"\n💡 EXAMPLES:")
        print(f"   # Auto-detect project ID and use 20 campaigns starting from 1000:")
        print(f"   python {os.path.basename(__file__)} publish persistent://eventbus/org-1/post-ingestion-495 \\")
        print(f"     --count 100000 --rate 2000 --campaign-start 1000 --campaign-count 20")
        print(f"   ")
        print(f"   # Manual project ID with specific campaigns:")
        print(f"   python {os.path.basename(__file__)} publish persistent://eventbus/org-1/topic \\")
        print(f"     --project-id 123 --campaign-ids 100 200 300 400 500")


if __name__ == '__main__':
    main()