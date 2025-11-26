#!/usr/bin/env python3
"""
Monitor Shard Assignments Script

Displays current shard-to-worker assignments from DynamoDB, refreshing every N seconds.
Press Ctrl+C to stop.
"""

import argparse
import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional

try:
    import boto3
    from boto3.dynamodb.types import TypeDeserializer
except ImportError:
    print("Error: boto3 is required. Install with: pip install boto3")
    sys.exit(1)

try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False


# DynamoDB attribute keys (matching Go constants)
LEASE_KEY_KEY = "ShardID"
LEASE_OWNER_KEY = "AssignedTo"
STICKY_OWNER_KEY = "StickyOwner"
SEQUENCE_NUMBER_KEY = "Checkpoint"
LEASE_TIMEOUT_KEY = "LeaseTimeout"


def clear_screen():
    """Clear the terminal screen."""
    os.system('clear' if os.name != 'nt' else 'cls')


def format_checkpoint(checkpoint: Optional[str]) -> str:
    """Format checkpoint value for display."""
    if not checkpoint:
        return "-"
    if checkpoint == "SHARD_END":
        return "SHARD_END"
    # Truncate long checkpoint values
    return checkpoint[:20] + "..." if len(checkpoint) > 20 else checkpoint


def format_lease_timeout(timeout: Optional[str]) -> str:
    """Format lease timeout for display."""
    if not timeout:
        return "-"
    try:
        # Parse RFC3339Nano format
        dt = datetime.fromisoformat(timeout.replace('Z', '+00:00'))
        now = datetime.now(dt.tzinfo)
        diff = (dt - now).total_seconds()
        
        if diff < 0:
            return f"EXPIRED ({abs(int(diff))}s ago)"
        else:
            return f"{int(diff)}s"
    except:
        return timeout[:20]


def scan_dynamodb_table(dynamodb_client, table_name: str) -> List[Dict]:
    """Scan DynamoDB table and return lease information."""
    deserializer = TypeDeserializer()
    
    try:
        response = dynamodb_client.scan(
            TableName=table_name,
            ProjectionExpression=f"{LEASE_KEY_KEY},{LEASE_OWNER_KEY},{STICKY_OWNER_KEY},{SEQUENCE_NUMBER_KEY},{LEASE_TIMEOUT_KEY}",
            Select='SPECIFIC_ATTRIBUTES'
        )
        
        items = []
        for item in response.get('Items', []):
            # Deserialize DynamoDB item
            deserialized = {k: deserializer.deserialize(v) for k, v in item.items()}
            items.append(deserialized)
        
        return items
    except Exception as e:
        print(f"Error scanning table: {e}")
        return []


def display_shard_assignments(items: List[Dict], use_tabulate: bool = True):
    """Display shard assignments in a formatted table."""
    if not items:
        print("No shard assignments found.")
        return
    
    # Sort by ShardID
    items.sort(key=lambda x: x.get(LEASE_KEY_KEY, ""))
    
    # Prepare table data
    headers = ["ShardID", "Worker", "StickyOwner", "Checkpoint", "LeaseTimeout"]
    rows = []
    
    for item in items:
        shard_id = item.get(LEASE_KEY_KEY, "-")
        worker = item.get(LEASE_OWNER_KEY, "-")
        sticky_owner = item.get(STICKY_OWNER_KEY, "-")
        checkpoint = format_checkpoint(item.get(SEQUENCE_NUMBER_KEY))
        lease_timeout = format_lease_timeout(item.get(LEASE_TIMEOUT_KEY))
        
        # Highlight sticky shards
        if sticky_owner != "-":
            if use_tabulate:
                sticky_owner = f"★ {sticky_owner}"
            else:
                sticky_owner = f"* {sticky_owner}"
        
        rows.append([shard_id, worker, sticky_owner, checkpoint, lease_timeout])
    
    # Display table
    if use_tabulate and HAS_TABULATE:
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        # Simple text-based table
        # Calculate column widths
        col_widths = [len(h) for h in headers]
        for row in rows:
            for i, cell in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(cell)))
        
        # Print header
        header_line = " | ".join(h.ljust(col_widths[i]) for i, h in enumerate(headers))
        print(header_line)
        print("-" * len(header_line))
        
        # Print rows
        for row in rows:
            row_line = " | ".join(str(cell).ljust(col_widths[i]) for i, cell in enumerate(row))
            print(row_line)
    
    # Print summary
    sticky_count = sum(1 for item in items if item.get(STICKY_OWNER_KEY))
    print(f"\nTotal shards: {len(items)} | Sticky assignments: {sticky_count}")


def monitor_loop(args):
    """Main monitoring loop."""
    # Create DynamoDB client
    dynamodb = boto3.client(
        'dynamodb',
        endpoint_url=args.endpoint_url,
        region_name=args.region,
        aws_access_key_id='test',
        aws_secret_access_key='test'
    )
    
    print(f"Monitoring DynamoDB table: {args.table_name}")
    print(f"Region: {args.region}")
    print(f"Endpoint: {args.endpoint_url}")
    print(f"Refresh interval: {args.refresh_interval}s")
    print("\nPress Ctrl+C to stop...\n")
    
    try:
        while True:
            clear_screen()
            
            # Display timestamp
            print(f"Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            
            # Scan and display
            items = scan_dynamodb_table(dynamodb, args.table_name)
            display_shard_assignments(items, use_tabulate=HAS_TABULATE)
            
            # Wait for next refresh
            time.sleep(args.refresh_interval)
    
    except KeyboardInterrupt:
        print("\n\nMonitoring stopped.")
        sys.exit(0)


def main():
    parser = argparse.ArgumentParser(
        description='Monitor shard-to-worker assignments from DynamoDB',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--table-name',
        default='appName',
        help='DynamoDB table name'
    )
    
    parser.add_argument(
        '--region',
        default='us-west-2',
        help='AWS region'
    )
    
    parser.add_argument(
        '--endpoint-url',
        default='http://localhost:4566',
        help='DynamoDB endpoint URL (for LocalStack)'
    )
    
    parser.add_argument(
        '--refresh-interval',
        type=int,
        default=10,
        help='Refresh interval in seconds'
    )
    
    args = parser.parse_args()
    
    if not HAS_TABULATE:
        print("Note: Install 'tabulate' for better table formatting: pip install tabulate\n")
    
    monitor_loop(args)


if __name__ == '__main__':
    main()

