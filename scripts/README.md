# KCL Worker Scripts

This directory contains Python scripts for monitoring and managing KCL workers for testing the sticky shard assignment feature.

## Prerequisites

1. **LocalStack**: Ensure LocalStack is running on `localhost:4566`
   ```bash
   localstack start
   ```

2. **Python Dependencies**: Install required Python packages
   ```bash
   pip install -r requirements.txt
   ```

3. **Build Worker Binary**: Build the Go worker runner
   ```bash
   cd worker_runner
   go build -o worker_runner .
   ```

## Scripts

### 1. monitor_shards.py

Continuously monitors and displays shard-to-worker assignments from DynamoDB.

**Usage:**
```bash
./monitor_shards.py [OPTIONS]
```

**Options:**
- `--table-name`: DynamoDB table name (default: `appName`)
- `--region`: AWS region (default: `us-west-2`)
- `--endpoint-url`: DynamoDB endpoint URL (default: `http://localhost:4566`)
- `--refresh-interval`: Refresh interval in seconds (default: `10`)

**Example:**
```bash
# Monitor with default settings
./monitor_shards.py

# Monitor with custom table and faster refresh
./monitor_shards.py --table-name my-table --refresh-interval 5
```

**Output:**
The script displays a table showing:
- ShardID
- Current Worker (AssignedTo)
- StickyOwner (marked with ★ if set)
- Checkpoint
- Lease Timeout

Press `Ctrl+C` to stop monitoring.

### 2. start_workers.py

Starts multiple KCL worker processes that consume from a Kinesis stream.

**Usage:**
```bash
./start_workers.py [OPTIONS]
```

**Options:**
- `--stream-name`: Kinesis stream name (default: `kcl-test`)
- `--table-name`: DynamoDB table name (default: `appName`)
- `--app-name`: Application name (default: `appName`)
- `--region`: AWS region (default: `us-west-2`)
- `--num-workers`: Number of workers to start (default: `3`)
- `--max-leases`: Maximum leases per worker (default: `2`)

**Example:**
```bash
# Start 3 workers with default settings (MaxLeasesForWorker=2)
./start_workers.py

# Start 3 workers with higher MaxLeasesForWorker
./start_workers.py --max-leases 4

# Start 5 workers with custom stream
./start_workers.py --stream-name my-stream --num-workers 5 --max-leases 3
```

**Output:**
The script displays log output from all workers. Press `Ctrl+C` to gracefully shutdown all workers.

## Testing Sticky Shard Assignment

### Setup

1. Start LocalStack:
   ```bash
   localstack start
   ```

2. Create a Kinesis stream with shards:
   ```bash
   aws --endpoint-url=http://localhost:4566 kinesis create-stream \
     --stream-name kcl-test \
     --shard-count 4 \
     --region us-west-2
   ```

3. Wait for stream to become active:
   ```bash
   aws --endpoint-url=http://localhost:4566 kinesis describe-stream \
     --stream-name kcl-test \
     --region us-west-2
   ```

### Test Scenario: Sticky Assignment

1. Start the monitoring script in one terminal:
   ```bash
   ./monitor_shards.py
   ```

2. Start workers in another terminal:
   ```bash
   ./start_workers.py --num-workers 3
   ```

3. Set a sticky assignment (in a third terminal):
   ```bash
   # Set shard-0 sticky to worker-0
   aws --endpoint-url=http://localhost:4566 dynamodb update-item \
     --table-name appName \
     --key '{"ShardID": {"S": "shardId-000000000000"}}' \
     --update-expression "SET StickyOwner = :owner" \
     --expression-attribute-values '{":owner": {"S": "worker-0"}}' \
     --region us-west-2
   ```

4. Observe in the monitoring terminal:
   - Shard `shardId-000000000000` should show `★ worker-0` in the StickyOwner column
   - Other workers should not claim this shard
   - If worker-0 is stopped, another worker can temporarily take over after the failover timeout

5. Test failover:
   ```bash
   # In the workers terminal, note the PID of worker-0, then:
   kill -INT <worker-0-pid>
   
   # Wait for failover timeout (10 seconds)
   # Watch in monitoring terminal as another worker claims the shard
   
   # Restart worker-0 by starting a new worker process
   # It should reclaim its sticky shard
   ```

### Cleanup

1. Stop workers (`Ctrl+C` in the start_workers.py terminal)
2. Stop monitoring (`Ctrl+C` in the monitor_shards.py terminal)
3. Delete the stream:
   ```bash
   aws --endpoint-url=http://localhost:4566 kinesis delete-stream \
     --stream-name kcl-test \
     --region us-west-2
   ```
4. Delete the DynamoDB table:
   ```bash
   aws --endpoint-url=http://localhost:4566 dynamodb delete-table \
     --table-name appName \
     --region us-west-2
   ```

## Worker Logs

Each worker writes logs to `worker-<id>.log` in the current directory.

## Troubleshooting

### Worker binary not found
```
Error: Worker binary not found
```
**Solution**: Build the worker binary:
```bash
cd worker_runner && go build -o worker_runner .
```

### Connection refused to LocalStack
```
Error scanning table: Connection refused
```
**Solution**: Ensure LocalStack is running:
```bash
localstack status
```

### boto3 not found
```
Error: boto3 is required
```
**Solution**: Install Python dependencies:
```bash
pip install -r requirements.txt
```

