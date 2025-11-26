#!/usr/bin/env python3
"""
Start Workers Script

Starts multiple KCL worker processes that run continuously until Ctrl+C.
"""

import argparse
import os
import signal
import subprocess
import sys
import time
from pathlib import Path


class WorkerManager:
    """Manages multiple worker processes."""
    
    def __init__(self, args):
        self.args = args
        self.processes = []
        self.worker_binary = self._find_worker_binary()
        self.should_stop = False
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _find_worker_binary(self):
        """Find the worker_runner binary."""
        script_dir = Path(__file__).parent
        worker_bin = script_dir / "worker_runner" / "worker_runner"
        
        if not worker_bin.exists():
            print(f"Error: Worker binary not found at {worker_bin}")
            print("Please build the worker_runner first:")
            print("  cd scripts/worker_runner && go build -o worker_runner .")
            sys.exit(1)
        
        return str(worker_bin)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        if not self.should_stop:
            print("\n\nReceived shutdown signal, stopping all workers...")
            self.should_stop = True
            self.stop_all_workers()
    
    def start_worker(self, worker_id):
        """Start a single worker process."""
        cmd = [
            self.worker_binary,
            f"--worker-id={worker_id}",
            f"--stream-name={self.args.stream_name}",
            f"--table-name={self.args.table_name}",
            f"--app-name={self.args.app_name}",
            f"--region={self.args.region}",
            f"--max-leases={self.args.max_leases}",
        ]
        
        print(f"Starting worker: {worker_id}")
        print(f"  Command: {' '.join(cmd)}")
        
        try:
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            self.processes.append({
                'id': worker_id,
                'process': process,
            })
            
            return process
        
        except Exception as e:
            print(f"Error starting worker {worker_id}: {e}")
            return None
    
    def start_all_workers(self):
        """Start all worker processes."""
        print(f"\nStarting {self.args.num_workers} workers...")
        print(f"  Stream: {self.args.stream_name}")
        print(f"  Table: {self.args.table_name}")
        print(f"  Region: {self.args.region}")
        print(f"  App Name: {self.args.app_name}")
        print(f"  Max Leases: {self.args.max_leases}")
        print("\nPress Ctrl+C to stop all workers...\n")
        
        for i in range(self.args.num_workers):
            worker_id = f"worker-{i}"
            self.start_worker(worker_id)
            time.sleep(0.5)  # Small delay between starts
        
        print(f"\nAll {len(self.processes)} workers started.\n")
    
    def monitor_workers(self):
        """Monitor worker output and status."""
        try:
            while not self.should_stop:
                # Check for output from each worker
                for worker_info in self.processes:
                    process = worker_info['process']
                    worker_id = worker_info['id']
                    
                    # Non-blocking read from stdout
                    try:
                        line = process.stdout.readline()
                        if line:
                            print(f"[{worker_id}] {line.rstrip()}")
                    except:
                        pass
                    
                    # Check if process has exited
                    if process.poll() is not None:
                        exit_code = process.returncode
                        print(f"\n[{worker_id}] Worker exited with code {exit_code}")
                        self.should_stop = True
                        break
                
                time.sleep(0.1)
        
        except KeyboardInterrupt:
            pass
    
    def stop_all_workers(self):
        """Stop all worker processes gracefully."""
        if not self.processes:
            return
        
        print("\nStopping workers...")
        
        # Send SIGINT to all workers
        for worker_info in self.processes:
            process = worker_info['process']
            worker_id = worker_info['id']
            
            if process.poll() is None:  # Still running
                try:
                    print(f"  Stopping {worker_id}...")
                    process.send_signal(signal.SIGINT)
                except Exception as e:
                    print(f"  Error stopping {worker_id}: {e}")
        
        # Wait for workers to exit (with timeout)
        timeout = 10
        start_time = time.time()
        
        while time.time() - start_time < timeout:
            all_stopped = True
            for worker_info in self.processes:
                if worker_info['process'].poll() is None:
                    all_stopped = False
                    break
            
            if all_stopped:
                break
            
            time.sleep(0.5)
        
        # Force kill any remaining processes
        for worker_info in self.processes:
            process = worker_info['process']
            worker_id = worker_info['id']
            
            if process.poll() is None:
                print(f"  Force killing {worker_id}...")
                try:
                    process.kill()
                except:
                    pass
        
        # Wait for all processes to fully terminate
        for worker_info in self.processes:
            try:
                worker_info['process'].wait(timeout=2)
            except:
                pass
        
        print("\nAll workers stopped.")
    
    def run(self):
        """Main run method."""
        self.start_all_workers()
        self.monitor_workers()
        self.stop_all_workers()


def main():
    parser = argparse.ArgumentParser(
        description='Start multiple KCL worker processes',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '--stream-name',
        default='kcl-test',
        help='Kinesis stream name'
    )
    
    parser.add_argument(
        '--table-name',
        default='appName',
        help='DynamoDB table name'
    )
    
    parser.add_argument(
        '--app-name',
        default='appName',
        help='Application name'
    )
    
    parser.add_argument(
        '--region',
        default='us-west-2',
        help='AWS region'
    )
    
    parser.add_argument(
        '--num-workers',
        type=int,
        default=3,
        help='Number of workers to start'
    )
    
    parser.add_argument(
        '--max-leases',
        type=int,
        default=2,
        help='Maximum leases per worker (MaxLeasesForWorker)'
    )
    
    args = parser.parse_args()
    
    manager = WorkerManager(args)
    manager.run()


if __name__ == '__main__':
    main()

