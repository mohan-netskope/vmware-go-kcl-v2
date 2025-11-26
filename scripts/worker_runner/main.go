/*
 * Copyright (c) 2025 VMware, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is furnished to do
 * so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or substantial
 * portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"

	kc "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl-v2/logger"
)

var (
	workerID   = flag.String("worker-id", "worker-0", "Unique worker ID")
	streamName = flag.String("stream-name", "kcl-test", "Kinesis stream name")
	tableName  = flag.String("table-name", "appName", "DynamoDB table name")
	appName    = flag.String("app-name", "appName", "Application name")
	regionName = flag.String("region", "us-west-2", "AWS region")
	maxLeases  = flag.Int("max-leases", 2, "Maximum leases per worker")
)

// Simple record processor that logs records
type simpleRecordProcessor struct {
	shardID string
	count   int
}

func (rp *simpleRecordProcessor) Initialize(input *kc.InitializationInput) {
	rp.shardID = input.ShardId
	rp.count = 0
	fmt.Printf("[%s] Initialized processor for shard: %s\n", *workerID, rp.shardID)
}

func (rp *simpleRecordProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	// Don't process empty records
	if len(input.Records) == 0 {
		return
	}

	for _, record := range input.Records {
		fmt.Printf("[%s] Shard %s: Received record: %s\n", *workerID, rp.shardID, string(record.Data))
		rp.count++
	}

	// Checkpoint after processing batch
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	fmt.Printf("[%s] Shard %s: Checkpointing at %s (processed %d records in batch)\n", 
		*workerID, rp.shardID, lastRecordSequenceNumber, len(input.Records))
	_ = input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

func (rp *simpleRecordProcessor) Shutdown(input *kc.ShutdownInput) {
	fmt.Printf("[%s] Shard %s: Shutdown (reason: %s, total processed: %d records)\n", 
		*workerID, rp.shardID, aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason)), rp.count)

	// Checkpoint on terminate
	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}

// Record processor factory
type simpleRecordProcessorFactory struct{}

func (f *simpleRecordProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &simpleRecordProcessor{}
}

func main() {
	flag.Parse()

	fmt.Printf("Starting KCL worker: %s\n", *workerID)
	fmt.Printf("  Stream: %s\n", *streamName)
	fmt.Printf("  Table: %s\n", *tableName)
	fmt.Printf("  Region: %s\n", *regionName)
	fmt.Printf("  MaxLeases: %d\n", *maxLeases)

	// Create logger
	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Info,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Debug,
		FileJSONFormat:    false,
		Filename:          fmt.Sprintf("worker-%s.log", *workerID),
	})

	// Use dummy credentials for LocalStack
	dummyCreds := credentials.NewStaticCredentialsProvider("test", "test", "")

	// Create KCL config
	kclConfig := config.NewKinesisClientLibConfigWithCredentials(
		*appName,
		*streamName,
		*regionName,
		*workerID,
		dummyCreds,
		dummyCreds,
	).
		WithInitialPositionInStream(config.LATEST).
		WithMaxRecords(10).
		WithMaxLeasesForWorker(*maxLeases).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithKinesisEndpoint("http://localhost:4566").
		WithDynamoDBEndpoint("http://localhost:4566").
		WithLogger(log)

	// Override table name if different from app name
	if *tableName != *appName {
		kclConfig.TableName = *tableName
	}

	// Create worker
	w := worker.NewWorker(&simpleRecordProcessorFactory{}, kclConfig)

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start worker in goroutine
	errChan := make(chan error, 1)
	go func() {
		if err := w.Start(); err != nil {
			errChan <- err
		}
	}()

	// Wait for shutdown signal or error
	select {
	case <-sigChan:
		fmt.Printf("\n[%s] Received shutdown signal, stopping worker...\n", *workerID)
		w.Shutdown()
		// Wait a bit for graceful shutdown
		time.Sleep(2 * time.Second)
	case err := <-errChan:
		fmt.Printf("[%s] Worker error: %v\n", *workerID, err)
		os.Exit(1)
	}

	fmt.Printf("[%s] Worker stopped\n", *workerID)
}

