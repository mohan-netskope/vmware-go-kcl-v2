package test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesisTypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
	"github.com/stretchr/testify/assert"

	chk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/checkpoint"
	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	kc "github.com/vmware/vmware-go-kcl-v2/clientlibrary/interfaces"
	wk "github.com/vmware/vmware-go-kcl-v2/clientlibrary/worker"
	"github.com/vmware/vmware-go-kcl-v2/logger"
)

// StickyShardTest provides testing infrastructure for sticky shard assignments
type StickyShardTest struct {
	t       *testing.T
	config  *TestClusterConfig
	cluster *TestCluster
	kc      *kinesis.Client
	dc      *dynamodb.Client

	backOffSeconds int
	maxRetries     int

	streamCreated bool
}

// NewStickyShardTest creates a new sticky shard test instance with LocalStack configuration
func NewStickyShardTest(t *testing.T, config *TestClusterConfig, workerFactory TestWorkerFactory) *StickyShardTest {
	cluster := NewTestCluster(t, config, workerFactory)

	// Use dummy credentials for LocalStack
	dummyCreds := credentials.NewStaticCredentialsProvider("test", "test", "")

	// Use LocalStack endpoints
	kinesisEndpoint := "http://localhost:4566"
	dynamoEndpoint := "http://localhost:4566"

	kc := NewKinesisClient(t, config.regionName, kinesisEndpoint, dummyCreds)
	dc := NewDynamoDBClient(t, config.regionName, dynamoEndpoint, dummyCreds)

	sst := &StickyShardTest{
		t:              t,
		config:         config,
		cluster:        cluster,
		kc:             kc,
		dc:             dc,
		backOffSeconds: 5,
		maxRetries:     60,
		streamCreated:  false,
	}

	// Setup: Create Kinesis stream with required shard count
	sst.setupStream()

	return sst
}

// setupStream creates the Kinesis stream with the required number of shards
func (sst *StickyShardTest) setupStream() {
	sst.t.Logf("Creating Kinesis stream %s with %d shards", sst.config.streamName, sst.config.numShards)

	// Check if stream already exists
	describeInput := &kinesis.DescribeStreamInput{
		StreamName: aws.String(sst.config.streamName),
	}

	_, err := sst.kc.DescribeStream(context.TODO(), describeInput)
	if err == nil {
		// Stream exists, delete it first
		sst.t.Logf("Stream %s already exists, deleting it", sst.config.streamName)
		deleteInput := &kinesis.DeleteStreamInput{
			StreamName: aws.String(sst.config.streamName),
		}
		_, _ = sst.kc.DeleteStream(context.TODO(), deleteInput)

		// Wait for stream to be deleted
		time.Sleep(5 * time.Second)
	}

	// Create stream
	createInput := &kinesis.CreateStreamInput{
		StreamName: aws.String(sst.config.streamName),
		ShardCount: aws.Int32(int32(sst.config.numShards)),
	}

	_, err = sst.kc.CreateStream(context.TODO(), createInput)
	if err != nil {
		sst.t.Fatalf("Failed to create Kinesis stream: %v", err)
	}

	sst.streamCreated = true

	// Wait for stream to become active
	sst.t.Logf("Waiting for stream %s to become active", sst.config.streamName)
	maxWait := 60
	for i := 0; i < maxWait; i++ {
		output, err := sst.kc.DescribeStream(context.TODO(), describeInput)
		if err != nil {
			sst.t.Logf("Error describing stream: %v", err)
			time.Sleep(1 * time.Second)
			continue
		}

		if output.StreamDescription.StreamStatus == kinesisTypes.StreamStatusActive {
			sst.t.Logf("Stream %s is now active with %d shards",
				sst.config.streamName, len(output.StreamDescription.Shards))
			return
		}

		time.Sleep(1 * time.Second)
	}

	sst.t.Fatalf("Stream %s did not become active within %d seconds", sst.config.streamName, maxWait)
}

// EnsureDynamoDBTable creates the DynamoDB table if it doesn't exist
func (sst *StickyShardTest) EnsureDynamoDBTable() {
	sst.t.Logf("Ensuring DynamoDB table %s exists", sst.config.appName)

	// Check if table exists
	_, err := sst.dc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
		TableName: aws.String(sst.config.appName),
	})

	if err == nil {
		sst.t.Logf("DynamoDB table %s already exists", sst.config.appName)
		return
	}

	// Create table
	_, err = sst.dc.CreateTable(context.TODO(), &dynamodb.CreateTableInput{
		TableName: aws.String(sst.config.appName),
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(chk.LeaseKeyKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(chk.LeaseKeyKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		BillingMode: types.BillingModePayPerRequest,
	})

	if err != nil {
		sst.t.Fatalf("Failed to create DynamoDB table: %v", err)
	}

	// Wait for table to be active
	sst.t.Logf("Waiting for DynamoDB table %s to become active", sst.config.appName)
	for i := 0; i < 30; i++ {
		output, err := sst.dc.DescribeTable(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(sst.config.appName),
		})
		if err == nil && output.Table.TableStatus == types.TableStatusActive {
			sst.t.Logf("DynamoDB table %s is now active", sst.config.appName)
			return
		}
		time.Sleep(1 * time.Second)
	}

	sst.t.Fatalf("DynamoDB table %s did not become active within timeout", sst.config.appName)
}

// WithBackoffSeconds sets the backoff between retries
func (sst *StickyShardTest) WithBackoffSeconds(backoff int) *StickyShardTest {
	sst.backOffSeconds = backoff
	return sst
}

// WithMaxRetries sets the maximum number of retries
func (sst *StickyShardTest) WithMaxRetries(retries int) *StickyShardTest {
	sst.maxRetries = retries
	return sst
}

// SetStickyOwner sets a sticky owner for a shard in DynamoDB
func (sst *StickyShardTest) SetStickyOwner(shardID, workerID string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(sst.config.appName),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
		UpdateExpression: aws.String("SET " + chk.StickyOwnerKey + " = :sticky_owner"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sticky_owner": &types.AttributeValueMemberS{
				Value: workerID,
			},
		},
	}

	_, err := sst.dc.UpdateItem(context.TODO(), input)
	return err
}

// GetStickyOwner retrieves the sticky owner for a shard from DynamoDB
func (sst *StickyShardTest) GetStickyOwner(shardID string) (string, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(sst.config.appName),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	}

	result, err := sst.dc.GetItem(context.TODO(), input)
	if err != nil {
		return "", err
	}

	if stickyOwner, ok := result.Item[chk.StickyOwnerKey]; ok {
		return stickyOwner.(*types.AttributeValueMemberS).Value, nil
	}
	return "", nil
}

// GetCheckpointSequence retrieves the checkpoint sequence number for a shard from DynamoDB
func (sst *StickyShardTest) GetCheckpointSequence(shardID string) (string, error) {
	input := &dynamodb.GetItemInput{
		TableName: aws.String(sst.config.appName),
		Key: map[string]types.AttributeValue{
			chk.LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	}

	result, err := sst.dc.GetItem(context.TODO(), input)
	if err != nil {
		return "", err
	}

	if checkpoint, ok := result.Item[chk.SequenceNumberKey]; ok {
		return checkpoint.(*types.AttributeValueMemberS).Value, nil
	}
	return "", nil
}

// GetShardOwnershipMap returns a map of worker ID to list of shard IDs they own
func (sst *StickyShardTest) GetShardOwnershipMap() map[string][]string {
	input := &dynamodb.ScanInput{
		TableName: aws.String(sst.config.appName),
	}

	shardsByWorker := map[string][]string{}
	scan, err := sst.dc.Scan(context.TODO(), input)
	assert.Nil(sst.t, err)

	for _, result := range scan.Items {
		shardID, hasShardID := result[chk.LeaseKeyKey]
		assignedTo, hasAssignedTo := result[chk.LeaseOwnerKey]

		if !hasShardID || !hasAssignedTo {
			continue
		}

		workerID := assignedTo.(*types.AttributeValueMemberS).Value
		shard := shardID.(*types.AttributeValueMemberS).Value

		shardsByWorker[workerID] = append(shardsByWorker[workerID], shard)
	}

	return shardsByWorker
}

// WaitForWorkerToClaimShard waits until the specified worker claims the specified shard
func (sst *StickyShardTest) WaitForWorkerToClaimShard(workerID, shardID string) bool {
	for i := 0; i < sst.maxRetries; i++ {
		time.Sleep(time.Duration(sst.backOffSeconds) * time.Second)

		ownership := sst.GetShardOwnershipMap()
		if shards, ok := ownership[workerID]; ok {
			for _, shard := range shards {
				if shard == shardID {
					sst.t.Logf("Worker %s successfully claimed shard %s", workerID, shardID)
					return true
				}
			}
		}
	}
	sst.t.Logf("Worker %s did NOT claim shard %s within timeout", workerID, shardID)
	return false
}

// WaitForShardDistribution waits for shards to be distributed according to expected counts
func (sst *StickyShardTest) WaitForShardDistribution(expectedCounts map[string]int) bool {
	for i := 0; i < sst.maxRetries; i++ {
		time.Sleep(time.Duration(sst.backOffSeconds) * time.Second)

		ownership := sst.GetShardOwnershipMap()
		matches := true

		for workerID, expectedCount := range expectedCounts {
			actualCount := len(ownership[workerID])
			if actualCount != expectedCount {
				matches = false
				break
			}
		}

		if matches {
			sst.t.Log("Shard distribution matches expected counts")
			return true
		}
	}
	sst.t.Log("Shard distribution did NOT match expected counts within timeout")
	return false
}

// WaitForWorkerToReleaseShard waits until the specified worker releases the specified shard
func (sst *StickyShardTest) WaitForWorkerToReleaseShard(workerID, shardID string) bool {
	for i := 0; i < sst.maxRetries; i++ {
		time.Sleep(time.Duration(sst.backOffSeconds) * time.Second)

		ownership := sst.GetShardOwnershipMap()
		if shards, ok := ownership[workerID]; ok {
			hasShard := false
			for _, shard := range shards {
				if shard == shardID {
					hasShard = true
					break
				}
			}
			if !hasShard {
				sst.t.Logf("Worker %s released shard %s", workerID, shardID)
				return true
			}
		} else {
			// Worker has no shards
			sst.t.Logf("Worker %s released shard %s (has no shards)", workerID, shardID)
			return true
		}
	}
	sst.t.Logf("Worker %s did NOT release shard %s within timeout", workerID, shardID)
	return false
}

// GetShardHashKey retrieves the starting hash key for a specific shard
func (sst *StickyShardTest) GetShardHashKey(shardID string) (string, error) {
	input := &kinesis.DescribeStreamInput{
		StreamName: aws.String(sst.config.streamName),
	}

	output, err := sst.kc.DescribeStream(context.TODO(), input)
	if err != nil {
		return "", err
	}

	for _, shard := range output.StreamDescription.Shards {
		if *shard.ShardId == shardID {
			return *shard.HashKeyRange.StartingHashKey, nil
		}
	}
	return "", nil
}

// PublishRecordsToShard publishes records to a specific shard using explicit hash key
func (sst *StickyShardTest) PublishRecordsToShard(shardID string, count int) error {
	hashKey, err := sst.GetShardHashKey(shardID)
	if err != nil {
		return err
	}

	sst.t.Logf("Publishing %d records to shard %s (hashKey: %s)", count, shardID, hashKey)

	for i := 0; i < count; i++ {
		input := &kinesis.PutRecordInput{
			Data:            []byte(specstr),
			StreamName:      aws.String(sst.config.streamName),
			PartitionKey:    aws.String("test-partition-key"),
			ExplicitHashKey: aws.String(hashKey),
		}

		_, err := sst.kc.PutRecord(context.TODO(), input)
		if err != nil {
			return err
		}
	}

	sst.t.Logf("Successfully published %d records to shard %s", count, shardID)
	return nil
}

// WaitForCheckpoint waits until the checkpoint for a shard is updated (non-empty)
func (sst *StickyShardTest) WaitForCheckpoint(shardID string) (string, bool) {
	for i := 0; i < sst.maxRetries; i++ {
		time.Sleep(time.Duration(sst.backOffSeconds) * time.Second)

		checkpoint, err := sst.GetCheckpointSequence(shardID)
		if err == nil && checkpoint != "" {
			sst.t.Logf("Checkpoint for shard %s: %s", shardID, checkpoint)
			return checkpoint, true
		}
	}
	sst.t.Logf("No checkpoint found for shard %s within timeout", shardID)
	return "", false
}

// WaitForCheckpointChange waits until the checkpoint for a shard changes from a previous value
func (sst *StickyShardTest) WaitForCheckpointChange(shardID, previousCheckpoint string) (string, bool) {
	for i := 0; i < sst.maxRetries; i++ {
		time.Sleep(time.Duration(sst.backOffSeconds) * time.Second)

		checkpoint, err := sst.GetCheckpointSequence(shardID)
		if err == nil && checkpoint != "" && checkpoint != previousCheckpoint {
			sst.t.Logf("Checkpoint for shard %s changed from %s to %s", shardID, previousCheckpoint, checkpoint)
			return checkpoint, true
		}
	}
	sst.t.Logf("Checkpoint for shard %s did not change from %s within timeout", shardID, previousCheckpoint)
	return "", false
}

// Shutdown shuts down all workers in the cluster and cleans up resources
func (sst *StickyShardTest) Shutdown() {
	sst.t.Log("Shutting down test cluster")
	sst.cluster.Shutdown()

	// Cleanup: Delete Kinesis stream
	if sst.streamCreated {
		sst.cleanupStream()
	}

	// Cleanup: Delete DynamoDB table
	sst.cleanupDynamoTable()
}

// cleanupStream deletes the Kinesis stream
func (sst *StickyShardTest) cleanupStream() {
	sst.t.Logf("Deleting Kinesis stream %s", sst.config.streamName)

	deleteInput := &kinesis.DeleteStreamInput{
		StreamName: aws.String(sst.config.streamName),
	}

	_, err := sst.kc.DeleteStream(context.TODO(), deleteInput)
	if err != nil {
		sst.t.Logf("Warning: Failed to delete stream: %v", err)
	} else {
		sst.t.Logf("Stream %s deleted successfully", sst.config.streamName)
	}
}

// cleanupDynamoTable deletes the DynamoDB table
func (sst *StickyShardTest) cleanupDynamoTable() {
	sst.t.Logf("Deleting DynamoDB table %s", sst.config.appName)

	deleteInput := &dynamodb.DeleteTableInput{
		TableName: aws.String(sst.config.appName),
	}

	_, err := sst.dc.DeleteTable(context.TODO(), deleteInput)
	if err != nil {
		sst.t.Logf("Warning: Failed to delete table: %v", err)
	} else {
		sst.t.Logf("Table %s deleted successfully", sst.config.appName)
	}
}

// stickyShardWorkerFactory creates workers with sticky shard configuration
type stickyShardWorkerFactory struct {
	t                   *testing.T
	maxLeasesForWorker  int
	enableLeaseStealing bool
}

func newStickyShardWorkerFactory(t *testing.T) *stickyShardWorkerFactory {
	return &stickyShardWorkerFactory{
		t:                   t,
		maxLeasesForWorker:  10, // Default high value
		enableLeaseStealing: false,
	}
}

func (wf *stickyShardWorkerFactory) WithMaxLeasesForWorker(max int) *stickyShardWorkerFactory {
	wf.maxLeasesForWorker = max
	return wf
}

func (wf *stickyShardWorkerFactory) WithLeaseStealing(enabled bool) *stickyShardWorkerFactory {
	wf.enableLeaseStealing = enabled
	return wf
}

func (wf *stickyShardWorkerFactory) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	})

	log.WithFields(logger.Fields{"worker": workerID})

	// Use dummy credentials for LocalStack
	dummyCreds := credentials.NewStaticCredentialsProvider("test", "test", "")

	kclConfig := cfg.NewKinesisClientLibConfigWithCredentials(
		config.appName,
		config.streamName,
		config.regionName,
		workerID,
		dummyCreds,
		dummyCreds,
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithMaxLeasesForWorker(wf.maxLeasesForWorker).
		WithKinesisEndpoint("http://localhost:4566").
		WithDynamoDBEndpoint("http://localhost:4566").
		WithLogger(log)

	if wf.enableLeaseStealing {
		kclConfig.WithLeaseStealing(true)
	}

	return kclConfig
}

func (wf *stickyShardWorkerFactory) CreateWorker(_ string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	worker := wk.NewWorker(recordProcessorFactory(wf.t), kclConfig)
	return worker
}

// InitSequenceRecord stores the initialization sequence number for a shard
type InitSequenceRecord struct {
	ShardID        string
	WorkerID       string
	SequenceNumber string
}

// checkpointTrackingProcessorFactory creates processors that track initialization sequence numbers
type checkpointTrackingProcessorFactory struct {
	t             *testing.T
	initSequences []InitSequenceRecord
	mutex         sync.RWMutex
}

func newCheckpointTrackingProcessorFactory(t *testing.T) *checkpointTrackingProcessorFactory {
	return &checkpointTrackingProcessorFactory{
		t:             t,
		initSequences: make([]InitSequenceRecord, 0),
	}
}

// GetInitSequences returns all recorded initialization sequences
func (f *checkpointTrackingProcessorFactory) GetInitSequences() []InitSequenceRecord {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	result := make([]InitSequenceRecord, len(f.initSequences))
	copy(result, f.initSequences)
	return result
}

// GetLatestInitSequence returns the latest initialization sequence for a shard
func (f *checkpointTrackingProcessorFactory) GetLatestInitSequence(shardID string) (string, bool) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	// Search in reverse to find the latest
	for i := len(f.initSequences) - 1; i >= 0; i-- {
		if f.initSequences[i].ShardID == shardID {
			return f.initSequences[i].SequenceNumber, true
		}
	}
	return "", false
}

// GetInitSequenceForWorker returns the initialization sequence for a specific worker and shard
func (f *checkpointTrackingProcessorFactory) GetInitSequenceForWorker(workerID, shardID string) (string, bool) {
	f.mutex.RLock()
	defer f.mutex.RUnlock()

	// Search in reverse to find the latest for this worker
	for i := len(f.initSequences) - 1; i >= 0; i-- {
		if f.initSequences[i].ShardID == shardID && f.initSequences[i].WorkerID == workerID {
			return f.initSequences[i].SequenceNumber, true
		}
	}
	return "", false
}

func (f *checkpointTrackingProcessorFactory) recordInitSequence(shardID, workerID, sequenceNumber string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.initSequences = append(f.initSequences, InitSequenceRecord{
		ShardID:        shardID,
		WorkerID:       workerID,
		SequenceNumber: sequenceNumber,
	})
	f.t.Logf("Recorded init sequence for shard %s, worker %s: %s", shardID, workerID, sequenceNumber)
}

func (f *checkpointTrackingProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return &checkpointTrackingProcessor{
		factory: f,
		t:       f.t,
	}
}

// checkpointTrackingProcessor tracks initialization sequence and checkpoints
type checkpointTrackingProcessor struct {
	factory  *checkpointTrackingProcessorFactory
	t        *testing.T
	shardID  string
	workerID string
}

func (p *checkpointTrackingProcessor) Initialize(input *kc.InitializationInput) {
	p.shardID = input.ShardId

	sequenceNumber := ""
	if input.ExtendedSequenceNumber != nil && input.ExtendedSequenceNumber.SequenceNumber != nil {
		sequenceNumber = *input.ExtendedSequenceNumber.SequenceNumber
	}

	p.t.Logf("Processor initialized for shard %s at sequence: %s", p.shardID, sequenceNumber)
	p.factory.recordInitSequence(p.shardID, p.workerID, sequenceNumber)
}

func (p *checkpointTrackingProcessor) ProcessRecords(input *kc.ProcessRecordsInput) {
	if len(input.Records) == 0 {
		return
	}

	p.t.Logf("Processing %d records for shard %s", len(input.Records), p.shardID)

	// Checkpoint after processing this batch
	lastRecordSequenceNumber := input.Records[len(input.Records)-1].SequenceNumber
	p.t.Logf("Checkpointing at sequence: %s", aws.ToString(lastRecordSequenceNumber))
	_ = input.Checkpointer.Checkpoint(lastRecordSequenceNumber)
}

func (p *checkpointTrackingProcessor) Shutdown(input *kc.ShutdownInput) {
	p.t.Logf("Shutdown for shard %s, reason: %v", p.shardID, aws.ToString(kc.ShutdownReasonMessage(input.ShutdownReason)))

	if input.ShutdownReason == kc.TERMINATE {
		_ = input.Checkpointer.Checkpoint(nil)
	}
}

// checkpointTrackingWorkerFactory creates workers with checkpoint tracking
type checkpointTrackingWorkerFactory struct {
	t                  *testing.T
	processorFactory   *checkpointTrackingProcessorFactory
	maxLeasesForWorker int
}

func newCheckpointTrackingWorkerFactory(t *testing.T, processorFactory *checkpointTrackingProcessorFactory) *checkpointTrackingWorkerFactory {
	return &checkpointTrackingWorkerFactory{
		t:                  t,
		processorFactory:   processorFactory,
		maxLeasesForWorker: 10,
	}
}

func (wf *checkpointTrackingWorkerFactory) WithMaxLeasesForWorker(max int) *checkpointTrackingWorkerFactory {
	wf.maxLeasesForWorker = max
	return wf
}

func (wf *checkpointTrackingWorkerFactory) CreateKCLConfig(workerID string, config *TestClusterConfig) *cfg.KinesisClientLibConfiguration {
	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
		EnableFile:        true,
		FileLevel:         logger.Info,
		FileJSONFormat:    true,
		Filename:          "log.log",
	})

	log.WithFields(logger.Fields{"worker": workerID})

	// Use dummy credentials for LocalStack
	dummyCreds := credentials.NewStaticCredentialsProvider("test", "test", "")

	kclConfig := cfg.NewKinesisClientLibConfigWithCredentials(
		config.appName,
		config.streamName,
		config.regionName,
		workerID,
		dummyCreds,
		dummyCreds,
	).
		WithInitialPositionInStream(cfg.LATEST).
		WithMaxRecords(10).
		WithShardSyncIntervalMillis(5000).
		WithFailoverTimeMillis(10000).
		WithMaxLeasesForWorker(wf.maxLeasesForWorker).
		WithKinesisEndpoint("http://localhost:4566").
		WithDynamoDBEndpoint("http://localhost:4566").
		WithLogger(log)

	return kclConfig
}

func (wf *checkpointTrackingWorkerFactory) CreateWorker(workerID string, kclConfig *cfg.KinesisClientLibConfiguration) *wk.Worker {
	// Create a processor that knows its worker ID
	processor := &checkpointTrackingProcessor{
		factory:  wf.processorFactory,
		t:        wf.t,
		workerID: workerID,
	}

	// Create a custom factory that returns the same processor instance
	customFactory := &singleProcessorFactory{processor: processor}

	worker := wk.NewWorker(customFactory, kclConfig)
	return worker
}

// singleProcessorFactory returns a specific processor instance
type singleProcessorFactory struct {
	processor kc.IRecordProcessor
}

func (f *singleProcessorFactory) CreateProcessor() kc.IRecordProcessor {
	return f.processor
}
