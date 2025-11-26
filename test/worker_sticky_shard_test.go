package test

import (
	"context"
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
	t                 *testing.T
	maxLeasesForWorker int
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

