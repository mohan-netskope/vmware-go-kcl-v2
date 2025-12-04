package checkpoint

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"

	cfg "github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl-v2/logger"
)

// TestCheckpointSequence_LostOwnership tests that checkpoint fails when worker has lost ownership
func TestCheckpointSequence_LostOwnership(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			LeaseKeyKey:   &types.AttributeValueMemberS{Value: "shard-001"},
			LeaseOwnerKey: &types.AttributeValueMemberS{Value: "worker-2"}, // Different owner!
		},
		shouldFailConditional: true,
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1", // We think we own it
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should fail with lease not acquired error
	assert.NotNil(t, err, "Checkpoint should fail when ownership is lost")
	var leaseErr ErrLeaseNotAcquired
	assert.True(t, errors.As(err, &leaseErr), "Error should be ErrLeaseNotAcquired")

	// Verify the original owner in DynamoDB is unchanged
	assert.Equal(t, "worker-2", mockSvc.item[LeaseOwnerKey].(*types.AttributeValueMemberS).Value,
		"DynamoDB owner should remain unchanged")
}

// TestCheckpointSequence_RetainsOwnership tests that checkpoint succeeds when worker still owns the lease
func TestCheckpointSequence_RetainsOwnership(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist:            true,
		shouldFailConditional: false,
		item: map[string]types.AttributeValue{
			LeaseKeyKey:   &types.AttributeValueMemberS{Value: "shard-001"},
			LeaseOwnerKey: &types.AttributeValueMemberS{Value: "worker-1"}, // Same owner
		},
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1", // We own it
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should succeed
	assert.Nil(t, err, "Checkpoint should succeed when ownership is retained")

	// Verify update was called
	assert.True(t, mockSvc.updateItemCalled, "UpdateItem should have been called")
}

// TestCheckpointSequence_PreservesStickyOwner tests that StickyOwner is preserved when not in memory
func TestCheckpointSequence_PreservesStickyOwner(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist:            true,
		shouldFailConditional: false,
		item: map[string]types.AttributeValue{
			LeaseKeyKey:    &types.AttributeValueMemberS{Value: "shard-001"},
			LeaseOwnerKey:  &types.AttributeValueMemberS{Value: "worker-1"},
			StickyOwnerKey: &types.AttributeValueMemberS{Value: "worker-3"}, // Sticky to another worker
		},
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1",
		StickyOwner:  "", // Empty in memory (not aware of sticky owner)
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should succeed
	assert.Nil(t, err, "Checkpoint should succeed")

	// Verify StickyOwner was NOT included in the update (preserving DynamoDB value)
	updateInput := mockSvc.lastUpdateInput
	assert.NotNil(t, updateInput, "UpdateItemInput should be captured")

	// Check that StickyOwner is not in the update expression
	_, hasStickyOwner := updateInput.ExpressionAttributeValues[":so"]
	assert.False(t, hasStickyOwner, "StickyOwner should not be in update when empty in memory")
}

// TestCheckpointSequence_UpdatesStickyOwnerWhenPresent tests that StickyOwner is updated when present in memory
func TestCheckpointSequence_UpdatesStickyOwnerWhenPresent(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist:            true,
		shouldFailConditional: false,
		item: map[string]types.AttributeValue{
			LeaseKeyKey:   &types.AttributeValueMemberS{Value: "shard-001"},
			LeaseOwnerKey: &types.AttributeValueMemberS{Value: "worker-1"},
		},
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1",
		StickyOwner:  "worker-1", // Sticky owner present in memory
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should succeed
	assert.Nil(t, err, "Checkpoint should succeed")

	// Verify StickyOwner WAS included in the update
	updateInput := mockSvc.lastUpdateInput
	assert.NotNil(t, updateInput, "UpdateItemInput should be captured")

	// Check that StickyOwner is in the update expression
	stickyOwnerVal, hasStickyOwner := updateInput.ExpressionAttributeValues[":so"]
	assert.True(t, hasStickyOwner, "StickyOwner should be in update when present in memory")
	assert.Equal(t, "worker-1", stickyOwnerVal.(*types.AttributeValueMemberS).Value,
		"StickyOwner value should match memory")
}

// TestCheckpointSequence_InvalidLeaseTimeout tests validation of zero lease timeout
func TestCheckpointSequence_InvalidLeaseTimeout(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist:            true,
		shouldFailConditional: false,
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1",
		LeaseTimeout: time.Time{}, // Zero time (invalid)
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should fail with validation error
	assert.NotNil(t, err, "Checkpoint should fail with zero lease timeout")
	assert.Contains(t, err.Error(), "invalid lease timeout", "Error should mention invalid lease timeout")
}

// TestCheckpointSequence_EmptyLeaseOwner tests validation of empty lease owner
func TestCheckpointSequence_EmptyLeaseOwner(t *testing.T) {
	mockSvc := &mockDynamoDB{
		tableExist:            true,
		shouldFailConditional: false,
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "", // Empty owner (invalid)
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Attempt to checkpoint
	err := checkpoint.CheckpointSequence(shard)

	// Should fail with validation error
	assert.NotNil(t, err, "Checkpoint should fail with empty lease owner")
	assert.Contains(t, err.Error(), "invalid lease owner", "Error should mention invalid lease owner")
}

// TestCheckpointSequence_RaceConditionScenario simulates the race condition scenario
func TestCheckpointSequence_RaceConditionScenario(t *testing.T) {
	// Simulate: Worker-1 owns shard, loses lease, Worker-2 takes over, Worker-1 tries to checkpoint

	mockSvc := &mockDynamoDB{
		tableExist: true,
		item: map[string]types.AttributeValue{
			LeaseKeyKey:   &types.AttributeValueMemberS{Value: "shard-001"},
			LeaseOwnerKey: &types.AttributeValueMemberS{Value: "worker-1"},
		},
		shouldFailConditional: false,
	}

	log := logger.NewLogrusLoggerWithConfig(logger.Configuration{
		EnableConsole:     true,
		ConsoleLevel:      logger.Debug,
		ConsoleJSONFormat: false,
	})

	kclConfig1 := cfg.NewKinesisClientLibConfig("test-app", "test-stream", "us-west-2", "worker-1").
		WithInitialPositionInStream(cfg.LATEST).
		WithLogger(log)

	checkpoint := NewDynamoCheckpoint(kclConfig1).WithDynamoDB(mockSvc)
	_ = checkpoint.Init()

	leaseTimeout := time.Now().Add(5 * time.Minute).UTC()
	shard1 := &par.ShardStatus{
		ID:           "shard-001",
		Checkpoint:   "seq-12345",
		AssignedTo:   "worker-1",
		LeaseTimeout: leaseTimeout,
		Mux:          &sync.RWMutex{},
	}

	// Step 1: Worker-1 successfully checkpoints
	err := checkpoint.CheckpointSequence(shard1)
	assert.Nil(t, err, "First checkpoint should succeed")

	// Step 2: Simulate lease expiry and Worker-2 taking over
	// In real scenario, Worker-2 would call GetLease which updates DynamoDB
	mockSvc.item[LeaseOwnerKey] = &types.AttributeValueMemberS{Value: "worker-2"}
	mockSvc.shouldFailConditional = true // Now conditional check will fail for Worker-1

	// Step 3: Worker-1 (thinking it still owns the lease) tries to checkpoint
	shard1.SetCheckpoint("seq-12346") // New checkpoint
	err = checkpoint.CheckpointSequence(shard1)

	// Should fail because Worker-1 no longer owns the lease
	assert.NotNil(t, err, "Checkpoint should fail after losing ownership")
	var leaseErr ErrLeaseNotAcquired
	assert.True(t, errors.As(err, &leaseErr), "Error should be ErrLeaseNotAcquired")

	// Verify DynamoDB still shows Worker-2 as owner (not overwritten by Worker-1)
	assert.Equal(t, "worker-2", mockSvc.item[LeaseOwnerKey].(*types.AttributeValueMemberS).Value,
		"DynamoDB owner should still be Worker-2")
}
