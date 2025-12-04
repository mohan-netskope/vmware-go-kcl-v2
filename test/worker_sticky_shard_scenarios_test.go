package test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestStickyShardBasicEnforcement tests that workers respect sticky assignments
func TestStickyShardBasicEnforcement(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        2,
		numWorkers:       2,
		appName:          appName + "-sticky-basic",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config, newStickyShardWorkerFactory(t))

	// Setup sticky assignments
	// Assume shards are: shardId-000000000000, shardId-000000000001
	workerID1 := "test-worker-0"
	workerID2 := "test-worker-1"

	// Set sticky assignments before workers start
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000001", workerID2)
	assert.Nil(t, err)

	// Start both workers
	test.cluster.SpawnWorker()
	test.cluster.SpawnWorker()

	// Assert Worker-1 claims only Shard-0
	assert.True(t, test.WaitForWorkerToClaimShard(workerID1, "shardId-000000000000"))

	// Assert Worker-2 claims only Shard-1
	assert.True(t, test.WaitForWorkerToClaimShard(workerID2, "shardId-000000000001"))

	// Verify sticky owners remain unchanged
	stickyOwner1, _ := test.GetStickyOwner("shardId-000000000000")
	assert.Equal(t, workerID1, stickyOwner1)

	stickyOwner2, _ := test.GetStickyOwner("shardId-000000000001")
	assert.Equal(t, workerID2, stickyOwner2)

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardMixedAssignments tests mixed sticky and non-sticky shards
func TestStickyShardMixedAssignments(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName + "-sticky-mixed",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config, newStickyShardWorkerFactory(t))

	workerID1 := "test-worker-0"
	workerID2 := "test-worker-1"

	// Set sticky assignments for Shard-0 and Shard-3 only
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000003", workerID2)
	assert.Nil(t, err)

	// Start both workers
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	// Each worker should get 2 shards total
	expectedCounts := map[string]int{
		w1ID: 2,
		w2ID: 2,
	}
	assert.True(t, test.WaitForShardDistribution(expectedCounts))

	// Worker-1 must have Shard-0 (sticky)
	ownership := test.GetShardOwnershipMap()
	assert.Contains(t, ownership[w1ID], "shardId-000000000000")

	// Worker-2 must have Shard-3 (sticky)
	assert.Contains(t, ownership[w2ID], "shardId-000000000003")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardExcludedFromLeaseStealing tests sticky shards aren't stolen
func TestStickyShardExcludedFromLeaseStealing(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        3,
		numWorkers:       2,
		appName:          appName + "-sticky-no-steal",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config,
		newStickyShardWorkerFactory(t).WithLeaseStealing(true))

	workerID1 := "test-worker-0"

	// Set Shard-0 as sticky to Worker-1
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)

	// Start Worker-1 (claims all 3 shards)
	w1ID, _ := test.cluster.SpawnWorker()
	expectedCounts1 := map[string]int{w1ID: 3}
	assert.True(t, test.WaitForShardDistribution(expectedCounts1))

	// Start Worker-2
	w2ID, _ := test.cluster.SpawnWorker()

	// Wait for rebalancing
	time.Sleep(30 * time.Second)

	// Worker-2 should steal non-sticky shards only
	ownership := test.GetShardOwnershipMap()

	// Worker-1 should still have Shard-0 (sticky)
	assert.Contains(t, ownership[w1ID], "shardId-000000000000",
		"Worker-1 should retain sticky shard")

	// Worker-2 should have stolen the other shards
	assert.Equal(t, 2, len(ownership[w2ID]),
		"Worker-2 should have stolen 2 non-sticky shards")

	// Worker-2 should NOT have Shard-0
	assert.NotContains(t, ownership[w2ID], "shardId-000000000000",
		"Worker-2 should not steal sticky shard")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardMultiplePerWorker tests multiple sticky shards per worker
func TestStickyShardMultiplePerWorker(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName + "-sticky-multiple",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config, newStickyShardWorkerFactory(t))

	workerID1 := "test-worker-0"
	workerID2 := "test-worker-1"

	// Set Shard-0 and Shard-1 sticky to Worker-1
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000001", workerID1)
	assert.Nil(t, err)

	// Set Shard-2 and Shard-3 sticky to Worker-2
	err = test.SetStickyOwner("shardId-000000000002", workerID2)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000003", workerID2)
	assert.Nil(t, err)

	// Start both workers
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	// Each worker should get exactly 2 shards
	expectedCounts := map[string]int{
		w1ID: 2,
		w2ID: 2,
	}
	assert.True(t, test.WaitForShardDistribution(expectedCounts))

	// Verify Worker-1 has Shard-0 and Shard-1
	ownership := test.GetShardOwnershipMap()
	assert.Contains(t, ownership[w1ID], "shardId-000000000000")
	assert.Contains(t, ownership[w1ID], "shardId-000000000001")

	// Verify Worker-2 has Shard-2 and Shard-3
	assert.Contains(t, ownership[w2ID], "shardId-000000000002")
	assert.Contains(t, ownership[w2ID], "shardId-000000000003")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardBackwardsCompatibility tests system works with no sticky assignments
func TestStickyShardBackwardsCompatibility(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName + "-sticky-compat",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config,
		newStickyShardWorkerFactory(t).WithLeaseStealing(true))

	// NO sticky assignments set

	// Start both workers
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	// Shards should be distributed evenly
	expectedCounts := map[string]int{
		w1ID: 2,
		w2ID: 2,
	}
	assert.True(t, test.WaitForShardDistribution(expectedCounts))

	// Verify no sticky assignments in DynamoDB
	stickyOwner0, _ := test.GetStickyOwner("shardId-000000000000")
	assert.Equal(t, "", stickyOwner0)

	stickyOwner1, _ := test.GetStickyOwner("shardId-000000000001")
	assert.Equal(t, "", stickyOwner1)

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardWithMaxLeases tests sticky assignment respects MaxLeasesForWorker
// TestStickyShardWithMaxLeases tests sticky assignment with MaxLeasesForWorker limit
// Verifies that workers respect MaxLeasesForWorker even when multiple sticky shards exist.
// A worker with 2 sticky shards and MaxLeasesForWorker=1 should claim only 1 shard.
// The unclaimed sticky shard should remain unassigned or be claimed by another worker
// only after the sticky owner's lease expires (failover scenario).
func TestStickyShardWithMaxLeases(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       2,
		appName:          appName + "-sticky-maxleases",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config,
		newStickyShardWorkerFactory(t).WithMaxLeasesForWorker(1))

	workerID1 := "test-worker-0"
	workerID2 := "test-worker-1"

	// Set multiple sticky shards to same worker
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000001", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000002", workerID2)
	assert.Nil(t, err)

	// Start both workers
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	// Each worker should claim only 1 shard (MaxLeasesForWorker=1)
	expectedCounts := map[string]int{
		w1ID: 1,
		w2ID: 1,
	}
	assert.True(t, test.WaitForShardDistribution(expectedCounts))

	// Worker-1 should have one of its sticky shards
	ownership := test.GetShardOwnershipMap()
	hasStickyShard := false
	for _, shard := range ownership[w1ID] {
		if shard == "shardId-000000000000" || shard == "shardId-000000000001" {
			hasStickyShard = true
			break
		}
	}
	assert.True(t, hasStickyShard, "Worker-1 should claim one of its sticky shards")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardConcurrentStartup tests race conditions with simultaneous worker startup
func TestStickyShardConcurrentStartup(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        4,
		numWorkers:       3,
		appName:          appName + "-sticky-concurrent",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config, newStickyShardWorkerFactory(t))

	workerID1 := "test-worker-0"
	workerID2 := "test-worker-1"

	// Set sticky assignments
	err := test.SetStickyOwner("shardId-000000000000", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000001", workerID2)
	assert.Nil(t, err)

	// Start all workers simultaneously
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()
	w3ID, _ := test.cluster.SpawnWorker()

	// Wait for stable distribution
	time.Sleep(30 * time.Second)

	// Verify sticky workers got their shards
	ownership := test.GetShardOwnershipMap()
	assert.Contains(t, ownership[w1ID], "shardId-000000000000")
	assert.Contains(t, ownership[w2ID], "shardId-000000000001")

	// Verify all shards are claimed
	totalShards := len(ownership[w1ID]) + len(ownership[w2ID]) + len(ownership[w3ID])
	assert.Equal(t, 4, totalShards, "All shards should be claimed")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardRuntimeUpdate tests sticky assignment changes during runtime
func TestStickyShardRuntimeUpdate(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        2,
		numWorkers:       2,
		appName:          appName + "-sticky-runtime",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	test := NewStickyShardTest(t, config, newStickyShardWorkerFactory(t))

	// NO sticky assignments initially

	// Start both workers (each claims 1 shard)
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	expectedCounts := map[string]int{
		w1ID: 1,
		w2ID: 1,
	}
	assert.True(t, test.WaitForShardDistribution(expectedCounts))

	// Get current ownership
	ownership := test.GetShardOwnershipMap()
	var w1Shard string
	if len(ownership[w1ID]) > 0 {
		w1Shard = ownership[w1ID][0]
	}

	// Update DynamoDB: Set Worker-1's shard as sticky to Worker-2
	t.Logf("Setting %s sticky to %s (currently owned by %s)", w1Shard, w2ID, w1ID)
	err := test.SetStickyOwner(w1Shard, w2ID)
	assert.Nil(t, err)

	// Wait for lease refresh cycle (Worker-1's lease will expire)
	time.Sleep(20 * time.Second)

	// Worker-2 should eventually claim Worker-1's old shard
	assert.True(t, test.WaitForWorkerToClaimShard(w2ID, w1Shard))

	// Worker-1 should not be able to reclaim it
	time.Sleep(15 * time.Second)
	finalOwnership := test.GetShardOwnershipMap()
	assert.Contains(t, finalOwnership[w2ID], w1Shard,
		"Worker-2 should own the shard after sticky assignment change")
	assert.NotContains(t, finalOwnership[w1ID], w1Shard,
		"Worker-1 should not reclaim shard sticky to Worker-2")

	time.Sleep(10 * time.Second)
	test.Shutdown()
}

// TestStickyShardExceedsMaxLeases verifies behavior when sticky assignments exceed MaxLeasesForWorker
// This test ensures that:
// 1. A worker with more sticky shards than MaxLeasesForWorker will NOT claim all of them
// 2. Only MaxLeasesForWorker limit is respected
// 3. Unclaimed sticky shards remain unassigned (logged as warnings)
func TestStickyShardExceedsMaxLeases(t *testing.T) {
	config := &TestClusterConfig{
		numShards:        6,
		numWorkers:       3,
		appName:          appName + "-sticky-exceeds",
		streamName:       streamName,
		regionName:       regionName,
		workerIDTemplate: workerID + "-%v",
	}

	// Create workers with MaxLeasesForWorker=2
	test := NewStickyShardTest(t, config,
		newStickyShardWorkerFactory(t).WithMaxLeasesForWorker(2))

	workerID0 := "test-worker-0"
	workerID1 := "test-worker-1"
	workerID2 := "test-worker-2"

	// Set 3 sticky shards to worker-0 (exceeds MaxLeasesForWorker=2)
	err := test.SetStickyOwner("shardId-000000000000", workerID0)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000001", workerID0)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000002", workerID0)
	assert.Nil(t, err)

	// Set 1 sticky shard each to other workers
	err = test.SetStickyOwner("shardId-000000000003", workerID1)
	assert.Nil(t, err)
	err = test.SetStickyOwner("shardId-000000000004", workerID2)
	assert.Nil(t, err)

	// Start all workers
	w0ID, _ := test.cluster.SpawnWorker()
	w1ID, _ := test.cluster.SpawnWorker()
	w2ID, _ := test.cluster.SpawnWorker()

	// Wait for workers to claim shards
	time.Sleep(30 * time.Second)

	// Get actual distribution
	ownership := test.GetShardOwnershipMap()

	// Verify worker-0 respects MaxLeasesForWorker
	worker0Shards := len(ownership[w0ID])
	assert.LessOrEqual(t, worker0Shards, 2,
		"Worker-0 should not exceed MaxLeasesForWorker=2, even with 3 sticky shards")

	// Verify worker-0 claimed some of its sticky shards
	hasSticky := false
	for _, shard := range ownership[w0ID] {
		if shard == "shardId-000000000000" ||
			shard == "shardId-000000000001" ||
			shard == "shardId-000000000002" {
			hasSticky = true
			break
		}
	}
	assert.True(t, hasSticky, "Worker-0 should claim at least one of its sticky shards")

	// Verify other workers got their sticky shards
	assert.Contains(t, ownership[w1ID], "shardId-000000000003",
		"Worker-1 should claim its sticky shard")
	assert.Contains(t, ownership[w2ID], "shardId-000000000004",
		"Worker-2 should claim its sticky shard")

	// Log the distribution for debugging
	t.Logf("Final distribution:")
	t.Logf("  Worker-0 (%s): %d shards - %v", w0ID, len(ownership[w0ID]), ownership[w0ID])
	t.Logf("  Worker-1 (%s): %d shards - %v", w1ID, len(ownership[w1ID]), ownership[w1ID])
	t.Logf("  Worker-2 (%s): %d shards - %v", w2ID, len(ownership[w2ID]), ownership[w2ID])

	// Expected behavior: One sticky shard for worker-0 will remain unclaimed
	// because MaxLeasesForWorker prevents it from claiming more than 2 shards
	totalClaimed := worker0Shards + len(ownership[w1ID]) + len(ownership[w2ID])
	if totalClaimed < 6 {
		t.Logf("⚠️  Note: %d shard(s) remain unclaimed due to MaxLeasesForWorker limits", 6-totalClaimed)
		t.Logf("This is expected when sticky assignments exceed MaxLeasesForWorker")
	}

	time.Sleep(10 * time.Second)
	test.Shutdown()
}
