/*
 * Copyright (c) 2018 VMware, Inc.
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

// Package checkpoint
// The implementation is derived from https://github.com/patrobinson/gokini
//
// Copyright 2018 Patrick robinson.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/vmware/vmware-go-kcl-v2/clientlibrary/config"
	par "github.com/vmware/vmware-go-kcl-v2/clientlibrary/partition"
	"github.com/vmware/vmware-go-kcl-v2/logger"
)

const (
	// NumMaxRetries is the max times of doing retry
	NumMaxRetries = 10
)

var (
	NoLeaseOwnerErr = errors.New("no LeaseOwner in checkpoints table")
)

// DynamoCheckpoint implements the Checkpoint interface using DynamoDB as a backend
type DynamoCheckpoint struct {
	log                     logger.Logger
	TableName               string
	leaseTableReadCapacity  int64
	leaseTableWriteCapacity int64

	LeaseDuration int
	svc           DynamoDBAPI
	kclConfig     *config.KinesisClientLibConfiguration
	Retries       int
	lastLeaseSync time.Time
}

func NewDynamoCheckpoint(kclConfig *config.KinesisClientLibConfiguration) *DynamoCheckpoint {
	checkpointer := &DynamoCheckpoint{
		log:                     kclConfig.Logger,
		TableName:               kclConfig.TableName,
		leaseTableReadCapacity:  int64(kclConfig.InitialLeaseTableReadCapacity),
		leaseTableWriteCapacity: int64(kclConfig.InitialLeaseTableWriteCapacity),
		LeaseDuration:           kclConfig.FailoverTimeMillis,
		kclConfig:               kclConfig,
		Retries:                 NumMaxRetries,
	}

	return checkpointer
}

// WithDynamoDB is used to provide DynamoDB service
func (checkpointer *DynamoCheckpoint) WithDynamoDB(svc DynamoDBAPI) *DynamoCheckpoint {
	checkpointer.svc = svc
	return checkpointer
}

// Init initialises the DynamoDB Checkpoint
func (checkpointer *DynamoCheckpoint) Init() error {
	checkpointer.log.Infof("Creating DynamoDB session")

	if checkpointer.svc == nil {
		resolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
			if service == dynamodb.ServiceID && len(checkpointer.kclConfig.DynamoDBEndpoint) > 0 {
				return aws.Endpoint{
					PartitionID:   "aws",
					URL:           checkpointer.kclConfig.DynamoDBEndpoint,
					SigningRegion: checkpointer.kclConfig.RegionName,
				}, nil
			}
			// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		})

		cfg, err := awsConfig.LoadDefaultConfig(
			context.TODO(),
			awsConfig.WithRegion(checkpointer.kclConfig.RegionName),
			awsConfig.WithCredentialsProvider(checkpointer.kclConfig.DynamoDBCredentials),
			awsConfig.WithEndpointResolverWithOptions(resolver),
			awsConfig.WithRetryer(func() aws.Retryer {
				return retry.AddWithMaxBackoffDelay(retry.NewStandard(), retry.DefaultMaxBackoff)
			}),
		)

		if err != nil {
			checkpointer.log.Fatalf("unable to load SDK config, %v", err)
		}

		checkpointer.svc = dynamodb.NewFromConfig(cfg)
	}

	if !checkpointer.doesTableExist() {
		return checkpointer.createTable()
	}

	return nil
}

// GetLease attempts to gain a lock on the given shard
func (checkpointer *DynamoCheckpoint) GetLease(shard *par.ShardStatus, newAssignTo string) error {
	newLeaseTimeout := time.Now().Add(time.Duration(checkpointer.LeaseDuration) * time.Millisecond).UTC()
	newLeaseTimeoutString := newLeaseTimeout.Format(time.RFC3339Nano)
	currentCheckpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	// Check for sticky owner assignment
	if stickyOwnerVar, hasStickyOwner := currentCheckpoint[StickyOwnerKey]; hasStickyOwner {
		stickyOwner := stickyOwnerVar.(*types.AttributeValueMemberS).Value
		if stickyOwner != "" && stickyOwner != newAssignTo {
			// This shard has a sticky owner and it's not the current worker
			// Check if this is a lease renewal by a temporary holder(~non sticky owner)
			assignedVar, assignedToOk := currentCheckpoint[LeaseOwnerKey]
			if assignedToOk {
				currentAssignee := assignedVar.(*types.AttributeValueMemberS).Value
				if currentAssignee == newAssignTo {
					// This is a temporary holder(~non sticky owner) trying to renew the lease
					// Don't allow renewal - let the lease expire so sticky owner can reclaim
					checkpointer.log.Infof("Shard %s is sticky to worker %s. Temporary holder(~non sticky owner) %s will not renew lease to allow sticky owner to reclaim",
						shard.ID, stickyOwner, newAssignTo)
					return ErrLeaseNotAcquired{"temporary holder(~non sticky owner) cannot renew lease for shard with sticky owner"}
				}
			}
		}
	}

	isClaimRequestExpired := shard.IsClaimRequestExpired(checkpointer.kclConfig)

	var claimRequest string
	if checkpointer.kclConfig.EnableLeaseStealing {
		if currentCheckpointClaimRequest, ok := currentCheckpoint[ClaimRequestKey]; ok &&
			currentCheckpointClaimRequest.(*types.AttributeValueMemberS).Value != "" {
			claimRequest = currentCheckpointClaimRequest.(*types.AttributeValueMemberS).Value
			if newAssignTo != claimRequest && !isClaimRequestExpired {
				checkpointer.log.Debugf("another worker: %s has a claim on this shard. Not going to renew the lease", claimRequest)
				return errors.New(ErrShardClaimed)
			}
		}
	}

	assignedVar, assignedToOk := currentCheckpoint[LeaseOwnerKey]
	leaseVar, leaseTimeoutOk := currentCheckpoint[LeaseTimeoutKey]

	var conditionalExpression string
	var expressionAttributeValues map[string]types.AttributeValue

	if !leaseTimeoutOk || !assignedToOk {
		conditionalExpression = "attribute_not_exists(AssignedTo)"
	} else {
		assignedTo := assignedVar.(*types.AttributeValueMemberS).Value
		leaseTimeout := leaseVar.(*types.AttributeValueMemberS).Value

		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, leaseTimeout)
		if err != nil {
			return err
		}

		if checkpointer.kclConfig.EnableLeaseStealing {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo && !isClaimRequestExpired {
				return ErrLeaseNotAcquired{"current lease timeout not yet expired"}
			}
		} else {
			if time.Now().UTC().Before(currentLeaseTimeout) && assignedTo != newAssignTo {
				return ErrLeaseNotAcquired{"current lease timeout not yet expired"}
			}
		}

		checkpointer.log.Debugf("Attempting to get a lock for shard: %s, leaseTimeout: %s, assignedTo: %s, newAssignedTo: %s", shard.ID, currentLeaseTimeout, assignedTo, newAssignTo)
		conditionalExpression = "ShardID = :id AND AssignedTo = :assigned_to AND LeaseTimeout = :lease_timeout"
		expressionAttributeValues = map[string]types.AttributeValue{
			":id": &types.AttributeValueMemberS{
				Value: shard.ID,
			},
			":assigned_to": &types.AttributeValueMemberS{
				Value: assignedTo,
			},
			":lease_timeout": &types.AttributeValueMemberS{
				Value: leaseTimeout,
			},
		}
	}

	marshalledCheckpoint := map[string]types.AttributeValue{
		LeaseKeyKey: &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		LeaseOwnerKey: &types.AttributeValueMemberS{
			Value: newAssignTo,
		},
		LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: newLeaseTimeoutString,
		},
	}

	if len(shard.ParentShardId) > 0 {
		marshalledCheckpoint[ParentShardIdKey] = &types.AttributeValueMemberS{
			Value: shard.ParentShardId,
		}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint != "" {
		marshalledCheckpoint[SequenceNumberKey] = &types.AttributeValueMemberS{
			Value: checkpoint,
		}
	}

	// Preserve StickyOwner from currentCheckpoint if present
	// Also set it on the shard object so CheckpointSequence can access it later
	if stickyOwnerVar, hasStickyOwner := currentCheckpoint[StickyOwnerKey]; hasStickyOwner {
		stickyOwnerValue := stickyOwnerVar.(*types.AttributeValueMemberS).Value
		marshalledCheckpoint[StickyOwnerKey] = stickyOwnerVar
		if stickyOwnerValue != "" {
			shard.SetStickyOwner(stickyOwnerValue)
		}
	}

	if checkpointer.kclConfig.EnableLeaseStealing {
		if claimRequest != "" && claimRequest == newAssignTo && !isClaimRequestExpired {
			if expressionAttributeValues == nil {
				expressionAttributeValues = make(map[string]types.AttributeValue)
			}
			conditionalExpression = conditionalExpression + " AND ClaimRequest = :claim_request"
			expressionAttributeValues[":claim_request"] = &types.AttributeValueMemberS{
				Value: claimRequest,
			}
		}
	}

	err = checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
	if err != nil {
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			return ErrLeaseNotAcquired{conditionalCheckErr.ErrorMessage()}
		}
		return err
	}

	shard.Mux.Lock()
	shard.AssignedTo = newAssignTo
	shard.LeaseTimeout = newLeaseTimeout
	shard.Mux.Unlock()

	return nil
}

// CheckpointSequence writes a checkpoint at the designated sequence ID
// This function now uses UpdateItem with a conditional check to prevent race conditions
// where a worker that has lost ownership can overwrite the current owner's lease
func (checkpointer *DynamoCheckpoint) CheckpointSequence(shard *par.ShardStatus) error {
	leaseTimeout := shard.GetLeaseTimeout()
	currentOwner := shard.GetLeaseOwner()

	// Validate inputs to prevent writing invalid data
	if leaseTimeout.IsZero() {
		checkpointer.log.Warnf("Invalid lease timeout (zero time) for shard %s", shard.ID)
		return fmt.Errorf("invalid lease timeout for shard %s", shard.ID)
	}

	if currentOwner == "" {
		checkpointer.log.Warnf("Invalid lease owner (empty) for shard %s", shard.ID)
		return fmt.Errorf("invalid lease owner for shard %s", shard.ID)
	}

	leaseTimeoutStr := leaseTimeout.UTC().Format(time.RFC3339Nano)

	// Build update expression using UpdateItem to preserve fields not being updated
	updateExpression := "SET #cp = :cp, #at = :at, #lt = :lt"
	expressionAttributeNames := map[string]string{
		"#cp": SequenceNumberKey,
		"#at": LeaseOwnerKey,
		"#lt": LeaseTimeoutKey,
	}
	expressionAttributeValues := map[string]types.AttributeValue{
		":cp": &types.AttributeValueMemberS{Value: shard.GetCheckpoint()},
		":at": &types.AttributeValueMemberS{Value: currentOwner},
		":lt": &types.AttributeValueMemberS{Value: leaseTimeoutStr},
		// Add current owner for conditional check
		":current_owner": &types.AttributeValueMemberS{Value: currentOwner},
	}

	// Include parent shard ID if present
	if len(shard.ParentShardId) > 0 {
		updateExpression += ", #ps = :ps"
		expressionAttributeNames["#ps"] = ParentShardIdKey
		expressionAttributeValues[":ps"] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
	}

	// Preserve StickyOwner if present in memory
	// Only update if we have a value - don't overwrite existing DynamoDB value with empty
	if stickyOwner := shard.GetStickyOwner(); stickyOwner != "" {
		updateExpression += ", #so = :so"
		expressionAttributeNames["#so"] = StickyOwnerKey
		expressionAttributeValues[":so"] = &types.AttributeValueMemberS{Value: stickyOwner}
	}

	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]types.AttributeValue{
			LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shard.ID,
			},
		},
		UpdateExpression:          aws.String(updateExpression),
		ExpressionAttributeNames:  expressionAttributeNames,
		ExpressionAttributeValues: expressionAttributeValues,
		// CRITICAL FIX: Add conditional check to ensure we still own the lease
		// This prevents race conditions where another worker has taken the lease
		ConditionExpression: aws.String("AssignedTo = :current_owner OR attribute_not_exists(AssignedTo)"),
	}

	checkpointer.log.Debugf("Checkpointing shard %s: checkpoint=%s, owner=%s, stickyOwner=%s",
		shard.ID, shard.GetCheckpoint(), currentOwner, shard.GetStickyOwner())

	_, err := checkpointer.svc.UpdateItem(context.Background(), input)

	// Handle conditional check failure (lost ownership)
	if err != nil {
		var conditionalCheckErr *types.ConditionalCheckFailedException
		if errors.As(err, &conditionalCheckErr) {
			checkpointer.log.Warnf("Lost ownership of shard %s during checkpoint, cannot update. "+
				"Another worker has likely taken the lease.", shard.ID)
			return ErrLeaseNotAcquired{"lost lease ownership during checkpoint"}
		}
		checkpointer.log.Errorf("Failed to checkpoint shard %s: %v", shard.ID, err)
		return err
	}

	checkpointer.log.Debugf("Successfully checkpointed shard %s at sequence %s", shard.ID, shard.GetCheckpoint())
	return nil
}

// FetchCheckpoint retrieves the checkpoint for the given shard
func (checkpointer *DynamoCheckpoint) FetchCheckpoint(shard *par.ShardStatus) error {
	checkpoint, err := checkpointer.getItem(shard.ID)
	if err != nil {
		return err
	}

	sequenceID, ok := checkpoint[SequenceNumberKey]
	if !ok {
		return ErrSequenceIDNotFound
	}

	checkpointer.log.Debugf("Retrieved Shard Iterator %s", sequenceID.(*types.AttributeValueMemberS).Value)
	shard.SetCheckpoint(sequenceID.(*types.AttributeValueMemberS).Value)

	if assignedTo, ok := checkpoint[LeaseOwnerKey]; ok {
		shard.SetLeaseOwner(assignedTo.(*types.AttributeValueMemberS).Value)
	}

	// Load sticky owner if present
	if stickyOwner, ok := checkpoint[StickyOwnerKey]; ok {
		shard.SetStickyOwner(stickyOwner.(*types.AttributeValueMemberS).Value)
	}

	// Use up-to-date leaseTimeout to avoid ConditionalCheckFailedException when claiming
	if leaseTimeout, ok := checkpoint[LeaseTimeoutKey]; ok && leaseTimeout.(*types.AttributeValueMemberS).Value != "" {
		currentLeaseTimeout, err := time.Parse(time.RFC3339Nano, leaseTimeout.(*types.AttributeValueMemberS).Value)
		if err != nil {
			return err
		}
		shard.LeaseTimeout = currentLeaseTimeout
	}

	return nil
}

// RemoveLeaseInfo to remove lease info for shard entry in dynamoDB because the shard no longer exists in Kinesis
func (checkpointer *DynamoCheckpoint) RemoveLeaseInfo(shardID string) error {
	err := checkpointer.removeItem(shardID)

	if err != nil {
		checkpointer.log.Errorf("Error in removing lease info for shard: %s, Error: %+v", shardID, err)
	} else {
		checkpointer.log.Infof("Lease info for shard: %s has been removed.", shardID)
	}

	return err
}

// RemoveLeaseOwner to remove lease owner for the shard entry
func (checkpointer *DynamoCheckpoint) RemoveLeaseOwner(shardID string) error {
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]types.AttributeValue{
			LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
		UpdateExpression: aws.String("remove " + LeaseOwnerKey),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":assigned_to": &types.AttributeValueMemberS{
				Value: checkpointer.kclConfig.WorkerID,
			},
		},
		ConditionExpression: aws.String("AssignedTo = :assigned_to"),
	}

	_, err := checkpointer.svc.UpdateItem(context.TODO(), input)

	return err
}

// GetLeaseOwner returns current lease owner of given shard in checkpoints table
func (checkpointer *DynamoCheckpoint) GetLeaseOwner(shardID string) (string, error) {
	currentCheckpoint, err := checkpointer.getItem(shardID)
	if err != nil {
		return "", err
	}

	assignedVar, assignedToOk := currentCheckpoint[LeaseOwnerKey]

	if !assignedToOk {
		return "", NoLeaseOwnerErr
	}

	return assignedVar.(*types.AttributeValueMemberS).Value, nil

}

// ListActiveWorkers returns a map of workers and their shards
func (checkpointer *DynamoCheckpoint) ListActiveWorkers(shardStatus map[string]*par.ShardStatus) (map[string][]*par.ShardStatus, error) {
	err := checkpointer.syncLeases(shardStatus)
	if err != nil {
		return nil, err
	}

	workers := map[string][]*par.ShardStatus{}
	for _, shard := range shardStatus {
		if shard.GetCheckpoint() == ShardEnd {
			continue
		}

		leaseOwner := shard.GetLeaseOwner()
		if leaseOwner == "" {
			checkpointer.log.Debugf("Shard Not Assigned Error. ShardID: %s, WorkerID: %s", shard.ID, checkpointer.kclConfig.WorkerID)
			return nil, ErrShardNotAssigned
		}

		if w, ok := workers[leaseOwner]; ok {
			workers[leaseOwner] = append(w, shard)
		} else {
			workers[leaseOwner] = []*par.ShardStatus{shard}
		}
	}
	return workers, nil
}

// ClaimShard places a claim request on a shard to signal a steal attempt
func (checkpointer *DynamoCheckpoint) ClaimShard(shard *par.ShardStatus, claimID string) error {
	err := checkpointer.FetchCheckpoint(shard)
	if err != nil && err != ErrSequenceIDNotFound {
		return err
	}
	leaseTimeoutString := shard.GetLeaseTimeout().Format(time.RFC3339Nano)

	conditionalExpression := `ShardID = :id AND LeaseTimeout = :lease_timeout AND attribute_not_exists(ClaimRequest)`
	expressionAttributeValues := map[string]types.AttributeValue{
		":id": &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		":lease_timeout": &types.AttributeValueMemberS{
			Value: leaseTimeoutString,
		},
	}

	marshalledCheckpoint := map[string]types.AttributeValue{
		LeaseKeyKey: &types.AttributeValueMemberS{
			Value: shard.ID,
		},
		LeaseTimeoutKey: &types.AttributeValueMemberS{
			Value: leaseTimeoutString,
		},
		SequenceNumberKey: &types.AttributeValueMemberS{
			Value: shard.Checkpoint,
		},
		ClaimRequestKey: &types.AttributeValueMemberS{
			Value: claimID,
		},
	}

	if leaseOwner := shard.GetLeaseOwner(); leaseOwner == "" {
		conditionalExpression += " AND attribute_not_exists(AssignedTo)"
	} else {
		marshalledCheckpoint[LeaseOwnerKey] = &types.AttributeValueMemberS{Value: leaseOwner}
		conditionalExpression += "AND AssignedTo = :assigned_to"
		expressionAttributeValues[":assigned_to"] = &types.AttributeValueMemberS{Value: leaseOwner}
	}

	// Preserve StickyOwner if present
	if stickyOwner := shard.GetStickyOwner(); stickyOwner != "" {
		marshalledCheckpoint[StickyOwnerKey] = &types.AttributeValueMemberS{Value: stickyOwner}
	}

	if checkpoint := shard.GetCheckpoint(); checkpoint == "" {
		conditionalExpression += " AND attribute_not_exists(Checkpoint)"
	} else if checkpoint == ShardEnd {
		conditionalExpression += " AND Checkpoint <> :checkpoint"
		expressionAttributeValues[":checkpoint"] = &types.AttributeValueMemberS{Value: ShardEnd}
	} else {
		conditionalExpression += " AND Checkpoint = :checkpoint"
		expressionAttributeValues[":checkpoint"] = &types.AttributeValueMemberS{Value: checkpoint}
	}

	if shard.ParentShardId == "" {
		conditionalExpression += " AND attribute_not_exists(ParentShardId)"
	} else {
		marshalledCheckpoint[ParentShardIdKey] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
		conditionalExpression += " AND ParentShardId = :parent_shard"
		expressionAttributeValues[":parent_shard"] = &types.AttributeValueMemberS{Value: shard.ParentShardId}
	}

	return checkpointer.conditionalUpdate(conditionalExpression, expressionAttributeValues, marshalledCheckpoint)
}

// GetStickyOwner returns the sticky owner for the given shard
func (checkpointer *DynamoCheckpoint) GetStickyOwner(shardID string) (string, error) {
	currentCheckpoint, err := checkpointer.getItem(shardID)
	if err != nil {
		return "", err
	}

	stickyOwner, ok := currentCheckpoint[StickyOwnerKey]
	if !ok {
		return "", nil
	}

	return stickyOwner.(*types.AttributeValueMemberS).Value, nil
}

func (checkpointer *DynamoCheckpoint) syncLeases(shardStatus map[string]*par.ShardStatus) error {
	log := checkpointer.kclConfig.Logger

	if (checkpointer.lastLeaseSync.Add(time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis) * time.Millisecond)).After(time.Now()) {
		return nil
	}

	checkpointer.lastLeaseSync = time.Now()
	input := &dynamodb.ScanInput{
		ProjectionExpression: aws.String(fmt.Sprintf("%s,%s,%s,%s", LeaseKeyKey, LeaseOwnerKey, SequenceNumberKey, StickyOwnerKey)),
		Select:               "SPECIFIC_ATTRIBUTES",
		TableName:            aws.String(checkpointer.kclConfig.TableName),
		ConsistentRead:       aws.Bool(true),
	}

	scanOutput, err := checkpointer.svc.Scan(context.TODO(), input)

	if err != nil {
		log.Debugf("Error performing DynamoDB Scan. Error: %+v ", err)
		return err
	}

	results := scanOutput.Items
	for _, result := range results {
		shardId, foundShardId := result[LeaseKeyKey]
		assignedTo, foundAssignedTo := result[LeaseOwnerKey]
		checkpoint, foundCheckpoint := result[SequenceNumberKey]
		if !foundShardId || !foundAssignedTo || !foundCheckpoint {
			continue
		}

		if shard, ok := shardStatus[shardId.(*types.AttributeValueMemberS).Value]; ok {
			shard.SetLeaseOwner(assignedTo.(*types.AttributeValueMemberS).Value)
			shard.SetCheckpoint(checkpoint.(*types.AttributeValueMemberS).Value)

			// Load sticky owner if present
			if stickyOwner, hasStickyOwner := result[StickyOwnerKey]; hasStickyOwner {
				shard.SetStickyOwner(stickyOwner.(*types.AttributeValueMemberS).Value)
			}
		}
	}

	log.Debugf("Lease sync completed. Next lease sync will occur in %s", time.Duration(checkpointer.kclConfig.LeaseSyncingTimeIntervalMillis)*time.Millisecond)
	return nil
}

func (checkpointer *DynamoCheckpoint) createTable() error {
	input := &dynamodb.CreateTableInput{
		AttributeDefinitions: []types.AttributeDefinition{
			{
				AttributeName: aws.String(LeaseKeyKey),
				AttributeType: types.ScalarAttributeTypeS,
			},
		},
		KeySchema: []types.KeySchemaElement{
			{
				AttributeName: aws.String(LeaseKeyKey),
				KeyType:       types.KeyTypeHash,
			},
		},
		ProvisionedThroughput: &types.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(checkpointer.leaseTableReadCapacity),
			WriteCapacityUnits: aws.Int64(checkpointer.leaseTableWriteCapacity),
		},
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.CreateTable(context.Background(), input)

	return err
}

func (checkpointer *DynamoCheckpoint) doesTableExist() bool {
	input := &dynamodb.DescribeTableInput{
		TableName: aws.String(checkpointer.TableName),
	}
	_, err := checkpointer.svc.DescribeTable(context.Background(), input)

	return err == nil
}

func (checkpointer *DynamoCheckpoint) saveItem(item map[string]types.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		TableName: aws.String(checkpointer.TableName),
		Item:      item,
	})
}

func (checkpointer *DynamoCheckpoint) conditionalUpdate(conditionExpression string, expressionAttributeValues map[string]types.AttributeValue, item map[string]types.AttributeValue) error {
	return checkpointer.putItem(&dynamodb.PutItemInput{
		ConditionExpression:       aws.String(conditionExpression),
		TableName:                 aws.String(checkpointer.TableName),
		Item:                      item,
		ExpressionAttributeValues: expressionAttributeValues,
	})
}

func (checkpointer *DynamoCheckpoint) putItem(input *dynamodb.PutItemInput) error {
	_, err := checkpointer.svc.PutItem(context.Background(), input)
	return err
}

func (checkpointer *DynamoCheckpoint) getItem(shardID string) (map[string]types.AttributeValue, error) {
	if !checkpointer.kclConfig.EnableRepeatableRead {
		// Standard single read with strong consistency
		return checkpointer.getItemSingle(shardID)
	}

	// Repeatable read: read multiple times and verify consistency
	return checkpointer.getItemRepeatable(shardID)
}

func (checkpointer *DynamoCheckpoint) getItemSingle(shardID string) (map[string]types.AttributeValue, error) {
	item, err := checkpointer.svc.GetItem(context.Background(), &dynamodb.GetItemInput{
		TableName:      aws.String(checkpointer.TableName),
		ConsistentRead: aws.Bool(true),
		Key: map[string]types.AttributeValue{
			LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})

	// fix problem when starts the environment from scratch (dynamo table is empty)
	if item == nil {
		return nil, err
	}

	return item.Item, err
}

func (checkpointer *DynamoCheckpoint) getItemRepeatable(shardID string) (map[string]types.AttributeValue, error) {
	log := checkpointer.kclConfig.Logger
	attempts := checkpointer.kclConfig.RepeatableReadAttempts
	if attempts < 2 {
		attempts = 2 // Minimum 2 reads for verification
	}

	var lastRead map[string]types.AttributeValue
	var lastErr error

	for i := 0; i < attempts; i++ {
		if i > 0 {
			// Sleep between reads to allow any pending writes to complete
			time.Sleep(time.Duration(checkpointer.kclConfig.RepeatableReadDelayMillis) * time.Millisecond)
		}

		item, err := checkpointer.svc.GetItem(context.Background(), &dynamodb.GetItemInput{
			TableName:      aws.String(checkpointer.TableName),
			ConsistentRead: aws.Bool(true),
			Key: map[string]types.AttributeValue{
				LeaseKeyKey: &types.AttributeValueMemberS{
					Value: shardID,
				},
			},
		})

		if err != nil {
			log.Warnf("Repeatable read attempt %d/%d failed for shard %s: %v", i+1, attempts, shardID, err)
			lastErr = err
			continue
		}

		if item == nil {
			return nil, err
		}

		currentRead := item.Item

		// First read - store as baseline
		if i == 0 {
			lastRead = currentRead
			continue
		}

		// Compare with previous read
		if checkpointer.areItemsEqual(lastRead, currentRead, shardID) {
			// Values are consistent across reads - return the latest
			log.Debugf("Repeatable read verification passed for shard %s after %d reads", shardID, i+1)
			return currentRead, nil
		}

		// Values differ - continue reading
		log.Warnf("Repeatable read inconsistency detected for shard %s at attempt %d/%d", shardID, i+1, attempts)
		lastRead = currentRead
	}

	// After all attempts, return the last successful read
	if lastRead != nil {
		log.Warnf("Repeatable read completed for shard %s after %d attempts, using latest read", shardID, attempts)
		return lastRead, nil
	}

	return nil, lastErr
}

// areItemsEqual compares two DynamoDB items for equality on critical lease fields
func (checkpointer *DynamoCheckpoint) areItemsEqual(item1, item2 map[string]types.AttributeValue, shardID string) bool {
	if item1 == nil && item2 == nil {
		return true
	}
	if item1 == nil || item2 == nil {
		return false
	}

	// Compare critical fields for lease management
	criticalFields := []string{LeaseOwnerKey, LeaseTimeoutKey, SequenceNumberKey, StickyOwnerKey, ClaimRequestKey}

	for _, field := range criticalFields {
		val1, ok1 := item1[field]
		val2, ok2 := item2[field]

		// If field exists in one but not the other
		if ok1 != ok2 {
			checkpointer.log.Debugf("Field %s existence mismatch for shard %s", field, shardID)
			return false
		}

		// If field exists in both, compare values
		if ok1 && ok2 {
			str1 := ""
			str2 := ""

			if memberS1, ok := val1.(*types.AttributeValueMemberS); ok {
				str1 = memberS1.Value
			}
			if memberS2, ok := val2.(*types.AttributeValueMemberS); ok {
				str2 = memberS2.Value
			}

			if str1 != str2 {
				checkpointer.log.Debugf("Field %s value mismatch for shard %s: '%s' vs '%s'", field, shardID, str1, str2)
				return false
			}
		}
	}

	return true
}

func (checkpointer *DynamoCheckpoint) removeItem(shardID string) error {
	_, err := checkpointer.svc.DeleteItem(context.Background(), &dynamodb.DeleteItemInput{
		TableName: aws.String(checkpointer.TableName),
		Key: map[string]types.AttributeValue{
			LeaseKeyKey: &types.AttributeValueMemberS{
				Value: shardID,
			},
		},
	})

	return err
}
