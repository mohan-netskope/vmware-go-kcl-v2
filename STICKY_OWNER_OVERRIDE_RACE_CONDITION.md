1. Worker-A owns shard-001 with StickyOwner=Worker-A 
2. External tool reassign to Worker-B:
   - Updates DynamoDB: SET StickyOwner = Worker-B

3. Worker-A still processing (hasn't lost lease yet):
   - In-memory: StickyOwner=Worker-A (stale)
   - Processing records and do checkpointing 
   - checkpoint overwrites StickyOwner back to Worker-A


### Solution 1: UpdateItem Instead of PutItem

**Before (PutItem)**:
```go
// Replaces ENTIRE item in DynamoDB
return checkpointer.saveItem(marshalledCheckpoint)  // Uses PutItem
```

**After (UpdateItem)**:
```go
// Only updates SPECIFIED fields, preserves others
input := &dynamodb.UpdateItemInput{
    UpdateExpression: aws.String("SET #cp = :cp, #at = :at, #lt = :lt"),
    // StickyOwner NOT included in update if empty in memory!
}
```

**Impact**:
- If Worker-A has empty/stale `StickyOwner` in memory, it's NOT included in the update
- DynamoDB's current `StickyOwner` value is **preserved**
- External tool's assignment is **not overwritten**

### Solution 2: Conditional StickyOwner Update

**New Logic**:
```go
// Only update StickyOwner if we have a value in memory
if stickyOwner := shard.GetStickyOwner(); stickyOwner != "" {
    updateExpression += ", #so = :so"
    expressionAttributeNames["#so"] = StickyOwnerKey
    expressionAttributeValues[":so"] = &types.AttributeValueMemberS{Value: stickyOwner}
}
// If empty/stale in memory: DON'T include in update → preserves DynamoDB value
```

**Benefits**:
- Worker only updates `StickyOwner` if it has a valid value
- Prevents accidental overwrite with stale values
- External assignments are preserved

### Solution 3: Atomic Ownership Verification

**New Conditional Expression**:
```go
ConditionExpression: aws.String("AssignedTo = :current_owner OR attribute_not_exists(AssignedTo)")
```

**Why This Helps**:
- Even if Worker-A has stale `StickyOwner`, it can only checkpoint if it still owns the lease
- If Worker-A lost the lease, the conditional check **fails**
- This prevents any overwrite scenario where Worker-A no longer owns the shard
