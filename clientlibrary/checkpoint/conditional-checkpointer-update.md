# CheckpointSequence Logic Explanation

The logic in `CheckpointSequence` is critical for ensuring **data consistency**, **fault tolerance**, and **lease stability** in the Kinesis Client Library (KCL) implementation.

Here is a detailed breakdown of why this specific implementation is needed:

### 1. Persistence of Processing Progress (The "Checkpoint")
The primary purpose is to save the `SequenceNumber` (checkpoint) to DynamoDB.
*   **Why:** If this worker crashes or the shard is rebalanced to another worker, the new worker reads this value to know exactly where to resume processing. Without this, the new worker would start from the beginning (duplicate processing) or the end (data loss) of the stream.

### 2. Implicit Lease Renewal
The function updates `LeaseOwner` and `LeaseTimeout` alongside the checkpoint.
*   **Why:** This acts as a "heartbeat." By updating the timeout, the worker proves it is still active and healthy. If it didn't do this, the lease would eventually expire while the worker was busy processing a large batch of records, causing another worker to "steal" the shard and resulting in two workers processing the same data (split brain).

### 3. Using `UpdateItem` vs. `PutItem` (Crucial Design Choice)
The code explicitly uses `dynamodb.UpdateItem` instead of `PutItem` (which replaces the entire row).

*   **The Problem with `PutItem`:** `PutItem` completely overwrites the existing item. If you used `PutItem` based only on the fields the `shard` object currently knows about, you would accidentally **delete** any other fields stored in DynamoDB that the current memory object might not be aware of.
*   **The Solution (`UpdateItem`):** `UpdateItem` allows for **partial updates**. It only modifies the specific fields listed in the expression (`#cp`, `#at`, `#lt`, etc.).
*   **Benefit:** This allows other tools or processes to add metadata to the DynamoDB record (like tags, custom configuration, or manual sticky assignments) without the worker accidentally wiping them out during a routine checkpoint.

### 4. The Sticky Owner Logic
This is the most complex part of the logic:

```go
// If we have a sticky owner in memory, ensure it's persisted.
// If we don't (empty), we leave the existing value in DynamoDB alone (don't overwrite/remove it).
if stickyOwner := shard.GetStickyOwner(); stickyOwner != "" {
    updateExpression += ", #so = :so"
    expressionAttributeNames["#so"] = StickyOwnerKey
    expressionAttributeValues[":so"] = &types.AttributeValueMemberS{Value: stickyOwner}
}
```

**Why is this conditional logic needed?**

1.  **Enforcing Assignment (If Known):** If the current worker (`shard` object) *knows* this shard is supposed to be sticky (e.g., it was assigned specifically to this worker), it explicitly writes that to DynamoDB. This ensures that if the worker restarts, the persistent store knows "this shard belongs to Worker X."

2.  **Preventing Accidental Deletion (If Unknown):** If `shard.GetStickyOwner()` is empty (the worker doesn't think there is a sticky owner), it **does nothing** regarding that field.
    *   **Scenario:** Imagine an administrator runs a script to manually "pin" a shard to a worker by writing to DynamoDB directly. The current worker might not have refreshed its metadata to see this change yet.
    *   **Risk:** If the code blindly wrote `StickyOwner = ""` (empty) to DynamoDB, it would overwrite the administrator's manual setting.
    *   **Safety:** By omitting the field from the `UpdateItem` expression when empty, the code says: *"I don't have a sticky owner to set, so keep whatever value is already in the database."*
