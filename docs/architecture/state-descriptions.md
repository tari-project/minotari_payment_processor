#### State Descriptions

| Status | Worker Responsible | Description |
| :--- | :--- | :--- |
| **PENDING_BATCHING** | `Batch Creator` / `Broadcaster` | A new batch created, OR a batch that has completed a consolidation cycle (Split) and is waiting for the final transaction to be built using the new UTXOs. |
| **AWAITING_SIGNATURE** | `Unsigned TX Creator` | The transaction structure (unsigned) has been retrieved. It may be a CoinJoin (Split) or a Final Payment. |
| **SIGNING_IN_PROGRESS** | `Transaction Signer` | A worker has picked up the batch and is calculating signatures. |
| **AWAITING_BROADCAST** | `Transaction Signer` | The transaction is fully signed and stored in the DB. |
| **BROADCASTING** | `Broadcaster` | Submitting transactions. If `is_consolidation=true`, it verifies mempool presence and loops status back to `PENDING_BATCHING`. If `false`, moves to `AWAITING_CONFIRMATION`. |
| **AWAITING_CONFIRMATION** | `Broadcaster` / `Confirmation Checker` | The final transaction was accepted. System polls for block depth. Can also be set by reorg handling when a confirmed batch is detected to have been reorged. |
| **CONFIRMED** | `Confirmation Checker` | The transaction has reached the required block depth. |
| **FAILED** | All | Terminal error state. |

### Reorg Handling

The `Confirmation Checker` worker implements robust chain reorganization detection by maintaining a local cache of block header hashes.

#### Header Tracking

- The system stores the last **2000 block headers** in the `block_headers` table
- Each header includes: `height`, `header_hash`, `prev_hash`
- Headers older than 2000 blocks from the tip are automatically pruned

#### Reorg Detection Algorithm

On each confirmation check cycle:

1. **Get new tip info** from the base node (height, hash, prev_hash)
2. **Compare with stored tip**:
   - **Same hash**: No change, continue normally
   - **New tip extends stored tip**: Normal block progression (prev_hash matches stored tip hash)
   - **Mismatch detected**: Potential reorg - find common ancestor

3. **Find common ancestor** (when mismatch detected):
   - Walk back through the chain from the new tip
   - Query the base node for each block's header hash
   - Compare against stored headers
   - The first matching header is the common ancestor
   - The reorg height is `common_ancestor_height + 1`

4. **Handle affected transactions**:
   - Delete invalidated headers at and above the reorg height
   - Find all confirmed batches mined at or after the reorg height
   - For each affected batch, query the base node to check its current status:
     - **Still mined (same block)**: No action needed
     - **Re-mined in different block**: Update mined info and recalculate `payref` values
     - **In mempool**: Revert batch to `AWAITING_CONFIRMATION`, payments to `BATCHED`
     - **Not found**: Same as mempool case - full revert

5. **Update stored headers** with the new chain data

#### Benefits of This Approach

- **Accurate detection**: By tracking actual header hashes, we can detect reorgs precisely
- **Efficient**: Only affected transactions are checked, not all confirmed transactions
- **Deep protection**: With 2000 headers, we can detect reorgs up to ~2000 blocks deep
- **Chain continuity**: By checking if new tip links to our stored tip via prev_hash, we detect any chain discontinuity
