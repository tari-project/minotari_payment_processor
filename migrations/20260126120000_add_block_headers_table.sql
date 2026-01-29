-- Create table to store block header hashes for reorg detection
-- This table stores the last 2000 block headers to detect chain reorganizations
CREATE TABLE block_headers (
    -- The block height
    height BIGINT PRIMARY KEY NOT NULL,
    -- The block header hash (hex encoded)
    header_hash TEXT NOT NULL,
    -- The previous block's header hash (hex encoded) for chain linking
    prev_hash TEXT NOT NULL,
    -- Timestamp when this header was recorded
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Index for efficient lookup by header hash
CREATE INDEX idx_block_headers_hash ON block_headers(header_hash);
