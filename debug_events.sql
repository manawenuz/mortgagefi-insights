-- Check for ANY events emitted by the contract
-- Contract: 0xE93131620945A1273b48F57f453983d270b62DC7

SELECT
    block_time,
    block_number,
    tx_hash,
    topic0, -- The event signature hash
    data -- The data emitted
FROM base.logs
WHERE contract_address = 0xE93131620945A1273b48F57f453983d270b62DC7
ORDER BY block_time DESC
LIMIT 50
