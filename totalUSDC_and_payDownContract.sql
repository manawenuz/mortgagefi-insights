-- Simplified Query: Daily and Weekly USDC Transfers to Contract
-- Contract: 0xe93131620945a1273b48f57f453983d270b62dc7
-- USDC (Base): 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913

-- Query 1: Daily Aggregation with PayDown Separation
SELECT
    DATE_TRUNC('day', t.evt_block_time) AS time_period,
    SUM(t.value / 1e6) AS total_usdc_volume,
    -- Check if function selector matches payDownContract (0x857ff689)
    SUM(CASE WHEN bytearray_substring(tx.data, 1, 4) = 0x857ff689 THEN t.value / 1e6 ELSE 0 END) AS paydown_usdc_volume,
    COUNT(*) AS total_transfers,
    COUNT(CASE WHEN bytearray_substring(tx.data, 1, 4) = 0x857ff689 THEN 1 END) AS paydown_transfers
FROM erc20_base.evt_Transfer t
JOIN base.transactions tx ON t.evt_tx_hash = tx.hash
WHERE t.contract_address = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC on Base
AND t."to" = 0xE93131620945A1273b48F57f453983d270b62DC7 -- Target Contract
GROUP BY 1
ORDER BY 1 DESC

-- Query 2: Weekly Aggregation with PayDown Separation
/*
SELECT
    DATE_TRUNC('week', t.evt_block_time) AS time_period,
    SUM(t.value / 1e6) AS total_usdc_volume,
    SUM(CASE WHEN bytearray_substring(tx.data, 1, 4) = 0x857ff689 THEN t.value / 1e6 ELSE 0 END) AS paydown_usdc_volume,
    COUNT(*) AS total_transfers,
    COUNT(CASE WHEN bytearray_substring(tx.data, 1, 4) = 0x857ff689 THEN 1 END) AS paydown_transfers
FROM erc20_base.evt_Transfer t
JOIN base.transactions tx ON t.evt_tx_hash = tx.hash
WHERE t.contract_address = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC on Base
AND t."to" = 0xE93131620945A1273b48F57f453983d270b62DC7 -- Target Contract
GROUP BY 1
ORDER BY 1 DESC
*/
