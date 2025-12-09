-- Aggregated Daily Deposits for Contract 0xe93131620945a1273b48f57f453983d270b62dc7
-- Method: 0x47e7ef24 (Deposit)
-- Token Mint: 0x00...00 -> Callee (User) (Decimals: 6)
-- USDC Deposit: Callee (User) -> Contract (Decimals: 6)

SELECT
    DATE_TRUNC('day', tx.block_time) AS time_period,
    SUM(usdc_transfer.value / 1e6) AS total_usdc_deposited,
    SUM(SUM(usdc_transfer.value / 1e6)) OVER (ORDER BY DATE_TRUNC('day', tx.block_time)) AS cumulative_usdc_deposited,
    COUNT(*) AS deposit_count
FROM base.transactions tx
JOIN erc20_base.evt_Transfer usdc_transfer
    ON tx.hash = usdc_transfer.evt_tx_hash
JOIN erc20_base.evt_Transfer token_mint
    ON tx.hash = token_mint.evt_tx_hash
WHERE tx.to = 0xe93131620945a1273b48f57f453983d270b62dc7
AND bytearray_substring(tx.data, 1, 4) = 0x47e7ef24
-- Logic for USDC Deposit (User -> Contract)
AND usdc_transfer.contract_address = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC Base
AND usdc_transfer."from" = tx."from"
AND usdc_transfer."to" = 0xe93131620945a1273b48f57f453983d270b62dc7
-- Logic for Token Mint (Zero -> User)
AND token_mint.contract_address = 0xe93131620945a1273b48f57f453983d270b62dc7 -- Our Contract/Token
AND token_mint."from" = 0x0000000000000000000000000000000000000000
AND token_mint."to" = tx."from"
GROUP BY 1
ORDER BY 1 DESC
