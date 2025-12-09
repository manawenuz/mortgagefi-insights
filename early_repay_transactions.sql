-- Aggregated Daily Early Repayments (Full) for Contract 0xe93131620945a1273b48f57f453983d270b62dc7
-- Method: 0x6386255a (earlyRepayContractInFull)
-- Flow: 
-- 1. User calls method.
-- 2. User pays USDC to Contract (Repayment).
-- 3. Contract returns cbBTC to User (Collateral Redemption).

WITH tx_data AS (
    SELECT
        tx.hash,
        tx.block_time,
        -- Sum USDC transfers from User to Contract in this transaction (Decimals: 6)
        SUM(CASE 
            WHEN t.contract_address = 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 -- USDC
            AND t."from" = tx."from" 
            AND t."to" = 0xe93131620945a1273b48f57f453983d270b62dc7 
            THEN t.value 
            ELSE 0 
        END) / 1e6 AS usdc_amount,
        
        -- Sum cbBTC transfers from Contract to User in this transaction (Decimals: 8 for cbBTC)
        SUM(CASE 
            WHEN t.contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf -- cbBTC
            AND t."from" = 0xe93131620945a1273b48f57f453983d270b62dc7 
            AND t."to" = tx."from" 
            THEN t.value 
            ELSE 0 
        END) / 1e8 AS cbbtc_amount
    FROM base.transactions tx
    JOIN erc20_base.evt_Transfer t ON tx.hash = t.evt_tx_hash
    WHERE tx.to = 0xe93131620945a1273b48f57f453983d270b62dc7
    AND bytearray_substring(tx.data, 1, 4) = 0x6386255a
    GROUP BY 1, 2
)
SELECT
    DATE_TRUNC('day', block_time) AS time_period,
    SUM(usdc_amount) AS daily_usdc_repaid,
    SUM(cbbtc_amount) AS daily_cbbtc_redeemed,
    SUM(SUM(usdc_amount)) OVER (ORDER BY DATE_TRUNC('day', block_time)) AS cumulative_usdc_repaid,
    SUM(SUM(cbbtc_amount)) OVER (ORDER BY DATE_TRUNC('day', block_time)) AS cumulative_cbbtc_redeemed
FROM tx_data
GROUP BY 1
ORDER BY 1 DESC
