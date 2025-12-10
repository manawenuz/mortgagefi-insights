-- Daily APR Calculation for MortgageFi Pool
-- Contract: 0xE93131620945A1273b48F57f453983d270b62DC7
-- Formula from contract: APR = buffer * 10000 * 365 / 30 / totalSupply
-- Where buffer = accumulated fees from payments (90% of fee portion)
-- Payment Event: 0x9b91293b84cf4e5a5368d30b768220e06528807714c10e224375fc2cce6e0a1d
-- Transfer Event: 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef

WITH 
-- Track all payments (yield source)
payments AS (
    SELECT
        DATE_TRUNC('day', block_time) as day,
        block_time,
        -- paymentSize is in data (uint256)
        bytearray_to_uint256(data) as payment_size
    FROM base.logs
    WHERE contract_address = 0xE93131620945A1273b48F57f453983d270b62DC7
    AND topic0 = 0x9b91293b84cf4e5a5368d30b768220e06528807714c10e224375fc2cce6e0a1d
    AND block_number >= 30513551
),

-- Daily payment totals
daily_payments AS (
    SELECT
        day,
        SUM(payment_size) as total_payments,
        COUNT(*) as num_payments
    FROM payments
    GROUP BY 1
),

-- Track mints and burns for totalSupply
-- Mint: from = 0x0, Burn: to = 0x0
supply_changes AS (
    SELECT
        DATE_TRUNC('day', block_time) as day,
        block_time,
        -- topic1 = from (indexed), topic2 = to (indexed)
        topic1 as from_addr,
        topic2 as to_addr,
        bytearray_to_uint256(data) as amount
    FROM base.logs
    WHERE contract_address = 0xE93131620945A1273b48F57f453983d270b62DC7
    AND topic0 = 0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef
    AND block_number >= 30513551
),

-- Calculate net supply change per day
daily_supply_delta AS (
    SELECT
        day,
        SUM(CASE 
            -- Mint: from is zero address (32 bytes of zeros)
            WHEN from_addr = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN CAST(amount AS INT256)
            -- Burn: to is zero address
            WHEN to_addr = 0x0000000000000000000000000000000000000000000000000000000000000000 THEN -1 * CAST(amount AS INT256)
            ELSE 0 
        END) as net_change
    FROM supply_changes
    GROUP BY 1
),

-- Cumulative supply timeline
supply_timeline AS (
    SELECT
        day,
        SUM(net_change) OVER (ORDER BY day ASC) as total_supply
    FROM daily_supply_delta
),

-- Estimate buffer: ~90% of fees go to buffer
-- Fees are roughly proportional to payments (varies by contract terms)
-- Using a simplified estimate: buffer accumulates ~10-15% of payments as fees, 90% of that to buffer
-- So buffer_addition ≈ payment * 0.12 * 0.9 ≈ payment * 0.108
-- But buffer also depletes over 30 days via giveYield
-- For simplicity, estimate rolling 30-day payment fees as proxy for buffer
rolling_buffer AS (
    SELECT
        day,
        -- Sum payments over last 30 days, estimate ~10% as buffer contribution
        SUM(total_payments) OVER (
            ORDER BY day 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) * 0.10 as estimated_buffer
    FROM daily_payments
)

-- Final APR calculation (sampled daily) with moving averages
SELECT
    COALESCE(p.day, s.day) as day,
    COALESCE(p.total_payments, 0) / 1e6 as daily_payments_usdc,
    p.num_payments,
    s.total_supply / 1e6 as total_supply_tokens,
    b.estimated_buffer / 1e6 as estimated_buffer_usdc,
    -- Daily APR (one snapshot per day)
    CASE 
        WHEN s.total_supply > 0 AND b.estimated_buffer > 0 
        THEN (b.estimated_buffer * 10000.0 * 365 / 30) / s.total_supply / 100.0
        ELSE 0 
    END as daily_apr_percent,
    -- 7-day moving average APR
    AVG(CASE 
        WHEN s.total_supply > 0 AND b.estimated_buffer > 0 
        THEN (b.estimated_buffer * 10000.0 * 365 / 30) / s.total_supply / 100.0
        ELSE 0 
    END) OVER (ORDER BY COALESCE(p.day, s.day) ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as apr_7d_avg,
    -- 30-day moving average APR
    AVG(CASE 
        WHEN s.total_supply > 0 AND b.estimated_buffer > 0 
        THEN (b.estimated_buffer * 10000.0 * 365 / 30) / s.total_supply / 100.0
        ELSE 0 
    END) OVER (ORDER BY COALESCE(p.day, s.day) ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) as apr_30d_avg,
    -- 60-day moving average APR
    AVG(CASE 
        WHEN s.total_supply > 0 AND b.estimated_buffer > 0 
        THEN (b.estimated_buffer * 10000.0 * 365 / 30) / s.total_supply / 100.0
        ELSE 0 
    END) OVER (ORDER BY COALESCE(p.day, s.day) ROWS BETWEEN 59 PRECEDING AND CURRENT ROW) as apr_60d_avg
FROM daily_payments p
FULL OUTER JOIN supply_timeline s ON p.day = s.day
LEFT JOIN rolling_buffer b ON p.day = b.day
WHERE COALESCE(p.day, s.day) IS NOT NULL
ORDER BY 1 DESC
