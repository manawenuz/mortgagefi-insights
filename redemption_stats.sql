-- Redemption Stats for Contract 0xB7CAa3eAa9826A4d489E0260A1f582116A53C7df
-- Tracks "Amounts waiting for redemptions" via inflow (deposits) - withdrawal events
-- Contract created at block 36259651

-- Contract: 0xB7CAa3eAa9826A4d489E0260A1f582116A53C7df (Redeemer)
-- Events (from ABI):
--   inflow(address indexed player, uint256 indexed amount) - emitted on deposit()
--   withdrawal(address indexed player, uint256 indexed amount) - emitted on withdraw()
-- Key state variable: totalNotRedeemed = USD deposited waiting for redemption

WITH inflow_events AS (
    -- deposit() emits inflow(who, amount) where amount is indexed (topic2)
    SELECT
        block_time,
        block_number,
        tx_hash,
        bytearray_to_uint256(topic2) / 1e6 as amount  -- 6 decimals for earnStable
    FROM base.logs
    WHERE contract_address = 0xB7CAa3eAa9826A4d489E0260A1f582116A53C7df
    AND topic0 = 0x57088b038e35a53998d4e5a58200fc48d897496d01c0c6b6079e227829de2173  -- inflow(address,uint256)
    AND block_number >= 36259651
),

withdrawal_events AS (
    -- withdraw() emits withdrawal(player, amount) where amount is indexed (topic2)
    SELECT
        block_time,
        block_number,
        tx_hash,
        bytearray_to_uint256(topic2) / 1e6 as amount
    FROM base.logs
    WHERE contract_address = 0xB7CAa3eAa9826A4d489E0260A1f582116A53C7df
    AND topic0 = 0x5a6b26bafd3957bf06ea3b4915f51221953ed272573c779820e6e39df3e646cf  -- withdrawal(address,uint256)
    AND block_number >= 36259651
),

-- Combine all events
all_events AS (
    SELECT block_time, 'inflow' as event_type, amount FROM inflow_events
    UNION ALL
    SELECT block_time, 'withdrawal' as event_type, amount FROM withdrawal_events
),

daily_flows AS (
    SELECT
        DATE_TRUNC('day', block_time) as day,
        SUM(CASE WHEN event_type = 'inflow' THEN amount ELSE 0 END) as volume_deposited,
        SUM(CASE WHEN event_type = 'withdrawal' THEN amount ELSE 0 END) as volume_redeemed
    FROM all_events
    GROUP BY 1
),

stats_with_cumulative AS (
    SELECT
        day,
        volume_deposited,
        volume_redeemed,
        -- Running total = totalNotRedeemed (USD deposited waiting for redemption)
        SUM(volume_deposited - volume_redeemed) OVER (ORDER BY day ASC) as amount_waiting_for_redemption
    FROM daily_flows
)

SELECT
    day,
    volume_deposited as "Volume Deposited (Inflow)",
    volume_redeemed as "Volume Redeemed (Withdrawals)",
    amount_waiting_for_redemption as "Amounts Waiting for Redemptions",
    -- Metric: Avg waiting time to redeem
    -- Approximation: Queue Size / Daily Redemption Rate = Days to clear current queue
    CASE 
        WHEN volume_redeemed > 0 THEN amount_waiting_for_redemption / volume_redeemed 
        ELSE NULL 
    END as "Est. Avg Waiting Time (Days)"
FROM stats_with_cumulative
ORDER BY day DESC
