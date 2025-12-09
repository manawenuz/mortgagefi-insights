-- Defaulted NFTs for Contract 0xe93131620945a1273b48f57f453983d270b62dc7
-- Method: 0x9abca031 (defaultContract)
-- Parameter: _nftID (uint256) - bytes 5-36 of data
-- NFT Contract: 0x042AB03a3493e289134C85A7eEC62871f3703492
-- cbBTC Contract: 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf

WITH defaults AS (
    SELECT
        tx.block_time,
        bytearray_to_uint256(bytearray_substring(tx.data, 5, 32)) AS nft_id,
        tx.hash AS default_tx_hash
    FROM base.transactions tx
    WHERE tx.to = 0xe93131620945a1273b48f57f453983d270b62dc7
    AND bytearray_substring(tx.data, 1, 4) = 0x9abca031
),
nft_mints AS (
    SELECT
        evt_tx_hash AS mint_tx_hash,
        tokenId AS nft_id
    FROM erc721_base.evt_Transfer
    WHERE contract_address IN (
        0x042AB03a3493e289134C85A7eEC62871f3703492,
        0xcc9a350c5b1e1c9ecd23d376e6618cdfd6bbbdbe
    )
    AND "from" = 0x0000000000000000000000000000000000000000
),
nft_values AS (
    SELECT
        m.nft_id,
        -- Sum cbBTC in the mint transaction (Decimals: 8)
        SUM(t.value) / 1e8 AS cbbtc_size
    FROM nft_mints m
    JOIN erc20_base.evt_Transfer t ON m.mint_tx_hash = t.evt_tx_hash
    WHERE t.contract_address = 0xcbB7C0000aB88B473b1f5aFd9ef808440eed33Bf -- cbBTC
    GROUP BY 1
)
SELECT
    d.block_time AS date,
    d.nft_id,
    COALESCE(v.cbbtc_size, 0) AS cbbtc_size,
    d.default_tx_hash,
    COUNT(*) OVER (ORDER BY d.block_time) AS total_defaulted_nfts,
    SUM(COALESCE(v.cbbtc_size, 0)) OVER (ORDER BY d.block_time) AS total_defaulted_cbbtc_volume
FROM defaults d
LEFT JOIN nft_values v ON d.nft_id = v.nft_id
ORDER BY d.block_time DESC
