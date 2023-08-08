-- CREATE MATERIALIZED VIEW
--     candlestick_1m
-- WITH (timescaledb.continuous) AS
--
-- SELECT
--     time_bucket('1 minute'::INTERVAL, to_timestamp(timestamp)) AS bucket,
--     token0_id as token_id,
--     candlestick_agg(
--         to_timestamp(timestamp),
--         abs(amount_usd/amount0),
--         amount_usd
--     ) as candlestick
-- FROM swap
--     WHERE
--         amount_usd!=0
--     AND
--         amount0!=0
--
-- GROUP BY
--     bucket,
--     token0_id
-- ORDER BY
--     bucket,
--     token0_id
-- WITH NO DATA;
