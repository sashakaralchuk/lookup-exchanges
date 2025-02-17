CREATE TABLE default.klines (
    pair String,
    time_frame String,
    o Float64,
    h Float64,
    l Float64,
    c Float64,
    utc_begin DateTime,
    volume_bs__buy_base Float64,
    volume_bs__sell_base Float64,
    volume_bs__buy_quote Float64,
    volume_bs__sell_quote Float64
)
ENGINE = TinyLog;
--
CREATE TABLE default.temp_klines_2025_02_17 (
    pair String,
    time_frame String,
    utc_begin DateTime,
    o Float64,
    h Float64,
    l Float64,
    c Float64
)
ENGINE = TinyLog;
--
INSERT INTO default.temp_klines_2025_02_17
SELECT
    'BTC_USDT' pair,
    '1m' time_frame,
    toDateTime(tupleElement(t, 'startTime') / 1000) utc_begin,
    toFloat64(tupleElement(t, 'open')) o,
    toFloat64(tupleElement(t, 'high')) h,
    toFloat64(tupleElement(t, 'low')) l,
    toFloat64(tupleElement(t, 'close')) c
FROM (
    SELECT
        JSONExtract(
            arrayJoin(JSONExtractArrayRaw(line)),
            'Tuple(low String, high String, open String, close String, amount String, quantity String, buyTakerAmount String, buyTakerQuantity String, tradeCount UInt64, ts UInt64, weightedAverage String, interval String, startTime UInt64, closeTime UInt64)'
        ) t
    FROM url('https://api.poloniex.com/markets/BTC_USDT/candles?interval=MINUTE_1&startTime=1739750400000&limit=500', 'LineAsString')
);
--
SELECT t1.utc_begin, t2.utc_begin,
    t1.o - t2.o diff_o,
    t1.o - t2.o diff_h,
    t1.o - t2.o diff_l,
    t1.o - t2.o diff_c
FROM default.temp_klines_2025_02_17 t1
LEFT JOIN (
    SELECT *
    FROM default.klines
    WHERE pair = 'BTC_USDT' AND time_frame = '1m'
) t2
    ON t1.utc_begin = t2.utc_begin
WHERE t2.utc_begin != 0;
