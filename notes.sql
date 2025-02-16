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
