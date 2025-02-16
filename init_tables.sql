CREATE TABLE public.candles (
    pair VARCHAR,
    time_frame VARCHAR,
    o REAL,
    h REAL,
    l REAL,
    c REAL,
    utc_begin TIMESTAMP,
    volume_bs__buy_base REAL,
    volume_bs__sell_base REAL,
    volume_bs__buy_quote REAL,
    volume_bs__sell_quote REAL
);
