use std::collections::HashMap;

use lookup_exchanges::{
    exchange::poloniex, ChannelWs, ClientPublic, EventWs, Kline, KlineTimeframe, RecentTrade,
};
use std::sync::mpsc::{channel, Receiver, Sender};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    dotenv::dotenv().ok();
    let config = Config::new_from_envs();
    let config_fill_absent_kline = config.clone();
    let client_clickhouse = clickhouse::Client::default().with_url(&config.clickhouse_url);
    let client_clickhouse_curr_kline = client_clickhouse.clone();
    let client_clickhouse_fill_absent_kline = client_clickhouse.clone();
    let (tx_curr_kline, rx_curr_kline): (Sender<Kline>, Receiver<Kline>) = channel();
    let (tx_fill_absent_kline, rx_fill_absent_kline) = channel();
    let mut curr_klines = HashMap::new();
    let mut first_trade_ts = HashMap::new();
    let client = match config.ex {
        Ex::Poloniex => poloniex::public::Client::new(),
    };
    let client_fill_absent_kline = client.clone();
    for symbol in config.poloniex_symbols.clone() {
        let mut m_timeframes: HashMap<KlineTimeframe, std::option::Option<Kline>> = HashMap::new();
        let mut m_timeframes_ts: HashMap<KlineTimeframe, std::option::Option<i64>> = HashMap::new();
        for timeframe in config.poloniex_timeframes.clone() {
            m_timeframes.insert(timeframe.clone(), None);
            m_timeframes_ts.insert(timeframe.clone(), None);
            client
                .fetch_insert_klines(
                    &client_clickhouse,
                    config.start_date_millis,
                    &symbol,
                    &timeframe,
                )
                .await?;
        }
        curr_klines.insert(symbol.clone(), m_timeframes);
        first_trade_ts.insert(symbol, m_timeframes_ts);
    }
    tokio::spawn(async move {
        while let Ok(k) = rx_curr_kline.recv() {
            log::info!("save kline to db k={:?}", k);
            let mut insert_klines = client_clickhouse_curr_kline.insert("klines").unwrap();
            insert_klines.write(&k.to_row()).await.unwrap();
            insert_klines.end().await.unwrap();
        }
    });
    tokio::spawn(async move {
        while let Ok((pair, timeframe)) = rx_fill_absent_kline.recv() {
            log::info!("fill absent kline in db pair={pair} timeframe={timeframe:?}");
            client_fill_absent_kline
                .fetch_insert_klines(
                    &client_clickhouse_fill_absent_kline,
                    config_fill_absent_kline.start_date_millis,
                    &pair,
                    &timeframe,
                )
                .await
                .unwrap();
        }
    });
    let on_message = |event: EventWs| {
        if let EventWs::Trade(t) = event {
            apply_trade(
                &t,
                &mut curr_klines,
                &mut first_trade_ts,
                &tx_curr_kline,
                &tx_fill_absent_kline,
            );
        }
    };
    client
        .listen_ws_channel(ChannelWs::Trade, &config.poloniex_symbols, on_message)
        .await?;
    Ok(())
}

fn apply_trade(
    trade: &RecentTrade,
    curr_klines: &mut HashMap<String, HashMap<KlineTimeframe, Option<Kline>>>,
    first_trade_ts: &mut HashMap<String, HashMap<KlineTimeframe, Option<i64>>>,
    tx_curr_kline: &Sender<Kline>,
    tx_fill_absent_kline: &Sender<(String, KlineTimeframe)>,
) {
    log::debug!("trade={:?}", trade);
    let curr_timeframes = curr_klines.get_mut(&trade.pair).unwrap();
    for (timeframe, curr_kline) in curr_timeframes.iter_mut() {
        let first_trade_ts = first_trade_ts
            .get_mut(&trade.pair)
            .unwrap()
            .get_mut(timeframe)
            .unwrap();
        if first_trade_ts.is_none() {
            log::debug!(
                "fill first_trade_ts for ({}, {:?}) to miss constructing inconsistent kline",
                trade.pair,
                timeframe.to_str()
            );
            *first_trade_ts = Some(trade.timestamp);
            continue;
        }
        let first_trade_ts_threshold = first_trade_ts.unwrap() / timeframe.to_inserval_millis()
            * timeframe.to_inserval_millis()
            + timeframe.to_inserval_millis()
            - 1;
        if trade.timestamp <= first_trade_ts_threshold {
            log::debug!("skip trade={:?} to avoid inconsistent kline", trade);
            continue;
        }
        if curr_kline.is_none() {
            log::debug!("start constructing new kline");
            curr_kline.replace(Kline::new_from_trade(timeframe.clone(), trade));
            tx_fill_absent_kline
                .send((trade.pair.clone(), timeframe.clone()))
                .unwrap();
            continue;
        }
        if curr_kline.as_mut().unwrap().expired(trade) {
            tx_curr_kline
                .send(curr_kline.as_mut().unwrap().clone())
                .unwrap();
            curr_kline.replace(Kline::new_from_trade(timeframe.clone(), trade));
            continue;
        }
        curr_kline.as_mut().unwrap().apply_recent_trade(trade);
    }
}

#[derive(Clone)]
enum Ex {
    Poloniex,
}

#[derive(Clone)]
struct Config {
    clickhouse_url: String,
    poloniex_symbols: Vec<String>,
    poloniex_timeframes: Vec<KlineTimeframe>,
    start_date_millis: i64,
    ex: Ex,
}

impl Config {
    pub fn new_from_envs() -> Self {
        Self {
            clickhouse_url: std::env::var("CLICKHOUSE_URL").unwrap(),
            poloniex_symbols: std::env::var("CONFIG_SYMBOLS")
                .unwrap()
                .split(',')
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            poloniex_timeframes: std::env::var("CONFIG_TIMEFRAMES")
                .unwrap()
                .split(',')
                .map(KlineTimeframe::new_from_str)
                .collect::<Vec<KlineTimeframe>>(),
            start_date_millis: chrono::NaiveDate::parse_from_str(
                &std::env::var("START_DATE").unwrap(),
                "%Y-%m-%d",
            )
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .timestamp_millis(),
            ex: match std::env::var("EX").unwrap().as_str() {
                "poloniex" => Ex::Poloniex,
                _ => panic!("unsupported exchange"),
            },
        }
    }
}
