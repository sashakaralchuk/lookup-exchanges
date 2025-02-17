use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();
    dotenv::dotenv().ok();
    let config = Config::new_from_envs();
    let config_fill_absent_kline = config.clone();
    let client_clickhouse = clickhouse::Client::default().with_url(&config.clickhouse_url);
    let client_clickhouse_curr_kline = client_clickhouse.clone();
    let client_clickhouse_fill_absent_kline = client_clickhouse.clone();
    let (tx_curr_kline, rx_curr_kline) = std::sync::mpsc::channel();
    let (tx_fill_absent_kline, rx_fill_absent_kline) = std::sync::mpsc::channel();
    let mut curr_klines = HashMap::new();
    let mut first_trade_ts = HashMap::new();
    for symbol in config.poloniex_symbols.clone() {
        let mut m_timeframes: HashMap<KlineTimeframe, std::option::Option<Kline>> = HashMap::new();
        let mut m_timeframes_ts: HashMap<KlineTimeframe, std::option::Option<i64>> = HashMap::new();
        for timeframe in config.poloniex_timeframes.clone() {
            m_timeframes.insert(timeframe.clone(), None);
            m_timeframes_ts.insert(timeframe.clone(), None);
            poloniex_pub::fetch_insert_klines(
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
    let on_message = |event: poloniex_pub::EventWs| {
        if let poloniex_pub::EventWs::Trade(t) = event {
            for trade in t.conv_to_recent_trade_vec() {
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
                    let first_trade_ts_threshold = first_trade_ts.unwrap()
                        / timeframe.to_inserval_millis()
                        * timeframe.to_inserval_millis()
                        + timeframe.to_inserval_millis()
                        - 1;
                    if trade.timestamp <= first_trade_ts_threshold {
                        log::debug!("skip trade={:?} due to avoid inconsistent kline", trade);
                        continue;
                    }
                    if curr_kline.is_none() {
                        log::debug!("start constructing new kline");
                        curr_kline.replace(Kline::new_from_trade(timeframe.clone(), &trade));
                        tx_fill_absent_kline
                            .send((trade.pair.clone(), timeframe.clone()))
                            .unwrap();
                        continue;
                    }
                    if curr_kline.as_mut().unwrap().expired(&trade) {
                        tx_curr_kline
                            .send(curr_kline.as_mut().unwrap().clone())
                            .unwrap();
                        curr_kline.replace(Kline::new_from_trade(timeframe.clone(), &trade));
                        continue;
                    }
                    curr_kline.as_mut().unwrap().apply_recent_trade(&trade);
                }
            }
        }
    };
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
            poloniex_pub::fetch_insert_klines(
                &client_clickhouse_fill_absent_kline,
                config_fill_absent_kline.start_date_millis,
                &pair,
                &timeframe,
            )
            .await
            .unwrap();
        }
    });
    poloniex_pub::listen_ws_channel(
        &poloniex_pub::ConfigWs::new(poloniex_pub::ChannelWs::Trade, config.poloniex_symbols),
        on_message,
    )
    .await?;
    Ok(())
}

mod poloniex_pub {
    use crate::{millis_to_hr_str, now_millis, Kline, KlineTimeframe, RecentTrade, VBS};
    use async_tungstenite::async_std::connect_async;
    use async_tungstenite::tungstenite::Message;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    const URL_WS: &str = "wss://ws.poloniex.com/ws/public";

    pub async fn listen_ws_channel(
        config: &ConfigWs,
        on_message: impl FnMut(EventWs),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut socket, _) = connect_async(URL_WS).await?;
        let channel_str = match config.channel {
            ChannelWs::Trade => "trades",
        };
        let sumbols_str = config
            .symbols
            .iter()
            .map(|s| format!(r#""{}""#, s))
            .collect::<Vec<String>>()
            .join(",");
        let subscribe_text = format!(
            r#"{{"event": "subscribe", "channel": ["{}"], "symbols": [{}]}}"#,
            channel_str, sumbols_str
        );
        log::info!("subscribe_text={subscribe_text}");
        socket.send(Message::Text(subscribe_text)).await?;
        let mut ping_last_millis = now_millis();
        let ping_threshold_millis = 15 * 1000; // NOTE: ping must happens every 30s
        let mut f = on_message;
        loop {
            if now_millis() > (ping_last_millis + ping_threshold_millis) {
                log::debug!("ping");
                socket
                    .send(Message::Text(r#"{"event": "ping"}"#.into()))
                    .await?;
                ping_last_millis = now_millis();
            }
            let msg = match timeout(Duration::from_secs(1), socket.next()).await {
                Ok(m) => m.unwrap().unwrap(),
                Err(_) => continue,
            };
            let msg_str = msg.to_text().unwrap();
            log::debug!("msg_str={msg_str}");
            if let Ok(_) = serde_json::from_str::<EventWsPingRaw>(msg_str) {
                continue;
            }
            if let Ok(o) = serde_json::from_str::<EventWsSubscribeRaw>(msg_str) {
                f(EventWs::Subscribe(o));
                continue;
            }
            if let Ok(o) = serde_json::from_str::<EventWsTradeRaw>(msg_str) {
                f(EventWs::Trade(o));
                continue;
            }
            panic!("unknown msg_str={msg_str}");
        }
    }

    /// Fetches all klines except current.
    pub async fn fetch_klines(
        symbol: &str,
        timeframe: KlineTimeframe,
        start_millis: i64,
        end_millis: i64,
    ) -> Result<Vec<Kline>, Box<dyn std::error::Error>> {
        let interval_str = match timeframe {
            KlineTimeframe::Minute(1) => "MINUTE_1".to_string(),
            KlineTimeframe::Minute(15) => "MINUTE_15".to_string(),
            KlineTimeframe::Hour(1) => "HOUR_1".to_string(),
            KlineTimeframe::Day(1) => "DAY_1".to_string(),
            _ => panic!("inproper timeframe={timeframe:?}"),
        };
        let limit = 500;
        let interval_millis = timeframe.to_inserval_millis();
        let window_millis = interval_millis * (limit - 1);
        let end_adj_millis = end_millis / interval_millis * interval_millis - 1;
        log::debug!(
            "start_ts={} end_adj_millis={}",
            millis_to_hr_str(start_millis),
            millis_to_hr_str(end_adj_millis)
        );
        let mut klines = vec![];
        for i in 0..((end_millis - start_millis) / window_millis + 1) {
            let start_time = start_millis + i * window_millis;
            let end_time = i64::min(start_millis + (i + 1) * window_millis - 1, end_adj_millis);
            let url = format!(
                "https://api.poloniex.com/markets/{}/candles?interval={}&startTime={}&endTime={}&limit={}",
                symbol,
                interval_str,
                start_time,
                end_time,
                limit
            );
            log::debug!("url={url}");
            let res = reqwest::Client::new()
                .get(url)
                .header("Content-Type", "application/json")
                .send()
                .await?;
            let res_text = res.text().await.unwrap();
            let mut res_obj: Vec<EventHttpKlineRaw> = serde_json::from_str(&res_text).unwrap();
            res_obj.sort_by(|a, b| a.12.partial_cmp(&b.12).unwrap());
            klines.extend(
                res_obj
                    .iter()
                    .map(|o| o.to_kline(&symbol.to_string()))
                    .into_iter(),
            );
            log::info!("res_obj={:?}", res_obj.len());
        }
        log::info!("klines.len()={}", klines.len());
        return Ok(klines);
    }

    pub async fn fetch_insert_klines(
        client_clickhouse: &clickhouse::Client,
        start_date_millis: i64,
        symbol: &String,
        timeframe: &KlineTimeframe,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let end_millis = now_millis();
        log::info!("load klines symbol={} timeframe={:?}", symbol, timeframe);
        let last_kline_utc_begin = client_clickhouse
            .query(
                r#"
                SELECT toUnixTimestamp(utc_begin)
                FROM default.klines
                WHERE pair = ? AND time_frame = ?
                ORDER BY utc_begin DESC
                LIMIT 1
                "#,
            )
            .bind(&symbol)
            .bind(&timeframe.to_str())
            .fetch::<i32>()
            .unwrap()
            .next()
            .await?;
        let start_millis = match last_kline_utc_begin {
            Some(ts) => (ts as i64) * 1000 + timeframe.to_inserval_millis(),
            None => start_date_millis,
        };
        let klines = fetch_klines(symbol, timeframe.clone(), start_millis, end_millis).await?;
        log::info!(
            "save klines to db symbol={} timeframe={:?} klines.len()={}",
            symbol,
            timeframe,
            klines.len()
        );
        let mut insert_klines = client_clickhouse.insert("klines")?;
        for k in klines.iter() {
            insert_klines.write(&k.to_row()).await?;
        }
        insert_klines.end().await?;
        Ok(())
    }

    pub struct ConfigWs {
        channel: ChannelWs,
        symbols: Vec<String>,
    }

    impl ConfigWs {
        pub fn new(channel: ChannelWs, symbols: Vec<String>) -> Self {
            Self { channel, symbols }
        }
    }

    pub enum ChannelWs {
        Trade,
    }

    pub enum EventWs {
        Subscribe(EventWsSubscribeRaw),
        Trade(EventWsTradeRaw),
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsSubscribeRaw {
        pub event: String,
        pub channel: String,
        pub symbols: Vec<String>,
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsPingRaw {
        pub event: String,
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRaw {
        pub channel: String,
        pub data: Vec<EventWsTradeRawData>,
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRawData {
        symbol: String,
        amount: String,
        quantity: String,
        #[allow(non_snake_case)]
        takerSide: String,
        #[allow(non_snake_case)]
        createTime: i64,
        price: String,
        id: String,
        ts: i64,
    }

    impl EventWsTradeRaw {
        pub fn conv_to_recent_trade_vec(&self) -> Vec<RecentTrade> {
            self.data
                .iter()
                .map(|d| RecentTrade {
                    tid: d.id.clone(),
                    pair: d.symbol.clone(),
                    price: d.price.clone(),
                    amount: d.amount.clone(),
                    side: d.takerSide.clone(),
                    timestamp: d.createTime,
                })
                .collect()
        }
    }

    #[derive(serde::Deserialize, Debug)]
    pub struct EventHttpKlineRaw(
        #[serde(rename = "low")] String,
        #[serde(rename = "high")] String,
        #[serde(rename = "open")] String,
        #[serde(rename = "close")] String,
        #[serde(rename = "amount")] String,
        #[serde(rename = "quantity")] String,
        #[serde(rename = "buyTakerAmount")] String,
        #[serde(rename = "buyTakerQuantity")] String,
        #[serde(rename = "tradeCount")] i64,
        #[serde(rename = "ts")] i64,
        #[serde(rename = "weightedAverage")] String,
        #[serde(rename = "interval")] String,
        #[serde(rename = "startTime")] i64,
        #[serde(rename = "closeTime")] i64,
    );

    impl EventHttpKlineRaw {
        fn to_kline(&self, symbol: &String) -> Kline {
            let time_frame = match self.11.as_str() {
                "MINUTE_1" => KlineTimeframe::Minute(1),
                "MINUTE_15" => KlineTimeframe::Minute(15),
                "HOUR_1" => KlineTimeframe::Hour(1),
                "DAY_1" => KlineTimeframe::Day(1),
                _ => panic!("unknown interval={}", self.11),
            };
            Kline {
                time_frame,
                pair: symbol.clone(),
                o: self.2.parse().unwrap(),
                h: self.1.parse().unwrap(),
                l: self.0.parse().unwrap(),
                c: self.3.parse().unwrap(),
                utc_begin: self.12,
                volume_bs: VBS {
                    buy_base: self.7.parse().unwrap(),
                    sell_base: self.5.parse().unwrap(),
                    buy_quote: self.6.parse().unwrap(),
                    sell_quote: self.4.parse().unwrap(),
                },
                last_tid: 0,
            }
        }
    }
}

#[derive(Debug)]
struct RecentTrade {
    tid: String,
    pair: String,
    price: String,
    amount: String,
    side: String,
    timestamp: i64,
}

#[derive(Debug, Clone)]
struct Kline {
    pair: String,
    time_frame: KlineTimeframe,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    utc_begin: i64,
    volume_bs: VBS,
    last_tid: i64,
}

#[derive(Debug, Clone, serde::Serialize, Eq, PartialEq, Hash)]
pub enum KlineTimeframe {
    Minute(i64),
    Hour(i64),
    Day(i64),
}

impl KlineTimeframe {
    fn new_from_str(s: &str) -> Self {
        let n = s[0..s.len() - 1].parse::<i64>().unwrap();
        match s.chars().last().unwrap() {
            'm' => KlineTimeframe::Minute(n),
            'h' => KlineTimeframe::Hour(n),
            'd' => KlineTimeframe::Day(n),
            _ => panic!("unknown time frame s={}", s),
        }
    }

    fn to_str(&self) -> String {
        match self {
            KlineTimeframe::Minute(n) => format!("{}m", n),
            KlineTimeframe::Hour(n) => format!("{}h", n),
            KlineTimeframe::Day(n) => format!("{}d", n),
        }
    }

    fn to_inserval_millis(&self) -> i64 {
        match self {
            KlineTimeframe::Minute(n) => n * 60 * 1000,
            KlineTimeframe::Hour(n) => n * 60 * 60 * 1000,
            KlineTimeframe::Day(n) => n * 24 * 60 * 60 * 1000,
        }
    }
}

#[derive(Debug, Clone)]
struct VBS {
    buy_base: f64,
    sell_base: f64,
    buy_quote: f64,
    sell_quote: f64,
}

impl Kline {
    fn new_from_trade(time_frame: KlineTimeframe, trade: &RecentTrade) -> Self {
        let utc_begin_millis = trade.timestamp / 60_000 * 60_000;
        let p: f64 = trade.price.parse().unwrap();
        let mut k = Self {
            time_frame,
            pair: trade.pair.clone(),
            o: p,
            h: p,
            l: p,
            c: p,
            utc_begin: utc_begin_millis,
            volume_bs: VBS {
                buy_base: 0.0,
                sell_base: 0.0,
                buy_quote: 0.0,
                sell_quote: 0.0,
            },
            last_tid: trade.tid.parse::<i64>().unwrap() - 1,
        };
        k.apply_recent_trade(trade);
        k
    }

    pub fn apply_recent_trade(&mut self, trade: &RecentTrade) {
        let p: f64 = trade.price.parse().unwrap();
        self.c = p;
        self.h = f64::max(self.h, p);
        self.l = f64::min(self.l, p);
        let tid = trade.tid.parse::<i64>().unwrap();
        if self.last_tid + 1 != tid {
            panic!("unconsistent last_tid={} tid={}", self.last_tid, tid);
        }
        self.last_tid = tid;
        // XXX: adjust f64 calcs
        if trade.side == "buy" {
            self.volume_bs.buy_base = trade.amount.parse().unwrap();
            self.volume_bs.buy_quote = trade.amount.parse::<f64>().unwrap() * p;
        } else {
            self.volume_bs.sell_base = trade.amount.parse().unwrap();
            self.volume_bs.sell_quote = trade.amount.parse::<f64>().unwrap() * p;
        }
    }

    pub fn expired(&self, recent_trade: &RecentTrade) -> bool {
        let interval_millis = self.time_frame.to_inserval_millis();
        let current_start_min = recent_trade.timestamp / interval_millis;
        let candle_start_min = self.utc_begin / interval_millis;
        current_start_min > candle_start_min
    }

    fn to_row(&self) -> KlineRow {
        KlineRow {
            pair: self.pair.clone(),
            time_frame: self.time_frame.to_str(),
            o: self.o,
            h: self.h,
            l: self.l,
            c: self.c,
            utc_begin: time::OffsetDateTime::from_unix_timestamp(self.utc_begin / 1000).unwrap(),
            volume_bs__buy_base: self.volume_bs.buy_base,
            volume_bs__sell_base: self.volume_bs.sell_base,
            volume_bs__buy_quote: self.volume_bs.buy_quote,
            volume_bs__sell_quote: self.volume_bs.sell_quote,
        }
    }
}

#[derive(clickhouse::Row, serde::Serialize)]
struct KlineRow {
    pair: String,
    time_frame: String,
    o: f64,
    h: f64,
    l: f64,
    c: f64,
    #[serde(with = "clickhouse::serde::time::datetime")]
    utc_begin: time::OffsetDateTime,
    volume_bs__buy_base: f64,
    volume_bs__sell_base: f64,
    volume_bs__buy_quote: f64,
    volume_bs__sell_quote: f64,
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn millis_to_hr_str(m: i64) -> String {
    chrono::DateTime::from_timestamp_millis(m)
        .unwrap()
        .to_string()
}

#[derive(Clone)]
struct Config {
    clickhouse_url: String,
    poloniex_symbols: Vec<String>,
    poloniex_timeframes: Vec<KlineTimeframe>,
    start_date_millis: i64,
}

impl Config {
    pub fn new_from_envs() -> Self {
        Self {
            clickhouse_url: std::env::var("CLICKHOUSE_URL").unwrap(),
            poloniex_symbols: std::env::var("CONFIG_SYMBOLS")
                .unwrap()
                .split(",")
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
            poloniex_timeframes: std::env::var("CONFIG_TIMEFRAMES")
                .unwrap()
                .split(",")
                .into_iter()
                .map(|s| KlineTimeframe::new_from_str(s))
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
        }
    }
}

#[cfg(test)]
mod test {
    fn create_recent_trade(timestamp: i64) -> super::RecentTrade {
        super::RecentTrade {
            tid: "1".to_string(),
            pair: "BTC_USDT".to_string(),
            price: "10000".to_string(),
            amount: "1".to_string(),
            side: "buy".to_string(),
            timestamp,
        }
    }

    #[test]
    fn test_handle_kline_expired() {
        let th = 1739645880000;
        let t1 = create_recent_trade(th);
        let t2 = create_recent_trade(th + 1);
        let t3 = create_recent_trade(th + 59_999);
        let t4 = create_recent_trade(th + 60_000);
        let k1 = super::Kline::new_from_trade(super::KlineTimeframe::Minute(1), &t1);
        assert_eq!(k1.expired(&t2), false);
        assert_eq!(k1.expired(&t3), false);
        assert_eq!(k1.expired(&t4), true);
        let k2 = super::Kline::new_from_trade(super::KlineTimeframe::Minute(1), &t2);
        assert_eq!(k2.expired(&t3), false);
        assert_eq!(k2.expired(&t4), true);
    }
}
