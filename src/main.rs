use std::collections::HashMap;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv::dotenv().ok();
    let config = Config::new_from_envs();
    let client_clickhouse = clickhouse::Client::default().with_url(config.clickhouse_url);
    let (tx, rx) = std::sync::mpsc::channel::<Kline>();
    let mut curr_klines = {
        let mut m_symbols = HashMap::new();
        for symbol in config.poloniex_symbols.clone() {
            let mut m_timeframes: HashMap<KlineTimeframe, std::option::Option<Kline>> =
                HashMap::new();
            for timeframe in config.poloniex_timeframes.clone() {
                m_timeframes.insert(timeframe, None);
            }
            m_symbols.insert(symbol, m_timeframes);
        }
        m_symbols
    };
    let on_message = |event: poloniex_ws_pub::Event| {
        if let poloniex_ws_pub::Event::Trade(t) = event {
            for trade in t.conv_to_recent_trade_vec() {
                log::debug!("trade={:?}", trade);
                let curr_timeframes = curr_klines.get_mut(&trade.pair).unwrap();
                for (timeframe, curr_kline) in curr_timeframes.iter_mut() {
                    if curr_kline.is_none() {
                        curr_kline.replace(Kline::new_from_trade(timeframe.clone(), &trade));
                        continue;
                    }
                    if curr_kline.as_mut().unwrap().expired(&trade) {
                        tx.send(curr_kline.as_mut().unwrap().clone()).unwrap();
                        curr_kline.replace(Kline::new_from_trade(timeframe.clone(), &trade));
                        continue;
                    }
                    curr_kline.as_mut().unwrap().apply_recent_trade(&trade);
                }
            }
        }
    };
    tokio::spawn(async move {
        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(k) => {
                    log::info!("save kline to db k={:?}", k);
                    let mut insert_klines = client_clickhouse.insert("klines").unwrap();
                    insert_klines.write(&k.to_row()).await.unwrap();
                    insert_klines.end().await.unwrap();
                }
                Err(e) => log::debug!("try_recv error {:?}", e),
            }
        }
    });
    poloniex_ws_pub::listen_pub_channel(
        &poloniex_ws_pub::Config::new(poloniex_ws_pub::Channel::Trade, config.poloniex_symbols),
        on_message,
    )
    .await
    .unwrap();
}

mod poloniex_ws_pub {
    use crate::{now_millis, RecentTrade};
    use async_tungstenite::async_std::connect_async;
    use async_tungstenite::tungstenite::Message;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    const URL_WS: &str = "wss://ws.poloniex.com/ws/public";

    pub async fn listen_pub_channel(
        config: &Config,
        on_message: impl FnMut(Event),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let (mut socket, _) = connect_async(URL_WS).await?;
        let channel_str = match config.channel {
            Channel::Trade => "trades",
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
            if let Ok(_) = serde_json::from_str::<EventPingRaw>(msg_str) {
                continue;
            }
            if let Ok(o) = serde_json::from_str::<EventSubscribeRaw>(msg_str) {
                f(Event::Subscribe(o));
                continue;
            }
            if let Ok(o) = serde_json::from_str::<EventTradeRaw>(msg_str) {
                f(Event::Trade(o));
                continue;
            }
            panic!("unknown msg_str={msg_str}");
        }
    }

    pub struct Config {
        channel: Channel,
        symbols: Vec<String>,
    }

    impl Config {
        pub fn new(channel: Channel, symbols: Vec<String>) -> Self {
            Self { channel, symbols }
        }
    }

    pub enum Channel {
        Trade,
    }

    pub enum Event {
        Subscribe(EventSubscribeRaw),
        Trade(EventTradeRaw),
    }

    #[derive(serde::Deserialize)]
    pub struct EventSubscribeRaw {
        pub event: String,
        pub channel: String,
        pub symbols: Vec<String>,
    }

    #[derive(serde::Deserialize)]
    pub struct EventPingRaw {
        pub event: String,
    }

    #[derive(serde::Deserialize)]
    pub struct EventTradeRaw {
        pub channel: String,
        pub data: Vec<EventTradeRawData>,
    }

    #[derive(serde::Deserialize)]
    pub struct EventTradeRawData {
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

    impl EventTradeRaw {
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

struct Config {
    pub clickhouse_url: String,
    pub poloniex_symbols: Vec<String>,
    pub poloniex_timeframes: Vec<KlineTimeframe>,
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
