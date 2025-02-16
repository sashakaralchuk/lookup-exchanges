use tokio_postgres::NoTls;

#[tokio::main]
async fn main() {
    env_logger::init();
    dotenv::dotenv().ok();
    let (client_postgres, connection_postgres) =
        tokio_postgres::connect(&std::env::var("POSTGRES_CONN_STR").unwrap(), NoTls)
            .await
            .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection_postgres.await {
            eprintln!("postgres conn error: {}", e);
        }
    });
    let (tx, rx) = std::sync::mpsc::channel::<Kline>();
    let symbol = "BTC_USDT".to_string();
    let config = poloniex_ws_pub::Config::new(poloniex_ws_pub::Channel::Trade, symbol);
    let mut curr_kline: Option<Kline> = None;
    let on_message = |event: poloniex_ws_pub::Event| {
        if let poloniex_ws_pub::Event::Trade(t) = event {
            for trade in t.conv_to_recent_trade_vec() {
                if curr_kline.is_none() {
                    curr_kline = Some(Kline::new_from_trade(KlineTimeframe::Minute(1), &trade));
                    continue;
                }
                if curr_kline.as_mut().unwrap().expired(&trade) {
                    tx.send(curr_kline.as_mut().unwrap().clone()).unwrap();
                    curr_kline = Some(Kline::new_from_trade(KlineTimeframe::Minute(1), &trade));
                    continue;
                }
                curr_kline.as_mut().unwrap().apply_recent_trade(&trade);
            }
        }
    };
    tokio::spawn(async move {
        loop {
            match rx.recv_timeout(std::time::Duration::from_millis(100)) {
                Ok(k) => {
                    log::info!("save kline to db k={:?}", k);
                    client_postgres
                        .execute(&k.to_sql_insert(), &[])
                        .await
                        .unwrap();
                }
                Err(e) => log::debug!("try_recv error {:?}", e),
            }
        }
    });
    poloniex_ws_pub::listen_pub_channel(&config, on_message)
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
        let subscribe_text = format!(
            r#"{{"event": "subscribe", "channel": ["{}"], "symbols": ["{}"]}}"#,
            channel_str, config.symbol
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
        symbol: String,
    }

    impl Config {
        pub fn new(channel: Channel, symbol: String) -> Self {
            Self { channel, symbol }
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

#[derive(Debug, Clone, serde::Serialize)]
pub enum KlineTimeframe {
    Minute(i64),
    Hour(i64),
    Day(i64),
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
        let interval_millis = match self.time_frame {
            KlineTimeframe::Minute(n) => n * 60 * 1000,
            KlineTimeframe::Hour(n) => n * 60 * 60 * 1000,
            KlineTimeframe::Day(n) => n * 24 * 60 * 60 * 1000,
        };
        let current_start_min = recent_trade.timestamp / interval_millis;
        let candle_start_min = self.utc_begin / interval_millis;
        current_start_min > candle_start_min
    }

    pub fn to_sql_insert(&self) -> String {
        let time_frame_str = match self.time_frame {
            KlineTimeframe::Minute(n) => format!("{}m", n),
            KlineTimeframe::Hour(n) => format!("{}h", n),
            KlineTimeframe::Day(n) => format!("{}d", n),
        };
        format!(
            r#"INSERT INTO public.candles (pair, time_frame, o, h, l, c,
            utc_begin, volume_bs__buy_base, volume_bs__sell_base, volume_bs__buy_quote,
            volume_bs__sell_quote) VALUES ('{}','{}',{},{},{},{},to_timestamp({}),
            {},{},{},{});"#,
            self.pair,
            time_frame_str,
            self.o,
            self.h,
            self.l,
            self.c,
            self.utc_begin,
            self.volume_bs.buy_base,
            self.volume_bs.sell_base,
            self.volume_bs.buy_quote,
            self.volume_bs.sell_quote
        )
    }
}

fn now_millis() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod test {
    fn create_recent_trade(timestamp: i64) -> super::RecentTrade {
        super::RecentTrade {
            tid: "".to_string(),
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
