pub mod public {
    use crate::{
        millis_to_hr_str, now_millis, ChannelWs, ClientPublic, EventWs, Kline, KlineTimeframe,
        RecentTrade, VBS,
    };
    use async_tungstenite::async_std::connect_async;
    use async_tungstenite::tungstenite::Message;
    use futures::StreamExt;
    use std::time::Duration;
    use tokio::time::timeout;

    const URL_WS: &str = "wss://ws.poloniex.com/ws/public";

    #[derive(Clone)]
    pub struct Client {}

    impl ClientPublic for Client {
        fn new() -> Self {
            Self {}
        }

        async fn listen_ws_channel(
            &self,
            channel: ChannelWs,
            symbols: &Vec<String>,
            on_message: impl FnMut(EventWs),
        ) -> Result<(), Box<dyn std::error::Error>> {
            let (mut socket, _) = connect_async(URL_WS).await?;
            let channel_str = match channel {
                ChannelWs::Trade => "trades",
            };
            let sumbols_str = symbols
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
                if serde_json::from_str::<EventWsPingRaw>(msg_str).is_ok() {
                    continue;
                }
                if let Ok(_) = serde_json::from_str::<EventWsSubscribeRaw>(msg_str) {
                    f(EventWs::Subscribe);
                    continue;
                }
                if let Ok(o) = serde_json::from_str::<EventWsTradeRaw>(msg_str) {
                    for trade in o.conv_to_recent_trade_vec() {
                        f(EventWs::Trade(trade));
                    }
                    continue;
                }
                panic!("unknown msg_str={msg_str}");
            }
        }

        async fn fetch_insert_klines(
            &self,
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
                .bind(symbol)
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
    }

    /// Fetches all klines except current.
    async fn fetch_klines(
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
            klines.extend(res_obj.iter().map(|o| o.to_kline(symbol)));
            log::info!("res_obj={:?}", res_obj.len());
        }
        log::info!("klines.len()={}", klines.len());
        Ok(klines)
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    pub struct EventWsSubscribeRaw {
        pub event: String,
        pub channel: String,
        pub symbols: Vec<String>,
    }

    #[allow(dead_code)]
    #[derive(serde::Deserialize)]
    pub struct EventWsPingRaw {
        pub event: String,
    }

    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRaw {
        #[allow(dead_code)]
        pub channel: String,
        pub data: Vec<EventWsTradeRawData>,
    }

    #[allow(non_snake_case)]
    #[derive(serde::Deserialize)]
    pub struct EventWsTradeRawData {
        symbol: String,
        amount: String,
        #[allow(dead_code)]
        quantity: String,
        takerSide: String,
        createTime: i64,
        price: String,
        id: String,
        #[allow(dead_code)]
        ts: i64,
    }

    impl EventWsTradeRaw {
        fn conv_to_recent_trade_vec(&self) -> Vec<RecentTrade> {
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

    #[derive(serde::Deserialize)]
    pub struct EventHttpKlineRaw(
        #[serde(rename = "low")] String,
        #[serde(rename = "high")] String,
        #[serde(rename = "open")] String,
        #[serde(rename = "close")] String,
        #[serde(rename = "amount")] String,
        #[serde(rename = "quantity")] String,
        #[serde(rename = "buyTakerAmount")] String,
        #[serde(rename = "buyTakerQuantity")] String,
        #[allow(dead_code)]
        #[serde(rename = "tradeCount")]
        i64,
        #[allow(dead_code)]
        #[serde(rename = "ts")]
        i64,
        #[allow(dead_code)]
        #[serde(rename = "weightedAverage")]
        String,
        #[serde(rename = "interval")] String,
        #[serde(rename = "startTime")] i64,
        #[allow(dead_code)]
        #[serde(rename = "closeTime")]
        i64,
    );

    impl EventHttpKlineRaw {
        fn to_kline(&self, symbol: &str) -> Kline {
            let time_frame = match self.11.as_str() {
                "MINUTE_1" => KlineTimeframe::Minute(1),
                "MINUTE_15" => KlineTimeframe::Minute(15),
                "HOUR_1" => KlineTimeframe::Hour(1),
                "DAY_1" => KlineTimeframe::Day(1),
                _ => panic!("unknown interval={}", self.11),
            };
            Kline {
                time_frame,
                pair: symbol.to_owned(),
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
