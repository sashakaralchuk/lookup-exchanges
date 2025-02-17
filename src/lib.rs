pub mod exchange;

#[derive(Debug)]
pub struct RecentTrade {
    tid: String,
    pub pair: String,
    price: String,
    amount: String,
    side: String,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct Kline {
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
    pub fn new_from_str(s: &str) -> Self {
        let n = s[0..s.len() - 1].parse::<i64>().unwrap();
        match s.chars().last().unwrap() {
            'm' => KlineTimeframe::Minute(n),
            'h' => KlineTimeframe::Hour(n),
            'd' => KlineTimeframe::Day(n),
            _ => panic!("unknown time frame s={}", s),
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            KlineTimeframe::Minute(n) => format!("{}m", n),
            KlineTimeframe::Hour(n) => format!("{}h", n),
            KlineTimeframe::Day(n) => format!("{}d", n),
        }
    }

    pub fn to_inserval_millis(&self) -> i64 {
        match self {
            KlineTimeframe::Minute(n) => n * 60 * 1000,
            KlineTimeframe::Hour(n) => n * 60 * 60 * 1000,
            KlineTimeframe::Day(n) => n * 24 * 60 * 60 * 1000,
        }
    }
}

#[allow(clippy::upper_case_acronyms)]
#[derive(Debug, Clone)]
struct VBS {
    buy_base: f64,
    sell_base: f64,
    buy_quote: f64,
    sell_quote: f64,
}

impl Kline {
    pub fn new_from_trade(time_frame: KlineTimeframe, trade: &RecentTrade) -> Self {
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

    pub fn to_row(&self) -> KlineRow {
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

#[allow(non_snake_case)]
#[derive(clickhouse::Row, serde::Serialize)]
pub struct KlineRow {
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
