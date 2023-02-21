use chrono::{DateTime, NaiveDateTime, Utc};

pub const SOURCE_DATETIME: u8 = 'S' as u8;
pub const GENERATED_DATETIME: u8 = 'G' as u8;
pub const OUR_DATETIME: u8 = 'O' as u8;
pub const MESSAGE_SPLITTER: &[u8; 1] = b" ";

#[derive(Debug, Clone)]
pub struct BidAskDataTcpModel {
    pub exchange_id: String,
    pub instrument_id: String,
    pub bid: f64,
    pub ask: f64,
    pub volume: f64,
    pub datetime: BidAskDateTimeTcpModel,
}

impl BidAskDataTcpModel {
    pub fn serialize(&self) -> Result<Vec<u8>, SerializeError> {
        let mut result = Vec::new();
        result.extend_from_slice(b"A");
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(self.exchange_id.as_bytes());
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(self.instrument_id.as_bytes());
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(b"B");
        result.extend_from_slice(format!("{}", self.bid).as_bytes());
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(b"A");
        result.extend_from_slice(format!("{}", self.ask).as_bytes());
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(format!("{}", self.volume).as_bytes());
        result.extend_from_slice(MESSAGE_SPLITTER);
        result.extend_from_slice(self.datetime.serialize()?.as_slice());

        return Ok(result);
    }

    pub fn deserialize(src: &[u8]) -> Result<Self, SerializeError> {
        let chunks = src.split(|x| *x == b' ').collect::<Vec<&[u8]>>();
        let exchange_id = String::from_utf8(chunks[1].to_vec()).unwrap();
        let instrument_id = String::from_utf8(chunks[2].to_vec()).unwrap();
        let bid = String::from_utf8(chunks[3][1..].to_vec()).unwrap();
        let ask = String::from_utf8(chunks[4][1..].to_vec()).unwrap();
        let volume = String::from_utf8(chunks[5].to_vec()).unwrap();

        Ok(Self {
            exchange_id,
            instrument_id,
            bid: bid.parse().unwrap(),
            ask: ask.parse().unwrap(),
            volume: volume.parse().unwrap(),
            datetime: BidAskDateTimeTcpModel::deserialize(chunks[6])?,
        })
    }
}

#[derive(Debug, Clone)]
pub enum BidAskDateTimeTcpModel {
    Source(DateTime<Utc>),
    Our(DateTime<Utc>),
    Generated(DateTime<Utc>),
}

impl BidAskDateTimeTcpModel {
    pub fn serialize(&self) -> Result<Vec<u8>, SerializeError> {
        let mut result = Vec::new();

        match self {
            &BidAskDateTimeTcpModel::Source(date) => {
                result.push(SOURCE_DATETIME);
                result.extend_from_slice(date.format("%Y%m%d%H%M%S%.3f").to_string().as_bytes());
            }
            &BidAskDateTimeTcpModel::Our(date) => {
                result.push(OUR_DATETIME);
                result.extend_from_slice(date.format("%Y%m%d%H%M%S%.3f").to_string().as_bytes());
            }
            &BidAskDateTimeTcpModel::Generated(date) => {
                result.push(GENERATED_DATETIME);
                result.extend_from_slice(date.format("%Y%m%d%H%M%S%.3f").to_string().as_bytes());
            }
        };

        return Ok(result);
    }

    pub fn deserialize(date_data: &[u8]) -> Result<Self, SerializeError> {
        let date_marker = date_data.first();
        let date = deserialize_date(&date_data[1..])?;

        if let Some(marker_byte) = date_marker {
            let date = match marker_byte {
                &OUR_DATETIME => Self::Our(date),
                &SOURCE_DATETIME => Self::Source(date),
                &GENERATED_DATETIME => Self::Generated(date),
                _ => return Err(SerializeError::InvalidDateMarker),
            };

            return Ok(date);
        }

        return Err(SerializeError::MissingDateMarker);
    }
}

fn deserialize_date(date: &[u8]) -> Result<DateTime<Utc>, SerializeError> {
    let string_date = String::from_utf8(date.to_vec());

    let Ok(date_string) = string_date else{
        return Err(SerializeError::DateSerializeError);
    };

    let Ok(date_time) = NaiveDateTime::parse_from_str(&date_string, "%Y%m%d%H%M%S%.3f") else{
        return Err(SerializeError::InvalidDate);
    };

    let date_time = DateTime::<Utc>::from_utc(date_time, Utc);

    Ok(date_time)
}

#[derive(Debug)]
pub enum SerializeError {
    InvalidDate,
    InvalidDateMarker,
    MissingDateMarker,
    DateSerializeError,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize() {
        let message = b"A BINANCE EURUSD B1.55555 A2.55555 50000000 S20230213142225.555";
        let result = BidAskDataTcpModel::deserialize(message).unwrap();

        assert_eq!(result.exchange_id, "BINANCE");
        assert_eq!(result.instrument_id, "EURUSD");
        assert_eq!(result.bid, 1.55555);
        assert_eq!(result.ask, 2.55555);
        assert_eq!(result.volume, 50000000.0);

        let is_source = match result.datetime {
            BidAskDateTimeTcpModel::Source(_) => true,
            BidAskDateTimeTcpModel::Our(_) => false,
            BidAskDateTimeTcpModel::Generated(_) => false,
        };

        assert_eq!(is_source, true);
    }

    #[test]
    fn test_serialize() {
        let message = "A BINANCE EURUSD B1.55555 A2.55555 50000000 S20230213142225.555";

        let datetime =
            NaiveDateTime::parse_from_str("20230213142225.555", "%Y%m%d%H%M%S%.3f").unwrap();
        let utc = DateTime::<Utc>::from_utc(datetime, Utc);
        let result = BidAskDataTcpModel {
            exchange_id: "BINANCE".to_string(),
            instrument_id: "EURUSD".to_string(),
            bid: 1.55555,
            ask: 2.55555,
            volume: 50000000.0,
            datetime: BidAskDateTimeTcpModel::Source(utc),
        };

        let deserialzie = result.serialize().unwrap();

        assert_eq!(String::from_utf8(deserialzie).unwrap(), message);
    }
}
