use my_tcp_sockets::{TcpContract, TcpWriteBuffer};

use super::bid_ask_data::{BidAskDataTcpModel, SerializeError};

#[derive(Debug, Clone)]
pub enum BidAskTcpMessage {
    Ping,
    Pong,
    BidAsk(BidAskDataTcpModel),
    Skip(String),
}

impl BidAskTcpMessage {
    pub fn is_ping(&self) -> bool {
        match self {
            BidAskTcpMessage::Ping => true,
            _ => false,
        }
    }

    pub fn parse(src: &[u8]) -> Result<Self, SerializeError> {
        if src.len() == 4 {
            if src == b"PING" {
                return Ok(Self::Ping);
            }
            if src == b"PONG" {
                return Ok(Self::Pong);
            }
        }

        if src[0] == b'A' {
            return Ok(Self::BidAsk(BidAskDataTcpModel::deserialize(src)?));
        }

        Ok(Self::Skip((std::str::from_utf8(src).unwrap()).to_string()))
    }

    pub fn serialize(&self, write_buffer: &mut impl TcpWriteBuffer) {
        match self {
            BidAskTcpMessage::Ping => write_buffer.write_slice("PING".as_bytes()),
            BidAskTcpMessage::Pong => write_buffer.write_slice(b"PONG"),
            BidAskTcpMessage::BidAsk(bid_ask) => bid_ask.serialize(write_buffer),
            BidAskTcpMessage::Skip(message) => {
                write_buffer.write_slice(message.as_bytes());
            }
        }
    }

    pub fn is_bid_ask(&self) -> bool {
        match self {
            BidAskTcpMessage::Ping => false,
            BidAskTcpMessage::Pong => false,
            BidAskTcpMessage::BidAsk(_) => true,
            BidAskTcpMessage::Skip(_) => false,
        }
    }
}

impl TcpContract for BidAskTcpMessage {
    fn is_pong(&self) -> bool {
        match self {
            BidAskTcpMessage::Pong => true,
            _ => false,
        }
    }
}

#[cfg(test)]
mod test {
    use crate::BidAskTcpMessage;

    #[test]
    fn test_message() {
        let src = "A binance BTCUSD B63687.33 A63687.34 0 S20240507153442.320";

        let itm = BidAskTcpMessage::parse(src.as_bytes()).unwrap();

        println!("{:?}", itm);
    }
}
