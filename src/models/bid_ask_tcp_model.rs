use my_tcp_sockets::tcp_connection::TcpContract;

use super::bid_ask_data::{BidAskDataTcpModel, SerializeError};

#[derive(Debug, Clone)]
pub enum BidAskTcpMessage {
    Ping,
    Pong,
    BidAsk(BidAskDataTcpModel),
}

impl BidAskTcpMessage {
    pub fn is_ping(&self) -> bool {
        match self {
            BidAskTcpMessage::Ping => true,
            _ => false,
        }
    }

    pub fn parse(src: &[u8]) -> Result<Self, SerializeError> {
        if src == b"PING" {
            return Ok(Self::Ping);
        }
        if src == b"PONG" {
            return Ok(Self::Pong);
        }

        Ok(Self::BidAsk(BidAskDataTcpModel::deserialize(src)?))
    }

    pub fn serialize(&self, dest: &mut Vec<u8>) -> Result<(), SerializeError> {
        match self {
            BidAskTcpMessage::Ping => Ok(dest.extend_from_slice(b"PING")),
            BidAskTcpMessage::Pong => Ok(dest.extend_from_slice(b"PONG")),
            BidAskTcpMessage::BidAsk(bid_ask) => {
                dest.extend_from_slice(bid_ask.serialize()?.as_slice());
                Ok(())
            }
        }
    }

    pub fn is_bid_ask(&self) -> bool {
        match self {
            BidAskTcpMessage::Ping => false,
            BidAskTcpMessage::Pong => false,
            BidAskTcpMessage::BidAsk(_) => true,
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
