use my_tcp_sockets::{TcpContract, TcpWriteBuffer};

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

    pub fn serialize(&self, write_buffer: &mut impl TcpWriteBuffer) {
        match self {
            BidAskTcpMessage::Ping => write_buffer.write_slice("PING".as_bytes()),
            BidAskTcpMessage::Pong => write_buffer.write_slice(b"PONG"),
            BidAskTcpMessage::BidAsk(bid_ask) => bid_ask.serialize(write_buffer),
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
