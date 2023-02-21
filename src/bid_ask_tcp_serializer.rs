use async_trait::async_trait;
use my_tcp_sockets::{
    socket_reader::{ReadBuffer, ReadingTcpContractFail, SocketReader},
    TcpSocketSerializer,
};

use super::models::BidAskTcpMessage;

static CLCR: &[u8] = &[13u8, 10u8];
const MAX_PACKET_CAPACITY: usize = 255;

pub struct BidAskTcpSerializer {
    read_buffer: ReadBuffer,
}

impl BidAskTcpSerializer {
    pub fn new() -> Self {
        Self {
            read_buffer: ReadBuffer::new(1024 * 24),
        }
    }
}

#[async_trait]
impl TcpSocketSerializer<BidAskTcpMessage> for BidAskTcpSerializer {
    const PING_PACKET_IS_SINGLETONE: bool = false;

    fn serialize(&self, contract: BidAskTcpMessage) -> Vec<u8> {
        let mut result = Vec::with_capacity(MAX_PACKET_CAPACITY);
        contract.serialize(&mut result).unwrap();
        result.extend_from_slice(CLCR);
        result
    }
    fn get_ping(&self) -> BidAskTcpMessage {
        return BidAskTcpMessage::Ping;
    }
    async fn deserialize<TSocketReader: Send + Sync + 'static + SocketReader>(
        &mut self,
        socket_reader: &mut TSocketReader,
    ) -> Result<BidAskTcpMessage, ReadingTcpContractFail> {
        let result = socket_reader
            .read_until_end_marker(&mut self.read_buffer, CLCR)
            .await?;

        let result = &result[..result.len() - CLCR.len()];
        let result = BidAskTcpMessage::parse(result);

        match result {
            Ok(result) => Ok(result),
            Err(_) => Err(ReadingTcpContractFail::ErrorReadingSize),
        }
    }

    fn serialize_ref(&self, contract: &BidAskTcpMessage) -> Vec<u8> {
        let mut result = Vec::with_capacity(MAX_PACKET_CAPACITY);
        contract.serialize(&mut result).unwrap();
        result.extend_from_slice(CLCR);
        result
    }

    fn apply_packet(&mut self, _: &BidAskTcpMessage) -> bool {
        false
    }
}
